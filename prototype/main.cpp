#include <absl/crc/crc32c.h>
#include <boost/asio.hpp>
#include <boost/beast/websocket.hpp>
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <simdjson.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <sys/resource.h>
#include <sys/uio.h>
#include <unistd.h>

namespace asio = boost::asio;
namespace beast = boost::beast;
using Json = nlohmann::ordered_json;
using Bytes = std::vector<std::uint8_t>;
using namespace std::chrono;
constexpr std::size_t frame_limit = 32 * 1024 * 1024;
constexpr std::size_t queue_limit = 64 * 1024 * 1024;
constexpr std::size_t queue_records = 8192;
constexpr std::size_t max_encoded_metadata = 4096;
std::atomic<std::uint64_t> allocations{}, allocated_bytes{};

#ifndef SBMDR_NO_NEW_COUNTER
// Counts C++ new requests. C malloc and aligned allocations are outside this counter.
void* operator new(std::size_t size) {
    if (void* ptr = std::malloc(std::max(size, std::size_t{1}))) {
        allocations.fetch_add(1, std::memory_order_relaxed);
        allocated_bytes.fetch_add(size, std::memory_order_relaxed);
        return ptr;
    }
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* ptr) noexcept { std::free(ptr); }
void operator delete[](void* ptr) noexcept { std::free(ptr); }
void operator delete(void* ptr, std::size_t) noexcept { std::free(ptr); }
void operator delete[](void* ptr, std::size_t) noexcept { std::free(ptr); }
#endif

template <class Clock> auto ns() {
    return duration_cast<nanoseconds>(Clock::now().time_since_epoch()).count();
}

Json canonical(Json value) {
    if (value.is_object()) {
        std::vector<std::string> keys;
        for (auto it = value.begin(); it != value.end(); ++it)
            keys.push_back(it.key());
        std::sort(keys.begin(), keys.end(), [](const auto& a, const auto& b) {
            if (a.size() != b.size())
                return a.size() < b.size();
            return a < b;
        });
        Json result = Json::object();
        for (const auto& key : keys)
            result[key] = canonical(std::move(value[key]));
        return result;
    }
    if (value.is_array()) {
        for (auto& item : value)
            item = canonical(std::move(item));
    }
    return value;
}

void big_endian(Bytes& bytes, std::uint32_t value, int count) {
    for (int i = count - 1; i >= 0; --i) {
        bytes.push_back(static_cast<std::uint8_t>(value >> (i * 8)));
    }
}

std::array<Bytes, 2> framed(Json value, Bytes prefix, std::size_t limit) {
    Bytes body;
    if (auto payload = value.find("raw_payload"); payload != value.end()) {
        body.reserve(payload->get_binary().size() + 1024);
    }
    Json::to_cbor(canonical(std::move(value)), body);
    if (body.size() > limit)
        throw std::runtime_error("Raw v1 body exceeds limit");
    big_endian(prefix, static_cast<std::uint32_t>(body.size()), 4);
    if (prefix.size() == 4)
        big_endian(prefix, 0, 4); // frame flags and reserved
    auto crc = absl::ComputeCrc32c(
        absl::string_view(reinterpret_cast<const char*>(prefix.data()), prefix.size()));
    crc = absl::ExtendCrc32c(crc,
                             absl::string_view(reinterpret_cast<const char*>(body.data()), body.size()));
    big_endian(prefix, static_cast<std::uint32_t>(crc), 4);
    return {std::move(prefix), std::move(body)};
}

constexpr std::array<std::uint8_t, 16> chunk_prefix{'B', 'M', 'R',  'C',  'H', 'N', 'K', 0x1a,
                                                    1,   0,   0xfe, 0xff, 0,   0,   0,   0};

void write_all(int fd, std::array<Bytes, 2> parts) {
    std::array<iovec, 2> vectors{{
        {parts[0].data(), parts[0].size()},
        {parts[1].data(), parts[1].size()},
    }};
    std::size_t first = 0;
    while (first < vectors.size()) {
        const auto count =
            ::writev(fd, vectors.data() + first, static_cast<int>(vectors.size() - first));
        if (count < 0 && errno == EINTR)
            continue;
        if (count <= 0)
            throw std::runtime_error("Raw writev failed");
        auto consumed = static_cast<std::size_t>(count);
        while (first < vectors.size() && consumed >= vectors[first].iov_len) {
            consumed -= vectors[first++].iov_len;
        }
        if (consumed) {
            auto& current = vectors[first];
            current.iov_base = static_cast<char*>(current.iov_base) + consumed;
            current.iov_len -= consumed;
        }
    }
}

struct Message {
    bool rest = false;
    Bytes body;
    Json sequence = Json::object();
    Json flags = Json::array();
    Json exchange_time = nullptr;
    Json transaction_time = nullptr;
    std::int64_t utc = 0, mono = 0, request_utc = 0, request_mono = 0;
    long status = 0;
    bool rest_valid = false;
    std::string content_type;
};

std::string base64(const Bytes& body) {
    std::string out(4 * ((body.size() + 2) / 3) + 1, '\0');
    const int length = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(out.data()), body.data(),
                                       static_cast<int>(body.size()));
    out.resize(static_cast<std::size_t>(length));
    return out;
}

class RawFile {
  public:
    ~RawFile() {
        if (fd_ >= 0)
            ::close(fd_);
    }
    RawFile() = default;
    RawFile(const RawFile&) = delete;

    void append(const std::filesystem::path& path, const Message& message, const std::string& run_id) {
        const auto stream = message.rest ? "rest_observations" : "ws_market_c0_depth";
        const auto symbol = message.rest ? "__GLOBAL__" : "BTCUSDT";
        if (fd_ < 0) {
            Bytes id(16);
            if (RAND_bytes(id.data(), static_cast<int>(id.size())) != 1) {
                throw std::runtime_error("chunk id generation failed");
            }
            id[6] = (id[6] & 0x0f) | 0x40;
            id[8] = (id[8] & 0x3f) | 0x80;
            fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
            if (fd_ < 0)
                throw std::runtime_error("Raw open failed");
            write_all(fd_, framed(
                               {
                                   {"format", "bmdr-raw-chunk"},
                                   {"chunk_schema_version", "raw-chunk.v1"},
                                   {"envelope_schema_version", "event-envelope.v1"},
                                   {"chunk_id", Json::binary(std::move(id))},
                                   {"created_at_utc_ns", ns<system_clock>()},
                                   {"collector_instance_id", run_id},
                                   {"collector_version", "c0"},
                                   {"market", "spot"},
                                   {"symbol", symbol},
                                   {"stream", stream},
                                   {"max_frame_bytes", frame_limit},
                               },
                               Bytes(chunk_prefix.begin(), chunk_prefix.end()), 65536));
        }

        Json envelope = {
            {"schema_version", "event-envelope.v1"},
            {"venue", "binance"},
            {"market", "spot"},
            {"symbol", symbol},
            {"stream", stream},
            {"module", message.rest ? "binance.public.rest.v1" : "binance.public.websocket.v1"},
            {"connection_id", run_id + (message.rest ? "-rest" : "-ws")},
            {"collector_instance_id", run_id},
            {"collector_version", "c0"},
            {"receive_time_utc_ns", message.utc},
            {"receive_monotonic_ns", message.mono},
            {"exchange_event_time", message.exchange_time},
            {"exchange_transaction_time", message.transaction_time},
            {"exchange_trade_time", nullptr},
            {"source_sequence", message.sequence},
            {"payload_encoding", message.rest ? "utf-8-json-provenance" : "utf-8-json-combined.v1"},
            {"capture_flags", message.flags},
        };
        if (message.rest) {
            const auto provenance =
                Json{
                    {"schema_version", "binance-public-rest-provenance.v1"},
                    {"representation", "http-body-base64.v1"},
                    {"family", "S01"},
                    {"source_stream", "exchange_info"},
                    {"scope", symbol},
                    {"venue_market", "spot"},
                    {"request",
                     {{"request_id", run_id + "-rest-1"},
                      {"method", "GET"},
                      {"path", "/api/v3/exchangeInfo"},
                      {"parameters", Json::object()},
                      {"request_time_utc_ns", message.request_utc},
                      {"request_monotonic_ns", message.request_mono},
                      {"timeout_ms", 10000}}},
                    {"response",
                     {{"status", message.status},
                      {"headers", {{"content-type", message.content_type}}},
                      {"raw_body_base64", base64(message.body)},
                      {"valid", message.rest_valid},
                      {"error_type", message.rest_valid ? Json(nullptr) : Json("InvalidResponse")},
                      {"receive_time_utc_ns", message.utc},
                      {"receive_monotonic_ns", message.mono}}},
                    {"transport",
                     {{"kind", "libcurl_http_response_body"},
                      {"raw_http_body_available", true},
                      {"content_decoding", false}}},
                }
                    .dump();
            envelope["raw_payload"] = Json::binary(Bytes(provenance.begin(), provenance.end()));
        } else {
            envelope["raw_payload"] = Json::binary(message.body);
        }
        write_all(fd_, framed(std::move(envelope), {}, frame_limit));
    }

    void finish() {
        if (fd_ < 0)
            return;
        if (::fsync(fd_) != 0)
            throw std::runtime_error("Raw fsync failed");
        if (::close(std::exchange(fd_, -1)) != 0)
            throw std::runtime_error("Raw close failed");
    }

  private:
    int fd_ = -1;
};

class Handoff {
  public:
    void push(Message message) {
        std::unique_lock lock(mutex_);
        const auto size = message.body.size();
        ready_.wait(lock, [&] {
            return closed_ || (pending_.size() < queue_records && bytes_ + size <= queue_limit);
        });
        if (closed_)
            throw std::runtime_error("storage handoff closed");
        bytes_ += size;
        pending_.push_back(std::move(message));
        peak_bytes = std::max(peak_bytes, bytes_);
        peak_records = std::max(peak_records, pending_.size());
        ready_.notify_all();
    }

    bool pop(Message& message) {
        std::unique_lock lock(mutex_);
        ready_.wait(lock, [&] { return closed_ || !pending_.empty(); });
        if (pending_.empty())
            return false;
        bytes_ -= pending_.front().body.size();
        message = std::move(pending_.front());
        pending_.pop_front();
        ready_.notify_all();
        return true;
    }

    void close() {
        std::lock_guard lock(mutex_);
        closed_ = true;
        ready_.notify_all();
    }

    std::size_t peak_bytes = 0, peak_records = 0;

  private:
    std::mutex mutex_;
    std::condition_variable ready_;
    std::deque<Message> pending_;
    std::size_t bytes_ = 0;
    bool closed_ = false;
};

void receive_ws(const std::string& port, std::size_t count, Handoff& queue) {
    asio::io_context io;
    beast::websocket::stream<asio::ip::tcp::socket> ws(io);
    beast::get_lowest_layer(ws).connect(
        {asio::ip::make_address("127.0.0.1"), static_cast<unsigned short>(std::stoul(port))});
    ws.read_message_max(frame_limit - max_encoded_metadata);
    ws.handshake("127.0.0.1:" + port, "/stream?streams=btcusdt@depth@100ms");
    simdjson::dom::parser parser;
    beast::flat_buffer buffer;
    for (std::size_t i = 0; i < count; ++i) {
        ws.read(buffer);
        Message message;
        message.utc = ns<system_clock>();
        message.mono = ns<steady_clock>();
        const std::string_view wire(static_cast<const char*>(buffer.data().data()), buffer.size());
        message.body.assign(wire.begin(), wire.end());
        simdjson::dom::element root, data;
        std::string_view stream, event, symbol;
        std::uint64_t time, first, last;
        simdjson::dom::array bids, asks;
        if (parser.parse(wire.data(), wire.size()).get(root) || root["stream"].get(stream) ||
            stream != "btcusdt@depth@100ms" || root["data"].get(data) || data["e"].get(event) ||
            event != "depthUpdate" || data["s"].get(symbol) || symbol != "BTCUSDT" ||
            data["E"].get(time) || time > std::numeric_limits<std::int64_t>::max() ||
            data["U"].get(first) || data["u"].get(last) || first > last || data["b"].get(bids) ||
            data["a"].get(asks)) {
            message.flags.push_back("malformed");
        } else {
            message.exchange_time = time;
            message.sequence = {{"U", first},
                                {"u", last},
                                {"sourceStream", "diff_depth"},
                                {"sourceFamily", "S02"},
                                {"wireStream", stream}};
            auto transaction = data["T"];
            std::uint64_t value;
            if (transaction.error() != simdjson::NO_SUCH_FIELD && !transaction.is_null()) {
                if (transaction.get(value) || value > std::numeric_limits<std::int64_t>::max()) {
                    message.flags.push_back("malformed");
                    message.exchange_time = nullptr;
                } else {
                    message.transaction_time = value;
                }
            }
        }
        queue.push(std::move(message));
        buffer.consume(buffer.size());
    }
    ws.close(beast::websocket::close_code::normal);
}

std::size_t receive_body(char* data, std::size_t size, std::size_t count, void* context) noexcept {
    auto& body = *static_cast<Bytes*>(context);
    const auto bytes = size * count;
    // Base64 plus provenance and CBOR must fit the 32 MiB encoded frame.
    constexpr auto body_limit = (frame_limit - max_encoded_metadata) / 4 * 3;
    if (bytes > body_limit - body.size())
        return 0;
    try {
        body.insert(body.end(), data, data + bytes);
    } catch (...) {
        return 0;
    }
    return bytes;
}

void receive_rest(const std::string& port, Handoff& queue) {
    using Easy = std::unique_ptr<CURL, decltype(&curl_easy_cleanup)>;
    using Multi = std::unique_ptr<CURLM, decltype(&curl_multi_cleanup)>;
    Easy easy(curl_easy_init(), curl_easy_cleanup);
    Multi multi(curl_multi_init(), curl_multi_cleanup);
    if (!easy || !multi)
        throw std::runtime_error("curl initialization failed");
    auto check = [](CURLMcode code) {
        if (code != CURLM_OK)
            throw std::runtime_error(curl_multi_strerror(code));
    };

    Message message;
    message.rest = true;
    const auto url = "http://127.0.0.1:" + port + "/api/v3/exchangeInfo";
    auto option = [&](CURLoption key, auto value) {
        const auto code = curl_easy_setopt(easy.get(), key, value);
        if (code != CURLE_OK)
            throw std::runtime_error(curl_easy_strerror(code));
    };
    option(CURLOPT_URL, url.c_str());
    option(CURLOPT_PROXY, "");
    option(CURLOPT_HTTP_CONTENT_DECODING, 0L);
    option(CURLOPT_TIMEOUT_MS, 10000L);
    option(CURLOPT_WRITEFUNCTION, receive_body);
    option(CURLOPT_WRITEDATA, &message.body);
    check(curl_multi_add_handle(multi.get(), easy.get()));
    message.request_utc = ns<system_clock>();
    message.request_mono = ns<steady_clock>();
    int running = 0;
    check(curl_multi_perform(multi.get(), &running));
    while (running) {
        check(curl_multi_poll(multi.get(), nullptr, 0, 1000, nullptr));
        check(curl_multi_perform(multi.get(), &running));
    }
    message.utc = ns<system_clock>();
    message.mono = ns<steady_clock>();
    int remaining = 0;
    const auto* result = curl_multi_info_read(multi.get(), &remaining);
    if (!result || result->msg != CURLMSG_DONE || result->data.result != CURLE_OK) {
        throw std::runtime_error("REST transfer failed or response exceeded the body limit");
    }
    char* type = nullptr;
    curl_easy_getinfo(easy.get(), CURLINFO_RESPONSE_CODE, &message.status);
    curl_easy_getinfo(easy.get(), CURLINFO_CONTENT_TYPE, &type);
    if (type)
        message.content_type.assign(type, strnlen(type, 256));
    check(curl_multi_remove_handle(multi.get(), easy.get()));
    simdjson::dom::parser parser;
    simdjson::dom::element root;
    simdjson::dom::array symbols, limits;
    message.flags = {"rest_poll", "exact_rest_http_body_preserved"};
    if (message.status != 200 ||
        parser.parse(reinterpret_cast<const char*>(message.body.data()), message.body.size())
            .get(root) ||
        root["symbols"].get(symbols) || root["rateLimits"].get(limits)) {
        message.flags.push_back("source_failed");
        message.flags.push_back("malformed");
    } else {
        message.rest_valid = true;
    }
    message.sequence = {{"sourceStream", "exchange_info"}, {"sourceFamily", "S01"}};
    queue.push(std::move(message));
}

void verify_golden(const char* path) {
    std::ifstream input(path);
    const auto hex = Json::parse(input).at("chunk_hex").get<std::string>();
    Bytes bytes;
    for (std::size_t i = 0; i < hex.size(); i += 2) {
        bytes.push_back(static_cast<std::uint8_t>(std::stoul(hex.substr(i, 2), nullptr, 16)));
    }
    std::uint32_t header_size = 0;
    for (std::size_t i = 16; i < 20; ++i)
        header_size = (header_size << 8) | bytes.at(i);
    if (bytes.size() < 36 || header_size > bytes.size() - 36) {
        throw std::runtime_error("truncated Raw golden");
    }
    const auto frame = bytes.begin() + 24 + header_size;
    auto header = framed(Json::from_cbor(bytes.begin() + 24, frame),
                         Bytes(chunk_prefix.begin(), chunk_prefix.end()), 65536);
    auto record = framed(Json::from_cbor(frame + 12, bytes.end()), {}, frame_limit);
    Bytes rebuilt;
    for (const auto& parts : {header, record}) {
        for (const auto& part : parts)
            rebuilt.insert(rebuilt.end(), part.begin(), part.end());
    }
    if (rebuilt != bytes)
        throw std::runtime_error("Raw golden mismatch");
}

int main(int argc, char** argv) try {
    if (argc == 3 && std::string_view(argv[1]) == "--golden") {
        verify_golden(argv[2]);
        return 0;
    }
    if (argc != 6)
        throw std::runtime_error(
            "usage: sbmdr-c0 WS_PORT HTTP_PORT WS_COUNT OUTPUT_DIR STORAGE_PAUSE_MS");
    const auto ws_count = std::stoul(argv[3]);
    const std::filesystem::path output(argv[4]);
    const auto pause = milliseconds(std::stoul(argv[5]));
    std::filesystem::create_directories(output);
    if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
        throw std::runtime_error("curl global initialization failed");
    }
    struct CurlCleanup {
        ~CurlCleanup() { curl_global_cleanup(); }
    } cleanup;
    Bytes run_bytes(16);
    if (RAND_bytes(run_bytes.data(), static_cast<int>(run_bytes.size())) != 1) {
        throw std::runtime_error("run id generation failed");
    }
    const std::string run_id = "c0-" + base64(run_bytes);
    const auto start_wall = steady_clock::now();
    const auto start_cpu = std::clock();
    const auto start_allocations = allocations.load();
    const auto start_bytes = allocated_bytes.load();
    Handoff queue;
    std::size_t records = 0;
    std::int64_t encode_write_ns = 0, sync_ns = 0, max_receive_to_sync_ns = 0;
    auto writer = std::async(std::launch::async, [&] {
        try {
            std::this_thread::sleep_for(pause);
            RawFile ws, rest;
            Message message;
            auto oldest = std::numeric_limits<std::int64_t>::max();
            while (queue.pop(message)) {
                const auto before = ns<steady_clock>();
                oldest = std::min(oldest, message.mono);
                if (message.rest)
                    rest.append(output / "rest.bmdr.partial", message, run_id);
                else
                    ws.append(output / "ws.bmdr.partial", message, run_id);
                encode_write_ns += ns<steady_clock>() - before;
                ++records;
            }
            const auto before = ns<steady_clock>();
            ws.finish();
            rest.finish();
            const auto after = ns<steady_clock>();
            sync_ns = after - before;
            max_receive_to_sync_ns = after - oldest;
        } catch (...) {
            queue.close();
            throw;
        }
    });
    std::exception_ptr input_error;
    try {
        if (ws_count)
            receive_ws(argv[1], ws_count, queue);
        receive_rest(argv[2], queue);
    } catch (...) {
        input_error = std::current_exception();
    }
    queue.close();
    writer.get();
    if (input_error)
        std::rethrow_exception(input_error);
    struct rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) != 0)
        throw std::runtime_error("getrusage failed");
#ifdef __APPLE__
    const auto rss = usage.ru_maxrss;
#else
    const auto rss = usage.ru_maxrss * 1024;
#endif
    std::cout << Json{
        {"records", records}, {"wall_ms", duration<double, std::milli>(steady_clock::now() - start_wall).count()},
        {"cpu_ms", 1000.0 * (std::clock() - start_cpu) / CLOCKS_PER_SEC}, {"peak_rss_bytes", rss},
        {"new_calls", allocations.load() - start_allocations},
        {"new_requested_bytes", allocated_bytes.load() - start_bytes},
        {"peak_queue_payload_bytes", queue.peak_bytes}, {"peak_queue_records", queue.peak_records},
        {"encode_write_ns", encode_write_ns}, {"final_fsync_close_ns", sync_ns},
        {"max_receive_to_final_fsync_ns", max_receive_to_sync_ns},
    }.dump() << '\n';
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
