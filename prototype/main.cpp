#include <absl/crc/crc32c.h>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <simdjson.h>
#include <sqlite3.h>
#include <zstd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
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
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <strings.h>
#include <sys/file.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

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
    bool symbol_scoped = false;
    unsigned generation = 0;
    Bytes body;
    Json sequence = Json::object();
    Json flags = Json::array();
    Json exchange_time = nullptr;
    Json transaction_time = nullptr;
    std::int64_t utc = 0, mono = 0, request_utc = 0, request_mono = 0;
    long status = 0;
    bool rest_valid = false;
    std::string content_type, retry_after;
};

std::string base64(const Bytes& body) {
    std::string out(4 * ((body.size() + 2) / 3) + 1, '\0');
    const int length = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(out.data()), body.data(),
                                       static_cast<int>(body.size()));
    out.resize(static_cast<std::size_t>(length));
    return out;
}

std::string hex(const Bytes& bytes) {
    constexpr std::string_view digits = "0123456789abcdef";
    std::string result;
    result.reserve(bytes.size() * 2);
    for (auto value : bytes) {
        result += digits[value >> 4];
        result += digits[value & 15];
    }
    return result;
}

Bytes uuid_bytes() {
    Bytes id(16);
    if (RAND_bytes(id.data(), static_cast<int>(id.size())) != 1)
        throw std::runtime_error("UUID generation failed");
    id[6] = (id[6] & 0x0f) | 0x40;
    id[8] = (id[8] & 0x3f) | 0x80;
    return id;
}

std::string uuid_text(const Bytes& id) {
    auto value = hex(id);
    for (auto offset : {20, 16, 12, 8})
        value.insert(offset, "-");
    return value;
}

class Sha256 {
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> context_{EVP_MD_CTX_new(), EVP_MD_CTX_free};

  public:
    Sha256() {
        if (!context_ || EVP_DigestInit_ex(context_.get(), EVP_sha256(), nullptr) != 1)
            throw std::runtime_error("SHA-256 initialization failed");
    }
    void add(const void* data, std::size_t size) {
        if (EVP_DigestUpdate(context_.get(), data, size) != 1)
            throw std::runtime_error("SHA-256 update failed");
    }
    std::string finish() {
        Bytes digest(32);
        if (EVP_DigestFinal_ex(context_.get(), digest.data(), nullptr) != 1)
            throw std::runtime_error("SHA-256 finalization failed");
        return hex(digest);
    }
};

void sync_directory(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0)
        throw std::runtime_error("directory open failed");
    const int sync_result = ::fsync(fd);
    const int close_result = ::close(fd);
    if (sync_result != 0 || close_result != 0)
        throw std::runtime_error("directory fsync failed");
}

std::string connection_id(const Message& message, const std::string& run_id) {
    if (message.rest)
        return run_id + "-rest";
    return run_id + "-ws" + (message.generation ? "-" + std::to_string(message.generation) : "");
}

class RawFile {
  public:
    ~RawFile() {
        if (fd_ >= 0)
            ::close(fd_);
    }
    RawFile() = default;
    RawFile(const RawFile&) = delete;

    void append(const std::filesystem::path& path, const Message& message, const std::string& run_id,
                std::string_view version = "c0") {
        const auto stream = message.rest ? "rest_observations" : "ws_market_c0_depth";
        if (message.rest && !message.symbol_scoped)
            symbol = "__GLOBAL__";
        if (fd_ < 0) {
            const auto& id = identity;
            created = ns<system_clock>();
            fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
            if (fd_ < 0)
                throw std::runtime_error("Raw open failed");
            write(framed(
                {
                    {"format", "bmdr-raw-chunk"},
                    {"chunk_schema_version", "raw-chunk.v1"},
                    {"envelope_schema_version", "event-envelope.v1"},
                    {"chunk_id", Json::binary(id)},
                    {"created_at_utc_ns", created},
                    {"collector_instance_id", run_id},
                    {"collector_version", version},
                    {"market", "spot"},
                    {"symbol", symbol},
                    {"stream", stream},
                    {"max_frame_bytes", frame_limit},
                },
                Bytes(chunk_prefix.begin(), chunk_prefix.end()), 65536));
            if (::fsync(fd_) != 0)
                throw std::runtime_error("Raw header fsync failed");
            sync_directory(path.parent_path());
        }

        Json envelope = {
            {"schema_version", "event-envelope.v1"},
            {"venue", "binance"},
            {"market", "spot"},
            {"symbol", symbol},
            {"stream", stream},
            {"module", message.rest ? "binance.public.rest.v1" : "binance.public.websocket.v1"},
            {"connection_id", connection_id(message, run_id)},
            {"collector_instance_id", run_id},
            {"collector_version", version},
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
                     {{"request_id", run_id + "-rest-" + std::to_string(message.request_mono)},
                      {"method", "GET"},
                      {"path", "/api/v3/exchangeInfo"},
                      {"parameters",
                       message.symbol_scoped ? Json{{"symbol", "BTCUSDT"}} : Json::object()},
                      {"request_time_utc_ns", message.request_utc},
                      {"request_monotonic_ns", message.request_mono},
                      {"timeout_ms", 10000}}},
                    {"response",
                     {{"status", message.status},
                      {"headers", message.retry_after.empty()
                                      ? Json{{"content-type", message.content_type}}
                                      : Json{{"content-type", message.content_type},
                                             {"retry-after", message.retry_after}}},
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
        write(framed(std::move(envelope), {}, frame_limit));
        ++records;
        connections.insert(connection_id(message, run_id));
        update_range(utc_range, message.utc);
        update_range(mono_range, message.mono);
        if (!message.exchange_time.is_null())
            update_range(exchange_ranges["event"], message.exchange_time);
        if (!message.transaction_time.is_null())
            update_range(exchange_ranges["transaction"], message.transaction_time);
        for (auto it = message.sequence.begin(); it != message.sequence.end(); ++it)
            update_range(sequence_ranges[it.key()], it.value());
        for (const auto& flag : message.flags)
            flags.insert(flag.get<std::string>());
        pending_since = std::min(pending_since, message.mono);
    }

    std::int64_t sync() {
        if (fd_ < 0 || pending_since == std::numeric_limits<std::int64_t>::max())
            return 0;
        if (::fsync(fd_) != 0)
            throw std::runtime_error("Raw fsync failed");
        fsync_utc = ns<system_clock>();
        return ns<steady_clock>() -
               std::exchange(pending_since, std::numeric_limits<std::int64_t>::max());
    }

    std::int64_t finish() {
        if (fd_ < 0)
            return 0;
        const auto latency = sync();
        if (::close(std::exchange(fd_, -1)) != 0)
            throw std::runtime_error("Raw close failed");
        return latency;
    }

    std::string symbol = "BTCUSDT";
    const Bytes identity = uuid_bytes();
    const std::string chunk_id = uuid_text(identity);
    std::uint64_t bytes = 0, records = 0;
    std::int64_t created = 0, fsync_utc = 0;
    Json utc_range, mono_range, exchange_ranges = Json::object(), sequence_ranges = Json::object();
    std::set<std::string> flags, connections;
    Sha256 digest;

  private:
    static void update_range(Json& range, const Json& value) {
        if (range.is_null())
            range = {{"min", value}, {"max", value}};
        else {
            if (value < range["min"])
                range["min"] = value;
            if (value > range["max"])
                range["max"] = value;
        }
    }
    void write(std::array<Bytes, 2> parts) {
        for (const auto& part : parts) {
            digest.add(part.data(), part.size());
            bytes += part.size();
        }
        write_all(fd_, std::move(parts));
    }
    std::int64_t pending_since = std::numeric_limits<std::int64_t>::max();
    int fd_ = -1;
};

#include "recorder_storage.hpp"

class IngressOverload : public std::runtime_error {
  public:
    explicit IngressOverload(bool rest_source)
        : std::runtime_error(rest_source ? "INGRESS_OVERLOAD REST S01"
                                         : "INGRESS_OVERLOAD WS S02 BTCUSDT"),
          rest(rest_source) {}
    const bool rest;
};

class Handoff {
  public:
    void push(Message message, bool bounded = false) {
        std::unique_lock lock(mutex_);
        const auto size = message.body.size();
        const auto available = [&] {
            return closed_ || (pending_.size() < queue_records && bytes_ + size <= queue_limit);
        };
        if (bounded) {
            if (!ready_.wait_for(lock, milliseconds(250), available))
                throw IngressOverload(message.rest);
        } else {
            ready_.wait(lock, available);
        }
        if (closed_)
            throw std::runtime_error("storage handoff closed");
        bytes_ += size;
        pending_.push_back(std::move(message));
        peak_bytes = std::max(peak_bytes, bytes_);
        peak_records = std::max(peak_records, pending_.size());
        ready_.notify_all();
    }

    bool rest_admissible() {
        std::lock_guard lock(mutex_);
        return !closed_ && pending_.size() < queue_records / 2 && bytes_ < queue_limit / 2;
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

    bool take(std::vector<Message>& batch) {
        std::unique_lock lock(mutex_);
        ready_.wait_for(lock, milliseconds(50), [&] { return closed_ || !pending_.empty(); });
        std::size_t batch_bytes = 0;
        constexpr std::size_t batch_limit = 1024 * 1024;
        while (!pending_.empty() && batch.size() < 32) {
            const auto next = pending_.front().body.size();
            if (!batch.empty() && batch_bytes + next > batch_limit)
                break;
            batch_bytes += next;
            bytes_ -= next;
            batch.push_back(std::move(pending_.front()));
            pending_.pop_front();
            if (batch_bytes >= batch_limit)
                break;
        }
        ready_.notify_all();
        return !closed_ || !batch.empty();
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

template <class Stream>
void read_ws(Stream& ws, asio::io_context& io, std::size_t count, Handoff& queue, std::stop_token stop,
             std::size_t* accepted, unsigned generation) {
    ws.read_message_max(frame_limit - max_encoded_metadata);
    simdjson::dom::parser parser;
    beast::flat_buffer buffer;
    std::optional<std::uint64_t> previous;
    const auto renew = steady_clock::now() + hours(23);
    for (std::size_t i = 0; i < count && !stop.stop_requested(); ++i) {
        if (stop.stop_possible()) {
            boost::system::error_code result;
            bool complete = false;
            ws.async_read(buffer, [&](boost::system::error_code error, std::size_t) {
                result = error;
                complete = true;
            });
            io.restart();
            const auto deadline = std::min(renew, steady_clock::now() + seconds(30));
            while (!complete && !stop.stop_requested() && steady_clock::now() < deadline)
                io.run_one_for(milliseconds(50));
            if (!complete) {
                boost::system::error_code ignored;
                beast::get_lowest_layer(ws).socket().close(ignored);
                io.restart();
                io.run();
                if (!stop.stop_requested())
                    throw boost::system::system_error(asio::error::timed_out);
                return;
            }
            if (result)
                throw boost::system::system_error(result);
        } else {
            ws.read(buffer);
        }
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
        if (stop.stop_possible()) {
            if (!message.flags.empty()) {
                message.flags.push_back("sequence_gap");
                previous.reset();
            } else {
                if (previous && first > *previous && first - *previous > 1)
                    message.flags.push_back("sequence_gap");
                previous = previous ? std::max(*previous, last) : last;
            }
        }
        message.generation = generation;
        if (generation && i == 0)
            message.flags.push_back("sequence_gap");
        queue.push(std::move(message), stop.stop_possible());
        if (accepted)
            ++*accepted;
        buffer.consume(buffer.size());
    }
    if (!stop.stop_possible())
        ws.close(beast::websocket::close_code::normal);
}

void receive_ws(const std::string& port, std::size_t count, Handoff& queue, std::stop_token stop = {},
                const std::optional<std::string>& ca = std::nullopt, std::size_t* accepted = nullptr,
                unsigned generation = 0) {
    asio::io_context io;
    constexpr auto target = "/stream?streams=btcusdt@depth@100ms";
    auto bounded = [&](auto start, auto cancel) {
        std::optional<boost::system::error_code> result;
        io.restart();
        start([&](boost::system::error_code error, auto&&...) { result = error; });
        const auto deadline = steady_clock::now() + seconds(10);
        while (!result && !stop.stop_requested() && steady_clock::now() < deadline)
            io.run_one_for(milliseconds(50));
        if (!result) {
            cancel();
            io.restart();
            io.run();
            if (stop.stop_requested())
                return false;
            throw boost::system::system_error(asio::error::timed_out);
        }
        if (*result)
            throw boost::system::system_error(*result);
        return true;
    };
    if (ca) {
        constexpr auto host = "stream.binance.com";
        asio::ssl::context tls(asio::ssl::context::tls_client);
        tls.set_verify_mode(asio::ssl::verify_peer);
        tls.load_verify_file(*ca);
        beast::websocket::stream<asio::ssl::stream<beast::tcp_stream>> ws(io, tls);
        ws.next_layer().set_verify_callback(asio::ssl::host_name_verification(host));
        if (SSL_set_tlsext_host_name(ws.next_layer().native_handle(), host) != 1)
            throw std::runtime_error("TLS SNI configuration failed");
        auto& tcp = beast::get_lowest_layer(ws);
        const auto close_socket = [&] {
            boost::system::error_code ignored;
            tcp.socket().close(ignored);
        };
        asio::ip::tcp::resolver resolver(io);
        asio::ip::tcp::resolver::results_type addresses;
        if (!bounded(
                [&](auto done) {
                    resolver.async_resolve(host, "9443", [&, done](auto error, auto found) {
                        addresses = std::move(found);
                        done(error);
                    });
                },
                [&] { resolver.cancel(); }))
            return;
        if (!bounded([&](auto done) { tcp.async_connect(addresses, done); }, close_socket))
            return;
        if (!bounded(
                [&](auto done) {
                    ws.next_layer().async_handshake(asio::ssl::stream_base::client, done);
                },
                close_socket))
            return;
        ws.set_option(beast::websocket::stream_base::timeout::suggested(beast::role_type::client));
        if (!bounded([&](auto done) { ws.async_handshake("stream.binance.com:9443", target, done); },
                     close_socket))
            return;
        read_ws(ws, io, count, queue, stop, accepted, generation);
    } else {
        beast::websocket::stream<beast::tcp_stream> ws(io);
        auto& tcp = beast::get_lowest_layer(ws);
        const asio::ip::tcp::endpoint endpoint{asio::ip::make_address("127.0.0.1"),
                                               static_cast<unsigned short>(std::stoul(port))};
        const auto host = "127.0.0.1:" + port;
        if (stop.stop_possible()) {
            const auto close_socket = [&] {
                boost::system::error_code ignored;
                tcp.socket().close(ignored);
            };
            if (!bounded([&](auto done) { tcp.async_connect(endpoint, done); }, close_socket) ||
                !bounded([&](auto done) { ws.async_handshake(host, target, done); }, close_socket))
                return;
        } else {
            tcp.connect(endpoint);
            ws.handshake(host, target);
        }
        read_ws(ws, io, count, queue, stop, accepted, generation);
    }
}

struct HttpBody {
    Bytes bytes;
    bool too_large = false;
};

std::size_t receive_body(char* data, std::size_t size, std::size_t count, void* context) noexcept {
    auto& response = *static_cast<HttpBody*>(context);
    const auto bytes = size * count;
    // Base64 plus provenance and CBOR must fit the 32 MiB encoded frame.
    constexpr auto body_limit = (frame_limit - max_encoded_metadata) / 4 * 3;
    if (bytes > body_limit - response.bytes.size()) {
        response.too_large = true;
        return 0;
    }
    try {
        response.bytes.insert(response.bytes.end(), data, data + bytes);
    } catch (...) {
        return 0;
    }
    return bytes;
}

std::size_t receive_header(char* data, std::size_t size, std::size_t count, void* context) noexcept {
    const std::string_view line(data, size * count);
    constexpr std::string_view field = "Retry-After:";
    auto& value = *static_cast<std::string*>(context);
    try {
        if (line.starts_with("HTTP/"))
            value.clear();
        if (line.size() > field.size() && strncasecmp(data, field.data(), field.size()) == 0) {
            auto header = line.substr(field.size());
            const auto first = header.find_first_not_of(" \t\r\n");
            const auto last = header.find_last_not_of(" \t\r\n");
            if (first != std::string_view::npos && last - first < 128)
                value.assign(header.substr(first, last - first + 1));
        }
    } catch (...) {
        return 0;
    }
    return line.size();
}

struct RestResult {
    long status;
    bool valid;
    std::string retry_after;
    std::string transfer_error;
};

using RestMulti = std::unique_ptr<CURLM, decltype(&curl_multi_cleanup)>;

RestMulti make_rest_multi() {
    RestMulti multi(curl_multi_init(), curl_multi_cleanup);
    if (!multi)
        throw std::runtime_error("curl multi initialization failed");
    return multi;
}

RestResult receive_rest(CURLM* multi, const std::string& port, Handoff& queue,
                        const std::optional<std::string>& ca = std::nullopt, bool bounded = false) {
    using Easy = std::unique_ptr<CURL, decltype(&curl_easy_cleanup)>;
    Easy easy(curl_easy_init(), curl_easy_cleanup);
    if (!easy)
        throw std::runtime_error("curl easy initialization failed");
    auto check = [](CURLMcode code) {
        if (code != CURLM_OK)
            throw std::runtime_error(curl_multi_strerror(code));
    };

    Message message;
    message.rest = true;
    message.symbol_scoped = ca.has_value();
    HttpBody response;
    const auto url = ca ? "https://api.binance.com/api/v3/exchangeInfo?symbol=BTCUSDT"
                        : "http://127.0.0.1:" + port + "/api/v3/exchangeInfo";
    auto option = [&](CURLoption key, auto value) {
        const auto code = curl_easy_setopt(easy.get(), key, value);
        if (code != CURLE_OK)
            throw std::runtime_error(curl_easy_strerror(code));
    };
    option(CURLOPT_URL, url.c_str());
    option(CURLOPT_PROXY, "");
    if (ca)
        option(CURLOPT_CAINFO, ca->c_str());
    option(CURLOPT_HTTP_CONTENT_DECODING, 0L);
    option(CURLOPT_TIMEOUT_MS, 10000L);
    option(CURLOPT_WRITEFUNCTION, receive_body);
    option(CURLOPT_WRITEDATA, &response);
    option(CURLOPT_HEADERFUNCTION, receive_header);
    option(CURLOPT_HEADERDATA, &message.retry_after);
    check(curl_multi_add_handle(multi, easy.get()));
    struct Attached {
        CURLM* multi;
        CURL* easy;
        ~Attached() { curl_multi_remove_handle(multi, easy); }
    } attached{multi, easy.get()};
    message.request_utc = ns<system_clock>();
    message.request_mono = ns<steady_clock>();
    int running = 0;
    check(curl_multi_perform(multi, &running));
    while (running) {
        check(curl_multi_poll(multi, nullptr, 0, 1000, nullptr));
        check(curl_multi_perform(multi, &running));
    }
    message.utc = ns<system_clock>();
    message.mono = ns<steady_clock>();
    int remaining = 0;
    const auto* result = curl_multi_info_read(multi, &remaining);
    curl_easy_getinfo(easy.get(), CURLINFO_RESPONSE_CODE, &message.status);
    std::string transfer_error;
    if (response.too_large)
        transfer_error = "REST response exceeded the body limit";
    else if (!result || result->msg != CURLMSG_DONE)
        transfer_error = "REST transfer did not complete";
    else if (result->data.result != CURLE_OK)
        transfer_error = std::string("REST transfer failed: ") + curl_easy_strerror(result->data.result);
    if (!transfer_error.empty()) {
        if (bounded && (message.status == 418 || message.status == 429))
            return {message.status, false, std::move(message.retry_after), std::move(transfer_error)};
        throw std::runtime_error(transfer_error);
    }
    message.body = std::move(response.bytes);
    char* type = nullptr;
    curl_easy_getinfo(easy.get(), CURLINFO_RESPONSE_CODE, &message.status);
    curl_easy_getinfo(easy.get(), CURLINFO_CONTENT_TYPE, &type);
    if (type)
        message.content_type.assign(type, strnlen(type, 256));
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
    RestResult outcome{message.status, message.rest_valid, message.retry_after, {}};
    queue.push(std::move(message), bounded);
    return outcome;
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

#include "recorder_run.hpp"

void verify_handoff_limit() {
    Handoff queue;
    for (std::size_t i = 0; i < queue_records; ++i) {
        Message message;
        message.mono = static_cast<std::int64_t>(i);
        queue.push(std::move(message));
    }
    if (queue.rest_admissible())
        throw std::runtime_error("saturated queue admitted REST");
    try {
        queue.push(Message{}, true);
        throw std::runtime_error("saturated queue accepted an extra record");
    } catch (const IngressOverload&) {
    }
    queue.close();
    Message message;
    std::size_t drained = 0;
    while (queue.pop(message)) {
        if (message.mono != static_cast<std::int64_t>(drained++))
            throw std::runtime_error("overload changed queued record order");
    }
    if (drained != queue_records)
        throw std::runtime_error("overload dropped queued records");
}

int main(int argc, char** argv) try {
    if (argc == 2 && std::string_view(argv[1]) == "--check-handoff") {
        verify_handoff_limit();
        return 0;
    }
    if (argc >= 2 && std::string_view(argv[1]) == "record")
        return record(argc, argv);
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
        const auto multi = make_rest_multi();
        receive_rest(multi.get(), argv[2], queue);
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
