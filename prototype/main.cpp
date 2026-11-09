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
#include <cctype>
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
#include <map>
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
constexpr std::size_t configured_queue_records = 65536;
constexpr std::size_t queue_message_overhead = 1024;
constexpr std::size_t max_encoded_metadata = 4096;
void status_line(const Json& event);
std::atomic<std::uint64_t> allocations{}, allocated_bytes{};
#ifdef SBMDR_NO_NEW_COUNTER
constexpr bool allocation_counter_enabled = false;
#else
constexpr bool allocation_counter_enabled = true;
#endif

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

std::array<Bytes, 2> framed(const Json& value, Bytes prefix, std::size_t limit,
                            bool canonical_input = false, Bytes* scratch = nullptr) {
    Bytes body = scratch ? std::move(*scratch) : Bytes{};
    body.clear();
    if (auto payload = value.find("raw_payload"); payload != value.end()) {
        body.reserve(payload->get_binary().size() + 1024);
    }
    if (canonical_input)
        Json::to_cbor(value, body);
    else
        Json::to_cbor(canonical(value), body);
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

void write_all(int fd, const std::array<Bytes, 2>& parts) {
    std::array<iovec, 2> vectors{{
        {const_cast<std::uint8_t*>(parts[0].data()), parts[0].size()},
        {const_cast<std::uint8_t*>(parts[1].data()), parts[1].size()},
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
#ifdef SBMDR_DIAGNOSTICS
    std::int64_t enqueued = 0, rest_preparation_ns = 0;
#endif
    bool rest = false;
    bool symbol_scoped = false;
    unsigned generation = 0;
    Bytes body;
    Json sequence = Json::object();
    Json flags = Json::array();
    Json exchange_time = nullptr;
    Json transaction_time = nullptr;
    Json trade_time = nullptr;
    std::int64_t utc = 0, mono = 0, request_utc = 0, request_mono = 0;
    long status = 0;
    bool rest_valid = false;
    std::string content_type, retry_after;
    std::string connection_group;
    std::string symbol = "BTCUSDT", stream = "ws_market_c0_depth";
    std::string market = "spot", source_stream = "exchange_info", family = "S01";
    std::string path = "/api/v3/exchangeInfo";
    Json parameters = Json::object();
    std::uint64_t snapshot_epoch = 0;
};

#include "recorder_core.hpp"

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

Bytes prepare_rest_payload(const Message& message, const std::string& run_id) {
    const auto provenance =
        Json{
            {"schema_version", "binance-public-rest-provenance.v1"},
            {"representation", "http-body-base64.v1"},
            {"family", message.family},
            {"source_stream", message.source_stream},
            {"scope", message.symbol_scoped ? message.symbol : "__GLOBAL__"},
            {"venue_market", message.market == "spot" ? "spot" : "usdm"},
            {"request",
             {{"request_id", run_id + "-rest-" + std::to_string(message.request_mono)},
              {"method", "GET"},
              {"path", message.path},
              {"parameters", message.parameters},
              {"request_time_utc_ns", message.request_utc},
              {"request_monotonic_ns", message.request_mono},
              {"timeout_ms", 10000}}},
            {"response",
             {{"status", message.status},
              {"headers", message.retry_after.empty() ? Json{{"content-type", message.content_type}}
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
    return Bytes(provenance.begin(), provenance.end());
}

std::string connection_id(const Message& message, const std::string& run_id) {
    if (message.rest)
        return run_id + "-rest";
    return run_id + "-ws" + message.connection_group + (message.generation ? "-" + std::to_string(message.generation) : "");
}

class RawFile {
  public:
    ~RawFile() {
        if (fd_ >= 0)
            ::close(fd_);
    }
    RawFile() = default;
    RawFile(const RawFile&) = delete;

    void append(const std::filesystem::path& path, Message& message, const std::string& run_id,
                std::string_view version = "c0", Bytes* scratch = nullptr) {
        stream = message.rest ? "rest_observations" : message.stream;
        symbol = message.symbol;
        market = message.market;
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
                    {"market", market},
                    {"symbol", symbol},
                    {"stream", stream},
                    {"max_frame_bytes", frame_limit},
                },
                Bytes(chunk_prefix.begin(), chunk_prefix.end()), 65536));
            if (::fsync(fd_) != 0)
                throw std::runtime_error("Raw header fsync failed");
            sync_directory(path.parent_path());
        }

#ifdef SBMDR_DIAGNOSTICS
        const auto preparation_start = ns<steady_clock>();
#endif
        if (envelope_.is_null()) {
            envelope_ = canonical(Json{
                {"schema_version", "event-envelope.v1"},
                {"venue", "binance"},
                {"market", market},
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
                {"exchange_trade_time", message.trade_time},
                {"source_sequence", message.sequence},
                {"payload_encoding", message.rest ? "utf-8-json-provenance" : "utf-8-json-combined.v1"},
                {"capture_flags", message.flags},
                {"raw_payload", nullptr},
            });
        }
        auto& envelope = envelope_;
        envelope["connection_id"] = connection_id(message, run_id);
        envelope["receive_time_utc_ns"] = message.utc;
        envelope["receive_monotonic_ns"] = message.mono;
        envelope["exchange_event_time"] = message.exchange_time;
        envelope["exchange_transaction_time"] = message.transaction_time;
        envelope["exchange_trade_time"] = message.trade_time;
        envelope["source_sequence"] = canonical(message.sequence);
        envelope["capture_flags"] = message.flags;
        envelope["raw_payload"] = Json::binary(std::move(message.body));
#ifdef SBMDR_DIAGNOSTICS
        const auto prepared = ns<steady_clock>();
#endif
        auto frame = framed(envelope, {}, frame_limit, true, scratch);
#ifdef SBMDR_DIAGNOSTICS
        const auto framed_at = ns<steady_clock>();
#endif
        write(frame);
        if (scratch)
            *scratch = std::move(frame[1]);
#ifdef SBMDR_DIAGNOSTICS
        const auto appended = ns<steady_clock>();
        preparation_ns += prepared - preparation_start;
        rest_preparation_ns += message.rest_preparation_ns;
        framing_ns += framed_at - prepared;
        digest_write_ns += appended - framed_at;
        receive_to_enqueue_ns += message.enqueued - message.mono;
        enqueue_to_append_ns += appended - message.enqueued;
        if (message.mono < pending_since) {
            pending_enqueue = message.enqueued;
            pending_append = appended;
        }
#endif
        ++records;
        connections.insert(connection_id(message, run_id));
        update_range(utc_range, message.utc);
        update_range(mono_range, message.mono);
        if (!message.exchange_time.is_null())
            update_range(exchange_ranges["event"], message.exchange_time);
        if (!message.transaction_time.is_null())
            update_range(exchange_ranges["transaction"], message.transaction_time);
        if (!message.trade_time.is_null())
            update_range(exchange_ranges["trade"], message.trade_time);
        for (auto it = message.sequence.begin(); it != message.sequence.end(); ++it)
            update_range(sequence_ranges[it.key()], it.value());
        for (const auto& flag : message.flags)
            flags.insert(flag.get<std::string>());
        pending_since = std::min(pending_since, message.mono);
    }

    std::int64_t sync() {
        if (fd_ < 0 || pending_since == std::numeric_limits<std::int64_t>::max())
            return 0;
#ifdef SBMDR_DIAGNOSTICS
        const auto sync_start = ns<steady_clock>();
#endif
        if (::fsync(fd_) != 0)
            throw std::runtime_error("Raw fsync failed");
        fsync_utc = ns<system_clock>();
#ifdef SBMDR_DIAGNOSTICS
        const auto sync_finish = ns<steady_clock>();
        ++sync_count;
        sync_total_ns += sync_finish - sync_start;
        if (worst_sync.is_null() ||
            sync_finish - pending_since > worst_sync["durability_ns"].get<std::int64_t>())
            worst_sync = {{"receive_monotonic_ns", pending_since},
                          {"enqueue_monotonic_ns", pending_enqueue},
                          {"append_monotonic_ns", pending_append},
                          {"fsync_start_monotonic_ns", sync_start},
                          {"fsync_finish_monotonic_ns", sync_finish},
                          {"durability_ns", sync_finish - pending_since}};
#endif
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

    std::string symbol = "BTCUSDT", market = "spot", stream;
    const Bytes identity = uuid_bytes();
    const std::string chunk_id = uuid_text(identity);
    std::uint64_t bytes = 0, records = 0;
    std::int64_t created = 0, fsync_utc = 0;
    Json utc_range, mono_range, exchange_ranges = Json::object(), sequence_ranges = Json::object();
    std::set<std::string> flags, connections;
    Sha256 digest;
#ifdef SBMDR_DIAGNOSTICS
    std::int64_t preparation_ns = 0, framing_ns = 0, digest_write_ns = 0, rest_preparation_ns = 0;
    std::int64_t receive_to_enqueue_ns = 0, enqueue_to_append_ns = 0, sync_total_ns = 0;
    std::int64_t pending_enqueue = 0, pending_append = 0;
    std::uint64_t sync_count = 0;
    Json worst_sync;
#endif

  private:
    Json envelope_;
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
    void write(const std::array<Bytes, 2>& parts) {
        for (const auto& part : parts) {
            digest.add(part.data(), part.size());
            bytes += part.size();
        }
        write_all(fd_, parts);
    }
    std::int64_t pending_since = std::numeric_limits<std::int64_t>::max();
    int fd_ = -1;
};

#include "recorder_storage.hpp"

class IngressOverload : public std::runtime_error {
  public:
    explicit IngressOverload(const Message& message)
        : std::runtime_error(std::string("INGRESS_OVERLOAD ") + (message.rest ? "REST " : "WS ") +
                             message.market + " " + message.symbol + " " +
                             (message.rest ? message.source_stream : message.stream)),
          rest(message.rest) {}
    const bool rest;
};

enum class PushResult { accepted, full, closed };

class Handoff {
  public:
    explicit Handoff(std::size_t records = queue_records) : records_(records) {}

    static std::size_t billing(const Message& message) {
        return message.body.size() + queue_message_overhead;
    }

    void push(Message message, bool bounded = false) {
        std::unique_lock lock(mutex_);
        const auto size = billing(message);
        const auto available = [&] {
            return closed_ || (pending_.size() < records_ && bytes_ + size <= queue_limit);
        };
        if (bounded) {
            if (!ready_.wait_for(lock, milliseconds(250), available))
                throw IngressOverload(message);
        } else {
            ready_.wait(lock, available);
        }
        if (closed_)
            throw std::runtime_error("storage handoff closed");
        enqueue(std::move(message), size);
    }

    PushResult try_push(Message message) {
        std::lock_guard lock(mutex_);
        if (closed_)
            return PushResult::closed;
        const auto size = billing(message);
        if (pending_.size() >= records_ || bytes_ + size > queue_limit)
            return PushResult::full;
        enqueue(std::move(message), size);
        return PushResult::accepted;
    }

    std::pair<std::size_t, std::size_t> snapshot() {
        std::lock_guard lock(mutex_);
        return {pending_.size(), bytes_};
    }

    bool rest_admissible() {
        std::lock_guard lock(mutex_);
        return !closed_ && pending_.size() < records_ / 2 && bytes_ < queue_limit / 2;
    }

    bool pop(Message& message) {
        std::unique_lock lock(mutex_);
        ready_.wait(lock, [&] { return closed_ || !pending_.empty(); });
        if (pending_.empty())
            return false;
        bytes_ -= billing(pending_.front());
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
            const auto next = billing(pending_.front());
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

    std::uint64_t dropped_total() const {
        return dropped_ws.load(std::memory_order_relaxed) +
               dropped_rest.load(std::memory_order_relaxed);
    }

    std::size_t peak_bytes = 0, peak_records = 0;
    std::atomic<std::uint64_t> dropped_ws{0}, dropped_rest{0};

  private:
    void enqueue(Message message, std::size_t size) {
#ifdef SBMDR_DIAGNOSTICS
        message.enqueued = ns<steady_clock>();
#endif
        bytes_ += size;
        pending_.push_back(std::move(message));
        peak_bytes = std::max(peak_bytes, bytes_);
        peak_records = std::max(peak_records, pending_.size());
        ready_.notify_all();
    }

    std::mutex mutex_;
    std::condition_variable ready_;
    std::deque<Message> pending_;
    std::size_t records_, bytes_ = 0;
    bool closed_ = false;
};

template <class Stream>
void read_ws(Stream& ws, asio::io_context& io, std::size_t count, Handoff& queue, std::stop_token stop,
             std::size_t* accepted, unsigned generation, bool usdm = false, const CaptureConfig* config = nullptr, std::string group = "") {
    ws.read_message_max(frame_limit - max_encoded_metadata);
    simdjson::dom::parser parser;
    beast::flat_buffer buffer;
    std::set<std::string> seen_streams;
    std::map<std::string, std::uint64_t> previous_by_symbol;
    std::optional<std::uint64_t> previous;
    struct DropState {
        std::uint64_t count = 0;
        std::int64_t first_utc = 0, last_utc = 0, first_mono = 0, last_mono = 0;
    };
    std::map<std::string, DropState> drops;
    std::uint64_t connection_dropped = 0;
    std::optional<steady_clock::time_point> last_drop_status;
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
        message.market = usdm ? "um_perpetual" : "spot";
        message.connection_group = group;
        message.utc = ns<system_clock>();
        message.mono = ns<steady_clock>();
        const std::string_view wire(static_cast<const char*>(buffer.data().data()), buffer.size());
        message.body.assign(wire.begin(), wire.end());
        simdjson::dom::element root, data;
        std::string_view stream, event, symbol;
        std::uint64_t time = 0, first = 0, last = 0;
        simdjson::dom::array bids, asks;
        if (config) {
            parse_configured_ws(message, wire, parser, *config, usdm);
            if (message.stream == "diff_depth" && message.flags.empty()) {
                const auto f = message.sequence.at("U").get<std::uint64_t>();
                const auto l = message.sequence.at("u").get<std::uint64_t>();
                auto found = previous_by_symbol.find(message.symbol);
                if (found != previous_by_symbol.end() &&
                    (usdm ? message.sequence.at("pu").get<std::uint64_t>() != found->second : f > found->second + 1))
                    message.flags.push_back("sequence_gap");
                previous_by_symbol[message.symbol] = l;
            }
        } else {
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
                                {"sourceFamily", usdm ? "D02" : "S02"},
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
        if (usdm && message.flags.empty()) {
            std::uint64_t previous_final;
            if (data["pu"].get(previous_final))
                message.flags.push_back("malformed");
            else
                message.sequence["pu"] = previous_final;
        }
        if (stop.stop_possible()) {
            if (!message.flags.empty()) {
                message.flags.push_back("sequence_gap");
                previous.reset();
            } else {
                if (previous && (usdm ? message.sequence.at("pu").get<std::uint64_t>() != *previous
                                     : first > *previous && first - *previous > 1))
                    message.flags.push_back("sequence_gap");
                previous = previous ? std::max(*previous, last) : last;
            }
        }
        }
        message.generation = generation;
        if (generation && (config ? seen_streams.insert(message.symbol + "/" + message.stream).second : i == 0))
            message.flags.push_back("sequence_gap");
        if (!config) {
            queue.push(std::move(message), stop.stop_possible());
            if (accepted)
                ++*accepted;
            buffer.consume(buffer.size());
            continue;
        }
        const auto stream_key = message.symbol + "/" + message.stream;
        auto pending = drops.find(stream_key);
        if (pending != drops.end() && pending->second.count) {
            message.flags.push_back("sequence_gap");
            message.flags.push_back("ingress_drop");
            message.sequence["ingressDropped"] = pending->second.count;
            message.sequence["ingressDropFirstUtcNs"] = pending->second.first_utc;
            message.sequence["ingressDropLastUtcNs"] = pending->second.last_utc;
            message.sequence["ingressDropFirstMonotonicNs"] = pending->second.first_mono;
            message.sequence["ingressDropLastMonotonicNs"] = pending->second.last_mono;
        }
        const auto drop_market = message.market;
        const auto drop_symbol = message.symbol;
        const auto drop_stream = message.stream;
        const auto drop_utc = message.utc;
        const auto drop_mono = message.mono;
        const auto outcome = queue.try_push(std::move(message));
        if (outcome == PushResult::closed)
            throw std::runtime_error("storage handoff closed");
        if (outcome == PushResult::full) {
            auto& state = drops[stream_key];
            if (!state.count) {
                state.first_utc = drop_utc;
                state.first_mono = drop_mono;
            }
            state.last_utc = drop_utc;
            state.last_mono = drop_mono;
            ++state.count;
            ++connection_dropped;
            queue.dropped_ws.fetch_add(1, std::memory_order_relaxed);
            const auto now = steady_clock::now();
            if (!last_drop_status || now - *last_drop_status >= seconds(1)) {
                last_drop_status = now;
                status_line(Json{{"event", "INGRESS_DROP"},
                                 {"transport", "ws"},
                                 {"market", drop_market},
                                 {"symbol", drop_symbol},
                                 {"stream", drop_stream},
                                 {"connection_dropped", connection_dropped},
                                 {"dropped_total", queue.dropped_total()}});
            }
        } else {
            if (pending != drops.end())
                pending->second.count = 0;
            if (accepted)
                ++*accepted;
        }
        buffer.consume(buffer.size());
    }
    if (!stop.stop_possible())
        ws.close(beast::websocket::close_code::normal);
}

void receive_ws(const std::string& port, std::size_t count, Handoff& queue, std::stop_token stop = {},
                const std::optional<std::string>& ca = std::nullopt, std::size_t* accepted = nullptr,
                unsigned generation = 0, bool usdm = false, const CaptureConfig* config = nullptr,
                const std::string& configured_target = "") {
    asio::io_context io;
    const std::string target = !configured_target.empty() ? configured_target : usdm ? "/public/stream?streams=btcusdt@depth@100ms"
                             : "/stream?streams=btcusdt@depth@100ms";
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
        const auto host = usdm ? "fstream.binance.com" : "stream.binance.com";
        const auto service = usdm ? "443" : "9443";
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
                    resolver.async_resolve(host, service, [&, done](auto error, auto found) {
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
        if (!bounded([&](auto done) { ws.async_handshake(std::string(host) + ":" + service, target, done); },
                     close_socket))
            return;
        read_ws(ws, io, count, queue, stop, accepted, generation, usdm, config, config ? (target.starts_with("/market") ? "-market" : "-public") : "");
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
        read_ws(ws, io, count, queue, stop, accepted, generation, usdm, config, config ? (target.starts_with("/market") ? "-market" : "-public") : "");
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
    bool dropped = false;
};

using RestMulti = std::unique_ptr<CURLM, decltype(&curl_multi_cleanup)>;

RestMulti make_rest_multi() {
    RestMulti multi(curl_multi_init(), curl_multi_cleanup);
    if (!multi)
        throw std::runtime_error("curl multi initialization failed");
    return multi;
}

RestResult receive_rest(CURLM* multi, const std::string& port, Handoff& queue,
                        const std::optional<std::string>& ca = std::nullopt, bool bounded = false,
                        const std::string& run_id = "c0", const RestSource* source = nullptr,
                        bool usdm = false, std::uint64_t snapshot_epoch = 0,
                        const CaptureConfig* config = nullptr, bool configured = false) {
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
    message.market = usdm ? "um_perpetual" : "spot";
    message.snapshot_epoch = snapshot_epoch;
    message.symbol_scoped = source ? source->parameters.contains("symbol") : ca.has_value();
    if (source) {
        message.source_stream = source->stream;
        message.family = source->family;
        message.path = source->path;
        message.parameters = source->parameters;
    } else if (message.symbol_scoped) {
        message.parameters = {{"symbol", "BTCUSDT"}};
    }
    if (message.symbol_scoped)
        message.symbol = message.parameters.at("symbol").get<std::string>();
    if (usdm && !message.symbol_scoped)
        message.market = "um_shared";
    HttpBody response;
    std::string url = ca ? (usdm ? "https://fapi.binance.com" : "https://api.binance.com")
                         : "http://127.0.0.1:" + port;
    url += message.path;
    bool first_parameter = true;
    for (auto it = message.parameters.begin(); it != message.parameters.end(); ++it) {
        url += first_parameter ? '?' : '&';
        first_parameter = false;
        const auto value = it.value().is_string() ? it.value().get<std::string>() : it.value().dump();
        std::unique_ptr<char, decltype(&curl_free)> escaped(curl_easy_escape(easy.get(), value.c_str(), static_cast<int>(value.size())), curl_free);
        if (!escaped) throw std::runtime_error("URL parameter encoding failed");
        url += it.key() + "=" + escaped.get();
    }
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
    bool valid = message.status == 200 && !parser.parse(
        reinterpret_cast<const char*>(message.body.data()), message.body.size()).get(root);
    if (valid && message.source_stream == "exchange_info")
        valid = !root["symbols"].get(symbols) && !root["rateLimits"].get(limits);
    else if (valid && message.source_stream == "depth_snapshot") {
        std::uint64_t last;
        valid = !root["lastUpdateId"].get(last) && !root["bids"].get(symbols) && !root["asks"].get(limits);
        if (valid)
            message.sequence["lastUpdateId"] = last;
    } else if (valid && message.source_stream == "open_interest") {
        std::string_view symbol, interest;
        valid = !root["symbol"].get(symbol) && symbol == message.symbol && !root["openInterest"].get(interest);
    } else if (valid) {
        simdjson::dom::array rows;
        valid = !root.get(rows); // Keep legitimate empty/old buckets; no retry state machine.
        if (valid) {
            for (auto row : rows) {
                std::uint64_t timestamp;
                if (!row.is_object() || row["timestamp"].get(timestamp)) {
                    valid = false;
                    break;
                }
            }
        }
    }
    if (config && message.source_stream == "exchange_info") {
        std::set<std::string> trading;
        if (valid) {
            for (auto row : symbols) {
                std::string_view name, status;
                if (!row["symbol"].get(name) && !row["status"].get(status) && status == "TRADING")
                    trading.emplace(name);
            }
        }
        for (const auto& symbol : config->symbols)
            if (!trading.contains(symbol))
                throw std::runtime_error("configured symbol not TRADING: " + symbol);
    }
    if (!valid) {
        message.flags.push_back("source_failed");
        message.flags.push_back("malformed");
    } else {
        message.rest_valid = true;
    }
    message.sequence["sourceStream"] = message.source_stream;
    message.sequence["sourceFamily"] = message.family;
    RestResult outcome{message.status, message.rest_valid, message.retry_after, {}};
    const auto drop_market = message.market;
    const auto drop_symbol = message.symbol;
    const auto drop_source = message.source_stream;
#ifdef SBMDR_DIAGNOSTICS
    const auto rest_prepare_start = ns<steady_clock>();
#endif
    message.body = prepare_rest_payload(message, run_id);
#ifdef SBMDR_DIAGNOSTICS
    message.rest_preparation_ns = ns<steady_clock>() - rest_prepare_start;
#endif
    if (configured) {
        const auto result = queue.try_push(std::move(message));
        if (result == PushResult::closed)
            throw std::runtime_error("storage handoff closed");
        if (result == PushResult::full) {
            outcome.dropped = true;
            queue.dropped_rest.fetch_add(1, std::memory_order_relaxed);
            status_line(Json{{"event", "INGRESS_DROP"},
                             {"transport", "rest"},
                             {"market", drop_market},
                             {"symbol", drop_symbol},
                             {"source", drop_source},
                             {"dropped_total", queue.dropped_total()}});
        }
    } else {
        queue.push(std::move(message), bounded);
    }
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
    auto record = framed(Json::from_cbor(frame + 12, bytes.end()), {}, frame_limit, true);
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
    Message billed;
    billed.body = Bytes(64);
    if (Handoff::billing(billed) != billed.body.size() + queue_message_overhead)
        throw std::runtime_error("queue billing does not include fixed message overhead");

    Handoff queue;
    for (std::size_t i = 0; i < queue_records; ++i) {
        Message message;
        message.mono = static_cast<std::int64_t>(i);
        queue.push(std::move(message));
    }
    if (queue.rest_admissible())
        throw std::runtime_error("saturated queue admitted REST");
    try {
        Message extra;
        extra.symbol = "ETHUSDT";
        extra.stream = "agg_trade";
        queue.push(std::move(extra), true);
        throw std::runtime_error("saturated queue accepted an extra record");
    } catch (const IngressOverload& error) {
        if (std::string(error.what()).find("ETHUSDT agg_trade") == std::string::npos)
            throw std::runtime_error("overload did not identify the configured source");
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

    Handoff configured(1024);
    for (std::size_t i = 0; i < 1024; ++i) {
        Message queued;
        queued.mono = static_cast<std::int64_t>(i);
        configured.push(std::move(queued));
    }
    if (configured.rest_admissible())
        throw std::runtime_error("saturated configured queue admitted REST");
    const auto before = steady_clock::now();
    if (configured.try_push(Message{}) != PushResult::full)
        throw std::runtime_error("saturated configured queue accepted an extra record");
    if (steady_clock::now() - before > milliseconds(50))
        throw std::runtime_error("configured queue rejection blocked");
    drained = 0;
    configured.close();
    while (configured.pop(message)) {
        if (message.mono != static_cast<std::int64_t>(drained++))
            throw std::runtime_error("configured queue changed record order");
    }
    if (drained != 1024)
        throw std::runtime_error("configured queue lost queued records");
    if (configured.try_push(Message{}) != PushResult::closed)
        throw std::runtime_error("closed configured queue did not reject ingress");
}

int main(int argc, char** argv) try {
    if (argc == 2 && std::string_view(argv[1]) == "--check-depth-bridge") {
        verify_depth_edges();
        return 0;
    }
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
        receive_rest(multi.get(), argv[2], queue, std::nullopt, false, run_id);
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
        {"allocation_counter_enabled", allocation_counter_enabled},
        {"new_calls", allocation_counter_enabled ? Json(allocations.load() - start_allocations) : Json(nullptr)},
        {"new_requested_bytes", allocation_counter_enabled ? Json(allocated_bytes.load() - start_bytes) : Json(nullptr)},
        {"peak_queue_payload_bytes", queue.peak_bytes}, {"peak_queue_records", queue.peak_records},
        {"encode_write_ns", encode_write_ns}, {"final_fsync_close_ns", sync_ns},
        {"max_receive_to_final_fsync_ns", max_receive_to_sync_ns},
    }.dump() << '\n';
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
