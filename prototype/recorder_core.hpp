constexpr std::size_t depth_pending_limit = 8192;

struct CaptureConfig {
    std::vector<std::string> symbols;
    static CaptureConfig load(const char* path, bool usdm) {
        std::ifstream input(path);
        const auto json = Json::parse(input);
        CaptureConfig config{json.at(usdm ? "usdm" : "spot").get<std::vector<std::string>>()};
        std::set<std::string> unique;
        if (config.symbols.empty() || config.symbols.size() > 20)
            throw std::runtime_error("whitelist requires 1..20 symbols per market");
        for (const auto& name : config.symbols) {
            if (name.empty() || name.find_first_not_of("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") != std::string::npos || !unique.insert(name).second)
                throw std::runtime_error("invalid or duplicate whitelist symbol");
        }
        return config;
    }
    std::string target(bool usdm, bool market_route) const {
        std::string result = usdm ? (market_route ? "/market" : "/public") : "";
        result += "/stream?streams=";
        bool first = true;
        for (auto symbol : symbols) {
            std::transform(symbol.begin(), symbol.end(), symbol.begin(), [](unsigned char c) { return std::tolower(c); });
            const std::vector<std::string> streams = !usdm ? std::vector<std::string>{"depth@100ms", "bookTicker", "aggTrade"} : market_route ? std::vector<std::string>{"aggTrade", "markPrice@1s", "forceOrder"} : std::vector<std::string>{"depth@100ms", "bookTicker"};
            for (const auto& stream : streams) {
                if (!first) result += '/';
                first = false;
                result += symbol + "@" + stream;
            }
        }
        return result;
    }
};

void parse_configured_ws(Message& message, std::string_view wire, simdjson::dom::parser& parser,
                         const CaptureConfig& config, bool usdm) {
    message.symbol = "__GLOBAL__";
    message.stream = "ws_malformed";
    simdjson::dom::element root, data, event_data;
    std::string_view stream, symbol;
    const auto malformed = [&] { message.flags.push_back("malformed"); };
    if (parser.parse(wire.data(), wire.size()).get(root) || root["stream"].get(stream) || root["data"].get(data)) { malformed(); return; }
    const auto separator = stream.find('@');
    if (separator == std::string_view::npos) { malformed(); return; }
    std::string routed_symbol(stream.substr(0, separator));
    std::transform(routed_symbol.begin(), routed_symbol.end(), routed_symbol.begin(), [](unsigned char c) { return std::toupper(c); });
    if (std::find(config.symbols.begin(), config.symbols.end(), routed_symbol) == config.symbols.end())
        throw std::runtime_error("WS returned symbol outside whitelist");
    message.symbol = routed_symbol;
    const auto kind = stream.substr(separator + 1);
    message.stream = kind == "depth@100ms" ? "diff_depth" : kind == "bookTicker" ? "book_ticker" : kind == "aggTrade" ? "agg_trade" : kind == "markPrice@1s" ? "mark_price" : kind == "forceOrder" ? "liquidation" : "unknown";
    message.sequence = {{"sourceStream", message.stream}, {"wireStream", stream}};
    if (message.stream == "unknown") { malformed(); return; }
    if (message.stream != "book_ticker") {
        std::string_view event;
        const std::string_view expected = message.stream == "diff_depth" ? "depthUpdate" : message.stream == "agg_trade" ? "aggTrade" : message.stream == "mark_price" ? "markPriceUpdate" : "forceOrder";
        if (data["e"].get(event) || event != expected) { malformed(); return; }
    }
    event_data = data;
    if (message.stream == "liquidation" && data["o"].get(event_data)) { malformed(); return; }
    if (event_data["s"].get(symbol) || symbol != message.symbol) { malformed(); return; }
    auto integer = [&](simdjson::dom::element source, const char* field, bool required) {
        std::uint64_t value;
        const auto result = source[field].get(value);
        if (!result) message.sequence[field] = value;
        else if (required) malformed();
    };
    std::uint64_t time;
    if (!data["E"].get(time)) message.exchange_time = time;
    else if (message.stream != "book_ticker") { malformed(); return; }
    if (!data["T"].get(time)) message.transaction_time = time;
    if (message.stream == "agg_trade") { message.trade_time = message.transaction_time; message.transaction_time = nullptr; }
    if (message.stream == "liquidation" && !event_data["T"].get(time)) message.trade_time = time;
    if (message.stream == "diff_depth") {
        message.sequence["sourceFamily"] = usdm ? "D02" : "S02";
        integer(data, "U", true); integer(data, "u", true);
        if (usdm) integer(data, "pu", true);
        simdjson::dom::array bids, asks;
        if (data["b"].get(bids) || data["a"].get(asks) || (message.flags.empty() && message.sequence["U"] > message.sequence["u"])) malformed();
    } else {
        message.sequence["sourceFamily"] = message.stream == "book_ticker" ? (usdm ? "D08" : "S05") : message.stream == "agg_trade" ? (usdm ? "D05" : "S03") : message.stream == "mark_price" ? "D09" : "D11";
        if (message.stream == "book_ticker") integer(data, "u", true);
        if (message.stream == "agg_trade") integer(data, "a", true);
    }
}

// C2: one depth sequence (no order book) and the existing REST thread's wakeup.
class RecorderWake {
    std::mutex mutex_;
    std::condition_variable ready_;

  public:
    void notify() {
        std::lock_guard lock(mutex_);
        ready_.notify_all();
    }
    template <class Predicate>
    void until(std::stop_token stop, steady_clock::time_point due, Predicate changed) {
        // Register before taking the mutex; destroy the lock before the callback.
        // The callback and predicate use the same mutex, including stop-before-wait.
        std::stop_callback callback(stop, [this] { notify(); });
        std::unique_lock lock(mutex_);
        ready_.wait_until(lock, due, [&] { return stop.stop_requested() || changed(); });
    }
    void until(std::stop_token stop, steady_clock::time_point due) {
        until(stop, due, [] { return false; });
    }
};

struct DepthRequests {
    std::atomic<std::uint64_t> epoch{0};
    RecorderWake wake;
    RecorderWake* parent = nullptr;
    std::map<std::string, std::unique_ptr<DepthRequests>> children;
    DepthRequests& for_symbol(const std::string& symbol) {
        return children.empty() ? *this : *children.at(symbol);
    }
    void initialize(const CaptureConfig& config) {
        for (const auto& symbol : config.symbols) {
            auto child = std::make_unique<DepthRequests>();
            child->parent = &wake;
            children.emplace(symbol, std::move(child));
        }
    }
    void request() {
        ++epoch;
        wake.notify();
        if (parent) parent->notify();
    }
};

class DepthBridge {
    struct Range {
        std::uint64_t first, last, previous;
    };
    DepthRequests *requests_;
    std::deque<Range> pending_;
    std::optional<unsigned> generation_;
    std::optional<std::uint64_t> snapshot_, last_;

    void reset() {
        pending_.clear();
        snapshot_.reset();
        last_.reset();
        requests_->request();
    }
    static Range range(const Message &message) {
        return {message.sequence.at("U"), message.sequence.at("u"),
                message.sequence.value("pu", std::uint64_t{0})};
    }
    bool advance(Range event, bool usdm) {
        if (last_ && event.last <= *last_)
            return true;
        const auto target = last_ ? *last_ + 1 : *snapshot_ + (usdm ? 0 : 1);
        const bool valid = last_ && usdm ? event.previous == *last_
                                         : event.first <= target && target <= event.last;
        if (valid)
            last_ = event.last;
        return valid;
    }

  public:
    explicit DepthBridge(DepthRequests *requests = nullptr) : requests_(requests) {}
    void observe(Message &message) {
        if (!requests_)
            return;
        if (message.rest) {
            if (message.source_stream != "depth_snapshot")
                return;
            message.sequence["depthSnapshotEpoch"] = message.snapshot_epoch;
            if (message.snapshot_epoch != requests_->epoch.load()) {
                message.flags.push_back("depth_snapshot_superseded");
                return;
            }
            if (!message.rest_valid)
                return;
            snapshot_ = message.sequence.at("lastUpdateId");
            last_.reset();
            for (const auto event : pending_) {
                if (event.last < *snapshot_ ||
                    (message.market == "spot" && event.last == *snapshot_))
                    continue;
                if (!advance(event, message.market != "spot")) {
                    message.flags.push_back("sequence_gap");
                    message.flags.push_back("depth_snapshot_too_old");
                    reset();
                    return;
                }
            }
            if (last_) {
                message.flags.push_back("depth_bridge");
                message.sequence["bridgedThroughU"] = *last_;
            } else {
                message.flags.push_back("depth_snapshot_pending");
            }
            pending_.clear();
            return;
        }
        if (message.stream != "ws_market_c0_depth" && message.stream != "diff_depth") return;
        if (!generation_ || *generation_ != message.generation) {
            generation_ = message.generation;
            reset();
        }
        const bool malformed = std::find(message.flags.begin(), message.flags.end(), "malformed") !=
                               message.flags.end();
        if (malformed) {
            message.flags.push_back("sequence_gap");
            reset();
        } else {
            const auto event = range(message);
            if (snapshot_) {
                const bool stale = event.last < *snapshot_ ||
                                   (message.market == "spot" && event.last == *snapshot_) ||
                                   (last_ && event.last <= *last_);
                if (stale)
                    message.flags.push_back("depth_stale");
                else if (advance(event, message.market != "spot"))
                    message.flags.push_back("depth_bridge");
                else {
                    message.flags.push_back("sequence_gap");
                    reset();
                }
            }
            if (!snapshot_) {
                if (pending_.size() == depth_pending_limit) {
                    message.flags.push_back("sequence_gap");
                    message.flags.push_back("depth_sequence_buffer_overflow");
                    reset();
                }
                pending_.push_back(event);
            }
        }
        message.sequence["depthSnapshotEpoch"] = requests_->epoch.load();
        if (snapshot_)
            message.sequence["snapshotLastUpdateId"] = *snapshot_;
        else
            message.flags.push_back("depth_snapshot_pending");
    }
};

struct RestSource {
    std::string stream, family, path;
    Json parameters;
    unsigned weight;
    bool statistics = false;
};

RestSource directory_source(bool usdm) {
    return {"exchange_info", usdm ? "D01" : "S01",
            usdm ? "/fapi/v1/exchangeInfo" : "/api/v3/exchangeInfo",
            usdm ? Json::object() : Json{{"symbol", "BTCUSDT"}}, usdm ? 1U : 20U};
}
RestSource depth_source(bool usdm) {
    return {"depth_snapshot",
            usdm ? "D02" : "S02",
            usdm ? "/fapi/v1/depth" : "/api/v3/depth",
            {{"symbol", "BTCUSDT"}, {"limit", usdm ? 1000 : 5000}},
            usdm ? 20U : 250U};
}

std::vector<RestSource> periodic_sources() {
    return {
        {"open_interest", "D14", "/fapi/v1/openInterest", {{"symbol", "BTCUSDT"}}, 1},
        {"top_long_short_account_ratio_5m",
         "D18",
         "/futures/data/topLongShortAccountRatio",
         {{"symbol", "BTCUSDT"}, {"period", "5m"}, {"limit", 1}},
         0,
         true},
        {"top_long_short_position_ratio_5m",
         "D19",
         "/futures/data/topLongShortPositionRatio",
         {{"symbol", "BTCUSDT"}, {"period", "5m"}, {"limit", 1}},
         0,
         true},
        {"global_long_short_ratio_5m",
         "D17",
         "/futures/data/globalLongShortAccountRatio",
         {{"symbol", "BTCUSDT"}, {"period", "5m"}, {"limit", 1}},
         0,
         true},
        {"taker_buy_sell_volume_5m",
         "D16",
         "/futures/data/takerlongshortRatio",
         {{"symbol", "BTCUSDT"}, {"period", "5m"}, {"limit", 1}},
         0,
         true},
    };
}

void verify_depth_edges() {
    const auto has = [](const Message &m, const char *flag) {
        return std::find(m.flags.begin(), m.flags.end(), flag) != m.flags.end();
    };
    const auto require = [](bool condition) {
        if (!condition)
            throw std::runtime_error("depth bridge edge regression");
    };
    DepthRequests requests;
    DepthBridge bridge(&requests);
    auto event = [&](std::uint64_t first, std::uint64_t last, unsigned generation = 0) {
        Message m;
        m.generation = generation;
        m.sequence = {{"U", first}, {"u", last}};
        bridge.observe(m);
        return m;
    };
    auto snapshot = [&](std::uint64_t epoch, std::uint64_t last) {
        Message m;
        m.rest = m.rest_valid = true;
        m.source_stream = "depth_snapshot";
        m.snapshot_epoch = epoch;
        m.sequence = {{"lastUpdateId", last}};
        bridge.observe(m);
        return m;
    };
    event(100, 102);
    const auto old_epoch = requests.epoch.load();
    event(200, 202, 1);
    require(has(snapshot(old_epoch, 101), "depth_snapshot_superseded"));
    const auto current_epoch = requests.epoch.load();
    require(has(snapshot(current_epoch, 100), "depth_snapshot_too_old"));
    require(requests.epoch.load() > current_epoch);
    event(300, 302, 1);
    require(has(snapshot(requests.epoch.load(), 301), "depth_bridge"));
    require(has(event(303, 305, 1), "depth_bridge"));
    require(has(event(303, 305, 1), "depth_stale"));
    event(400, 402, 2);
    for (std::size_t i = 1; i < depth_pending_limit; ++i)
        event(400 + i * 3, 402 + i * 3, 2);
    require(has(event(400 + depth_pending_limit * 3, 402 + depth_pending_limit * 3, 2),
                "depth_sequence_buffer_overflow"));
}
