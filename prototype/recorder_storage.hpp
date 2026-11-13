// One homogeneous Raw v1 chunk per market/symbol/stream, owned by the writer.
namespace fs = std::filesystem;

namespace partition_layout {

inline std::string utc_day(std::int64_t utc_ns) {
    const auto seconds = static_cast<std::time_t>(utc_ns / 1'000'000'000LL);
    std::tm value{};
    if (::gmtime_r(&seconds, &value) == nullptr)
        throw std::runtime_error("UTC day conversion failed");
    char text[16]{};
    if (::strftime(text, sizeof(text), "%Y-%m-%d", &value) == 0)
        throw std::runtime_error("UTC day formatting failed");
    return text;
}

inline std::string read_text(const fs::path& path) {
    std::ifstream input(path);
    if (!input)
        throw std::runtime_error("partition file read failed: " + path.string());
    std::string text((std::istreambuf_iterator<char>(input)),
                     std::istreambuf_iterator<char>());
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.back())))
        text.pop_back();
    return text;
}

inline Json read_json(const fs::path& path) {
    const auto text = read_text(path);
    try {
        return Json::parse(text);
    } catch (...) {
        throw std::runtime_error("partition JSON is invalid: " + path.string());
    }
}

inline void write_json_atomic(const fs::path& path, const Json& value) {
    const auto temporary = path.string() + ".tmp";
    {
        std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
        if (!output)
            throw std::runtime_error("partition file create failed: " + temporary);
        output << value.dump() << '\n';
        output.flush();
        if (!output)
            throw std::runtime_error("partition file write failed: " + temporary);
    }
    const int descriptor = ::open(temporary.c_str(), O_RDONLY);
    if (descriptor < 0 || ::fsync(descriptor) != 0) {
        if (descriptor >= 0)
            ::close(descriptor);
        throw std::runtime_error("partition file fsync failed: " + temporary);
    }
    ::close(descriptor);
    fs::rename(temporary, path);
    sync_directory(path.parent_path());
}

inline void ensure_partition_dirs(const fs::path& root) {
    for (const auto* directory :
         {"state", "data/active", "data/sealed", "data/manifests"})
        fs::create_directories(root / directory);
}

inline std::string read_current(const fs::path& container) {
    const auto current = container / "CURRENT";
    if (!fs::is_regular_file(current))
        return {};
    return read_text(current);
}

inline void write_current(const fs::path& container, const std::string& id) {
    const auto path = container / "CURRENT";
    const auto temporary = path.string() + ".tmp";
    {
        std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
        if (!output)
            throw std::runtime_error("CURRENT create failed");
        output << id << '\n';
        output.flush();
        if (!output)
            throw std::runtime_error("CURRENT write failed");
    }
    const int descriptor = ::open(temporary.c_str(), O_RDONLY);
    if (descriptor < 0 || ::fsync(descriptor) != 0) {
        if (descriptor >= 0)
            ::close(descriptor);
        throw std::runtime_error("CURRENT fsync failed");
    }
    ::close(descriptor);
    fs::rename(temporary, path);
    sync_directory(path.parent_path());
}

inline Json open_partition_metadata(const std::string& id, std::int64_t now) {
    return Json{{"schema_version", "partition.v1"},
                {"partition_id", id},
                {"created_at_utc_ns", now},
                {"receive_time_utc_range_ns", {{"min", now}, {"max", now}}},
                {"state", "open"}};
}

inline fs::path create_partition(const fs::path& container, std::int64_t now) {
    const auto day = utc_day(now);
    const auto id = day + "T" + std::to_string(now) + "-" +
                    uuid_text(uuid_bytes());
    const auto root = container / "partitions" / id;
    ensure_partition_dirs(root);
    write_json_atomic(root / "partition.json",
                      open_partition_metadata(id, now));
    write_current(container, id);
    return root;
}

inline void close_partition(const fs::path& root, std::int64_t last_receive_ns,
                            std::int64_t now) {
    auto metadata = read_json(root / "partition.json");
    metadata["state"] = "closed";
    metadata["closed_at_utc_ns"] = now;
    auto& range = metadata["receive_time_utc_range_ns"];
    range["max"] = std::max<std::int64_t>(
        range.value("max", last_receive_ns), last_receive_ns);
    write_json_atomic(root / "partition.json", metadata);
}

} // namespace partition_layout

class Catalog {
    std::unique_ptr<sqlite3, decltype(&sqlite3_close)> db_{nullptr, sqlite3_close};
    std::mutex mutex_;

    void check(int result, int expected = SQLITE_OK) {
        if (result != expected)
            throw std::runtime_error(sqlite3_errmsg(db_.get()));
    }

    void statement(const char* sql, const Json& args) {
        sqlite3_stmt* raw = nullptr;
        const auto result = sqlite3_prepare_v2(db_.get(), sql, -1, &raw, nullptr);
        std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> stmt(raw, sqlite3_finalize);
        check(result);
        int index = 1;
        for (const auto& arg : args) {
            if (arg.is_string()) {
                const auto& text = arg.get_ref<const std::string&>();
                check(sqlite3_bind_text(raw, index, text.c_str(), -1, SQLITE_TRANSIENT));
            } else if (arg.is_null()) {
                check(sqlite3_bind_null(raw, index));
            } else {
                check(sqlite3_bind_int64(raw, index, arg.get<std::int64_t>()));
            }
            ++index;
        }
        check(sqlite3_step(raw), SQLITE_DONE);
    }

    void transition(const std::string& id, const Json& from, const std::string& to,
                    std::int64_t timestamp, const std::string& evidence) {
        statement("INSERT INTO chunk_transitions(chunk_id,from_state,to_state,occurred_at_utc_ns,"
                  "evidence_json,idempotency_key) VALUES(?,?,?,?,?,?)",
                  Json::array({id, from, to, timestamp, evidence, to + ":" + id}));
    }

  public:
    explicit Catalog(const fs::path& path) {
        sqlite3* raw = nullptr;
        const auto status =
            sqlite3_open_v2(path.c_str(), &raw, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
        db_.reset(raw);
        check(status);
        check(sqlite3_busy_timeout(raw, 30000));
        exec(R"SQL(
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA foreign_keys=ON;
            BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id TEXT PRIMARY KEY, state TEXT NOT NULL, partial_path TEXT, sealed_path TEXT,
                manifest_path TEXT, record_count INTEGER NOT NULL DEFAULT 0, uncompressed_bytes INTEGER,
                stored_bytes INTEGER, uncompressed_sha256 TEXT, stored_sha256 TEXT,
                created_at_utc_ns INTEGER NOT NULL, updated_at_utc_ns INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS chunk_transitions (
                transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
                chunk_id TEXT NOT NULL REFERENCES chunks(chunk_id), from_state TEXT, to_state TEXT NOT NULL,
                occurred_at_utc_ns INTEGER NOT NULL, evidence_json TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS operational_events (
                event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL, occurred_at_utc_ns INTEGER NOT NULL,
                evidence_json TEXT NOT NULL, market TEXT, symbol TEXT, stream TEXT, gap_id TEXT,
                CHECK (event_type NOT IN ('STREAM_DISCONTINUITY_STARTED','STREAM_DISCONTINUITY_COMPLETED')
                       OR symbol IS NOT NULL)
            );
            CREATE TABLE IF NOT EXISTS side_data_cursors (
                kind TEXT NOT NULL, symbol TEXT NOT NULL,
                last_persisted_period_timestamp INTEGER NOT NULL, updated_at_utc_ns INTEGER NOT NULL,
                source_retention_window TEXT NOT NULL, retention_window_ms INTEGER NOT NULL,
                PRIMARY KEY(kind,symbol)
            );
            CREATE INDEX IF NOT EXISTS operational_events_discontinuity_identity
                ON operational_events(event_type,market,symbol,stream,gap_id);
            CREATE INDEX IF NOT EXISTS chunks_by_state_created_id ON chunks(state,created_at_utc_ns,chunk_id);
            COMMIT;
        )SQL");
    }

    void exec(const char* sql) { check(sqlite3_exec(db_.get(), sql, nullptr, nullptr, nullptr)); }

    void checkpoint() {
        check(sqlite3_exec(db_.get(), "PRAGMA wal_checkpoint(FULL)",
                           nullptr, nullptr, nullptr));
    }

    void copy_archive_targets_from(const fs::path& prior) {
        std::string escaped = prior.string();
        std::string quoted;
        quoted.reserve(escaped.size() + 2);
        quoted.push_back('\'');
        for (const char ch : escaped) {
            quoted.push_back(ch);
            if (ch == '\'')
                quoted.push_back('\'');
        }
        quoted.push_back('\'');
        const auto attach = "ATTACH DATABASE " + quoted + " AS prior";
        check(sqlite3_exec(db_.get(), attach.c_str(), nullptr, nullptr, nullptr));
        std::string prior_has_table;
        {
            sqlite3_stmt* check_raw = nullptr;
            const auto check_sql =
                "SELECT count(*) FROM prior.sqlite_master WHERE type='table' AND name='storage_targets'";
            check(sqlite3_prepare_v2(db_.get(), check_sql, -1, &check_raw, nullptr));
            std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> check_stmt(
                check_raw, sqlite3_finalize);
            check(sqlite3_step(check_stmt.get()), SQLITE_ROW);
            prior_has_table = sqlite3_column_int(check_stmt.get(), 0) != 0 ? "1" : "";
        }
        if (!prior_has_table.empty()) {
            const std::string create =
                "CREATE TABLE IF NOT EXISTS storage_targets AS "
                "SELECT * FROM prior.storage_targets WHERE 0";
            check(sqlite3_exec(db_.get(), create.c_str(), nullptr, nullptr, nullptr));
            const std::string insert =
                "INSERT INTO storage_targets SELECT * FROM prior.storage_targets "
                "WHERE NOT EXISTS (SELECT 1 FROM storage_targets)";
            check(sqlite3_exec(db_.get(), insert.c_str(), nullptr, nullptr, nullptr));
        }
        check(sqlite3_exec(db_.get(), "DETACH DATABASE prior", nullptr, nullptr, nullptr));
    }

    void startup(const std::string& run) {
        std::lock_guard lock(mutex_);
        statement("INSERT INTO operational_events(event_id,event_type,occurred_at_utc_ns,evidence_json)"
                  " VALUES(?,'PROCESS_START',?,?)",
                  Json::array({run, ns<system_clock>(), Json{{"collector_instance_id", run}}.dump()}));
    }

    void overload(const std::string& run, bool rest, bool symbol_scoped, const std::string& market, bool configured) {
        std::lock_guard lock(mutex_);
        const auto symbol = configured || (rest && !symbol_scoped) ? "__GLOBAL__" : "BTCUSDT";
        const auto stream = configured ? (rest ? "rest_observations" : "__ALL_WS__") : (rest ? "exchange_info" : "diff_depth");
        statement("INSERT INTO operational_events(event_id,event_type,occurred_at_utc_ns,"
                  "evidence_json) VALUES(?,'INGRESS_OVERLOAD',?,?)",
                  Json::array({run + "-overload-" + stream, ns<system_clock>(),
                               Json{{"collector_instance_id", run}, {"wait_limit_ms", 250},
                                    {"market", market}, {"symbol", symbol}, {"stream", stream}}.dump()}));
    }

    void ingress_drop(const std::string& event_id, const std::string& run,
                      const Message& message) {
        std::lock_guard lock(mutex_);
        const auto& first = message.sequence.at("ingressDropFirstUtcNs");
        const auto& last = message.sequence.at("ingressDropLastUtcNs");
        statement("INSERT INTO operational_events(event_id,event_type,occurred_at_utc_ns,"
                  "evidence_json) VALUES(?,'INGRESS_DROP',?,?)",
                  Json::array({event_id, ns<system_clock>(),
                               Json{{"collector_instance_id", run},
                                    {"market", message.market},
                                    {"symbol", message.symbol},
                                    {"stream", message.stream},
                                    {"rest", message.rest},
                                    {"dropped", message.sequence.at("ingressDropped")},
                                    {"first_drop_utc_ns", first},
                                    {"last_drop_utc_ns", last},
                                    {"first_drop_monotonic_ns",
                                     message.sequence.at("ingressDropFirstMonotonicNs")},
                                    {"last_drop_monotonic_ns",
                                     message.sequence.at("ingressDropLastMonotonicNs")}}.dump()}));
    }

    void ingress_drop_summary(const std::string& event_id, const std::string& run,
                              const DropSnapshot& drop) {
        std::lock_guard lock(mutex_);
        statement("INSERT INTO operational_events(event_id,event_type,occurred_at_utc_ns,"
                  "evidence_json) VALUES(?,'INGRESS_DROP',?,?)",
                  Json::array({event_id, ns<system_clock>(),
                               Json{{"collector_instance_id", run},
                                    {"market", drop.market},
                                    {"symbol", drop.symbol},
                                    {"stream", drop.stream},
                                    {"rest", drop.rest},
                                    {"dropped", drop.count},
                                    {"tail", true},
                                    {"first_drop_utc_ns", drop.first_utc},
                                    {"last_drop_utc_ns", drop.last_utc},
                                    {"first_drop_monotonic_ns", drop.first_mono},
                                    {"last_drop_monotonic_ns", drop.last_mono}}.dump()}));
    }

    void active(const RawFile& raw, const fs::path& path) {
        std::lock_guard lock(mutex_);
        const auto now = ns<system_clock>();
        exec("BEGIN IMMEDIATE");
        statement("INSERT INTO chunks(chunk_id,state,partial_path,created_at_utc_ns,updated_at_utc_ns)"
                  " VALUES(?,'ACTIVE',?,?,?)",
                  Json::array({raw.chunk_id, path.string(), raw.created, now}));
        transition(raw.chunk_id, nullptr, "ACTIVE", now, "{}");
        exec("COMMIT");
    }

    void sealing(const RawFile& raw, bool process_boundary, bool overload_gap) {
        std::lock_guard lock(mutex_);
        const auto now = ns<system_clock>();
        exec("BEGIN IMMEDIATE");
        statement(
            "UPDATE chunks SET state='SEALING',updated_at_utc_ns=? WHERE chunk_id=? AND state='ACTIVE'",
            Json::array({now, raw.chunk_id}));
        Json evidence{{"verified_frames", raw.records},
                      {"process_boundary_before_first_frame", process_boundary}};
        if (overload_gap)
            evidence["seal_forced_flags"] = Json::array({"sequence_gap"});
        transition(raw.chunk_id, "ACTIVE", "SEALING", now, evidence.dump());
        exec("COMMIT");
    }

    void sealed(const Json& manifest, const fs::path& path) {
        std::lock_guard lock(mutex_);
        const auto id = manifest.at("chunk_id").get<std::string>();
        const auto now = ns<system_clock>();
        exec("BEGIN IMMEDIATE");
        statement(
            "UPDATE chunks SET state='SEALED',partial_path=NULL,sealed_path=?,manifest_path=?,"
            "record_count=?,uncompressed_bytes=?,stored_bytes=?,uncompressed_sha256=?,stored_sha256=?,"
            "updated_at_utc_ns=? WHERE chunk_id=? AND state='SEALING'",
            Json::array({manifest.at("relative_path"), path.string(), manifest.at("record_count"),
                         manifest.at("uncompressed_bytes"), manifest.at("stored_bytes"),
                         manifest.at("uncompressed_sha256"), manifest.at("stored_sha256"), now, id}));
        transition(id, "SEALING", "SEALED", now, "{}");
        exec("COMMIT");
    }
};

class Output {
    int fd_;

  public:
    explicit Output(const fs::path& path)
        : fd_(::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600)) {
        if (fd_ < 0)
            throw std::runtime_error("create artifact failed: " + path.filename().string());
    }
    ~Output() {
        if (fd_ >= 0)
            ::close(fd_);
    }
    Output(const Output&) = delete;
    void write(const void* data, std::size_t size) {
        const auto* bytes = static_cast<const char*>(data);
        while (size) {
            const auto written = ::write(fd_, bytes, size);
            if (written < 0 && errno == EINTR)
                continue;
            if (written <= 0)
                throw std::runtime_error("artifact write failed");
            bytes += written;
            size -= static_cast<std::size_t>(written);
        }
    }
    void finish() {
        if (::fsync(fd_) != 0)
            throw std::runtime_error("artifact fsync failed");
        if (::close(std::exchange(fd_, -1)) != 0)
            throw std::runtime_error("artifact close failed");
    }
};

std::pair<std::uint64_t, std::string> compress_active(const fs::path& active, const fs::path& temporary,
                                                      std::uint64_t raw_bytes) {
    auto check = [](std::size_t result) {
        if (ZSTD_isError(result))
            throw std::runtime_error(ZSTD_getErrorName(result));
        return result;
    };
    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> context(ZSTD_createCCtx(), ZSTD_freeCCtx);
    if (!context)
        throw std::runtime_error("Zstd initialization failed");
    check(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_compressionLevel, 3));
    check(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_checksumFlag, 1));
    check(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_contentSizeFlag, 1));
    check(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_dictIDFlag, 0));
    check(ZSTD_CCtx_setPledgedSrcSize(context.get(), raw_bytes));
    std::ifstream input(active, std::ios::binary);
    if (!input)
        throw std::runtime_error("active read for compression failed");
    Output output(temporary);
    Sha256 digest;
    Bytes source(ZSTD_CStreamInSize()), destination(ZSTD_CStreamOutSize());
    std::uint64_t stored = 0;
    bool done = false;
    while (!done) {
        input.read(reinterpret_cast<char*>(source.data()), static_cast<std::streamsize>(source.size()));
        if (input.bad() || (input.fail() && !input.eof()))
            throw std::runtime_error("active read failed");
        ZSTD_inBuffer in{source.data(), static_cast<std::size_t>(input.gcount()), 0};
        const auto mode = input.eof() ? ZSTD_e_end : ZSTD_e_continue;
        std::size_t remaining;
        do {
            ZSTD_outBuffer out{destination.data(), destination.size(), 0};
            remaining = check(ZSTD_compressStream2(context.get(), &out, &in, mode));
            output.write(destination.data(), out.pos);
            digest.add(destination.data(), out.pos);
            stored += out.pos;
        } while (in.pos != in.size || (mode == ZSTD_e_end && remaining));
        done = mode == ZSTD_e_end;
    }
    output.finish();
    return {stored, digest.finish()};
}

class RecorderStorage {
    struct Slot {
        std::unique_ptr<RawFile> raw;
        fs::path active;
        steady_clock::time_point opened;
        std::int64_t last_append = 0;
        bool first = true;
        bool overload_gap = false;
    };
    struct SealJob {
        std::unique_ptr<RawFile> raw;
        fs::path active;
        bool first = false;
        bool overload_gap = false;
        bool rest = false;
    };
    fs::path root_;
    fs::path container_;
    bool partitioned_ = false;
    std::string partition_id_;
    std::string partition_day_;
    std::int64_t partition_created_ns_ = 0;
    bool partition_closed_ = false;
    std::optional<Catalog> catalog_;
    std::string run_;
    milliseconds period_;
    std::uint64_t byte_limit_;
    milliseconds sync_period_;
    std::map<std::string, Slot> slots_;
    DepthBridge depth_;
    DepthRequests* requests_;
    std::map<std::string, std::unique_ptr<DepthBridge>> bridges_;
    Bytes scratch_;
    steady_clock::time_point next_sync_;
    std::int64_t sync_ns_ = 0;
    std::uint64_t ingress_drop_sequence_ = 0;
    bool async_seal_ = false;
    std::thread sealer_;
    std::mutex seal_mutex_;
    std::condition_variable seal_ready_;
    std::deque<SealJob> seal_jobs_;
    bool seal_stopping_ = false;
    std::exception_ptr seal_error_;
    std::atomic<bool> seal_failed_{false};
    std::atomic<std::int64_t> seal_ns_{0};
    std::atomic<std::uint64_t> seal_queue_peak_{0};
    std::atomic<std::int64_t> seal_enqueue_wait_ns_{0};
    std::atomic<std::int64_t> seal_enqueue_wait_max_ns_{0};
    std::atomic<std::uint64_t> seal_pending_raw_bytes_{0};
    std::int64_t catalog_wait_ns_ = 0;
    std::int64_t catalog_wait_max_ns_ = 0;
    static constexpr std::size_t seal_queue_capacity = 256;

    std::array<std::uint64_t, 1252> append_histogram_{};
    std::uint64_t append_count_ = 0;

    void note_durability(std::int64_t value) {
        auto current = max_durability_ns.load(std::memory_order_relaxed);
        while (value > current &&
               !max_durability_ns.compare_exchange_weak(
                   current, value, std::memory_order_relaxed)) {
        }
    }

    void stop_sealer() {
        {
            std::lock_guard lock(seal_mutex_);
            seal_stopping_ = true;
        }
        seal_ready_.notify_all();
        if (sealer_.joinable())
            sealer_.join();
    }

    void enqueue_seal(SealJob job) {
        const auto wait_start = ns<steady_clock>();
        std::unique_lock lock(seal_mutex_);
        seal_ready_.wait(lock, [&] {
            return seal_stopping_ ||
                   seal_jobs_.size() < seal_queue_capacity;
        });
        const auto waited = ns<steady_clock>() - wait_start;
        seal_enqueue_wait_ns_.fetch_add(waited, std::memory_order_relaxed);
        auto previous = seal_enqueue_wait_max_ns_.load(std::memory_order_relaxed);
        while (waited > previous &&
               !seal_enqueue_wait_max_ns_.compare_exchange_weak(
                   previous, waited, std::memory_order_relaxed)) {
        }
        if (seal_stopping_)
            throw std::runtime_error("background sealer stopped");
        seal_pending_raw_bytes_.fetch_add(job.raw->bytes,
                                          std::memory_order_relaxed);
        seal_jobs_.push_back(std::move(job));
        auto peak = seal_queue_peak_.load(std::memory_order_relaxed);
        while (seal_jobs_.size() > peak &&
               !seal_queue_peak_.compare_exchange_weak(
                   peak, seal_jobs_.size(), std::memory_order_relaxed)) {
        }
        seal_ready_.notify_one();
    }

    void sealer_loop() {
        while (true) {
            SealJob job;
            {
                std::unique_lock lock(seal_mutex_);
                seal_ready_.wait(lock, [&] {
                    return seal_stopping_ || !seal_jobs_.empty();
                });
                if (seal_jobs_.empty()) {
                    if (seal_stopping_)
                        return;
                    continue;
                }
                job = std::move(seal_jobs_.front());
                seal_jobs_.pop_front();
                seal_pending_raw_bytes_.fetch_sub(job.raw->bytes,
                                                  std::memory_order_relaxed);
                seal_ready_.notify_all();
            }
            try {
                seal(std::move(job.raw), job.active, job.first, job.overload_gap,
                     job.rest);
            } catch (...) {
                std::lock_guard lock(seal_mutex_);
                seal_error_ = std::current_exception();
                seal_failed_.store(true, std::memory_order_relaxed);
                seal_stopping_ = true;
                seal_jobs_.clear();
                seal_pending_raw_bytes_.store(0, std::memory_order_relaxed);
                seal_ready_.notify_all();
                return;
            }
        }
    }

    void seal(std::unique_ptr<RawFile> raw, const fs::path& active, bool first,
              bool overload_gap, bool rest) {
        (void)rest;
        const auto rotate_start = ns<steady_clock>();
        note_durability(raw->finish());
#ifdef SBMDR_DIAGNOSTICS
        const auto key = rest ? "rest" : "ws";
        diagnostics[key] = {
            {"records", raw->records},
            {"rest_preparation_ns", raw->rest_preparation_ns},
            {"preparation_ns", raw->preparation_ns},
            {"framing_ns", raw->framing_ns},
            {"digest_write_ns", raw->digest_write_ns},
            {"receive_to_enqueue_ns_total", raw->receive_to_enqueue_ns},
            {"enqueue_to_append_ns_total", raw->enqueue_to_append_ns},
            {"fsync_count", raw->sync_count},
            {"fsync_ns_total", raw->sync_total_ns},
            {"worst_sync", raw->worst_sync},
        };
#endif
        catalog_->sealing(*raw, first, overload_gap);
        const auto basename = hex(raw->identity);
        const auto sealed = fs::path("data/sealed") / (basename + ".bmdr.zst");
        const auto manifest_path =
            fs::path("data/manifests") / (basename + ".manifest.json");
        const auto compressed_tmp = root_ / (sealed.string() + ".partial");
        const auto manifest_tmp = root_ / (manifest_path.string() + ".partial");
        auto [stored_bytes, stored_hash] =
            compress_active(root_ / active, compressed_tmp, raw->bytes);
        const auto sealed_at = ns<system_clock>();
        Json manifest = {
            {"manifest_schema_version", "raw-chunk-manifest.v1"},
            {"chunk_schema_version", "raw-chunk.v1"},
            {"envelope_schema_version", "event-envelope.v1"},
            {"chunk_id", raw->chunk_id},
            {"collector_instance_ids", {run_}},
            {"collector_version", "c1"},
            {"connection_ids", raw->connections},
            {"created_at_utc_ns", raw->created},
            {"sealed_at_utc_ns", sealed_at},
            {"fsync_completed_at_utc_ns", raw->fsync_utc},
            {"market", raw->market},
            {"symbol", raw->symbol},
            {"stream", raw->stream},
            {"receive_time_utc_range_ns", raw->utc_range},
            {"receive_monotonic_range_ns", raw->mono_range},
            {"exchange_time_ranges", raw->exchange_ranges},
            {"sequence_ranges", raw->sequence_ranges},
            {"capture_flags", raw->flags},
            {"gap", raw->flags.contains("sequence_gap")},
            {"overlap", false},
            {"resync", raw->flags.contains("depth_bridge")},
            {"complete", !raw->flags.contains("sequence_gap")},
            {"recovered", false},
            {"recovery", nullptr},
            {"record_count", raw->records},
            {"uncompressed_bytes", raw->bytes},
            {"uncompressed_sha256", raw->digest.finish()},
            {"stored_bytes", stored_bytes},
            {"stored_sha256", stored_hash},
            {"relative_path", sealed.string()},
            {"compression",
             {{"schema_version", "zstd-frame.v1"},
              {"level", 3},
              {"threads", 0},
              {"checksum", true},
              {"content_size", true},
              {"dictionary_id", false}}},
        };
        if (first)
            manifest["process_boundary_before_first_frame"] = true;
        const auto text = nlohmann::json(manifest).dump() + "\n";
        if (fs::exists(root_ / sealed) || fs::exists(root_ / manifest_path))
            throw std::runtime_error("refusing to replace immutable Raw artifacts");
        fs::rename(compressed_tmp, root_ / sealed);
        sync_directory((root_ / sealed).parent_path());
        Output manifest_output(manifest_tmp);
        manifest_output.write(text.data(), text.size());
        manifest_output.finish();
        fs::rename(manifest_tmp, root_ / manifest_path);
        sync_directory((root_ / manifest_path).parent_path());
        catalog_->sealed(manifest, manifest_path);
        fs::remove(root_ / active);
        sync_directory((root_ / active).parent_path());
        ++sealed_count;
        seal_ns_.fetch_add(ns<steady_clock>() - rotate_start,
                           std::memory_order_relaxed);
    }

    void rotate(Slot& slot, bool rest, const char* reason) {
        if (!slot.raw)
            return;
        const auto rotate_start = ns<steady_clock>();
        const auto active = slot.active;
        const auto first = slot.first;
        const auto overload_gap = slot.overload_gap;
        if (async_seal_) {
            SealJob job{std::move(slot.raw), active, first, overload_gap, rest};
            enqueue_seal(std::move(job));
        } else {
            seal(std::move(slot.raw), active, first, overload_gap, rest);
        }
        slot.first = false;
        slot.overload_gap = false;
        rotations[reason] = rotations.value(reason, 0) + 1;
        if (!async_seal_)
            (void)rotate_start;
    }

    template <class Function>
    void writer_catalog(Function&& function) {
        const auto start = ns<steady_clock>();
        function();
        const auto elapsed = ns<steady_clock>() - start;
        catalog_wait_ns_ += elapsed;
        catalog_wait_max_ns_ = std::max(catalog_wait_max_ns_, elapsed);
    }

  public:
    Json seal_status() {
        std::lock_guard lock(seal_mutex_);
        return {{"queue_depth", seal_jobs_.size()},
                {"queue_peak", seal_queue_peak_.load(std::memory_order_relaxed)},
                {"enqueue_wait_ns_total",
                 seal_enqueue_wait_ns_.load(std::memory_order_relaxed)},
                {"enqueue_wait_max_ns",
                 seal_enqueue_wait_max_ns_.load(std::memory_order_relaxed)},
                {"pending_raw_bytes",
                 seal_pending_raw_bytes_.load(std::memory_order_relaxed)},
                {"catalog_wait_ns_total", catalog_wait_ns_},
                {"catalog_wait_max_ns", catalog_wait_max_ns_}};
    }

    std::atomic<std::uint64_t> sealed_count{0};
    std::atomic<std::int64_t> max_durability_ns{0};
    std::int64_t max_append_ns = 0;
    Json rotations = Json::object();
#ifdef SBMDR_DIAGNOSTICS
    Json diagnostics = Json::object();
#endif

    std::uint64_t append_p99_ms() const {
        const auto threshold = (append_count_ * 99 + 99) / 100;
        std::uint64_t seen = 0;
        for (std::size_t bucket = 0; bucket < append_histogram_.size(); ++bucket) {
            seen += append_histogram_[bucket];
            if (seen >= threshold)
                return bucket;
        }
        return append_histogram_.size() - 1;
    }

    std::int64_t last_receive_utc_ns() const {
        std::int64_t last = 0;
        for (const auto& [key, slot] : slots_) {
            if (!slot.raw)
                continue;
            const auto value = slot.raw->utc_range.value("max", std::int64_t{0});
            last = std::max(last, value);
        }
        return last;
    }

    void start_sealer() {
        if (!async_seal_ || sealer_.joinable())
            return;
        seal_stopping_ = false;
        seal_error_ = nullptr;
        seal_failed_.store(false, std::memory_order_relaxed);
        sealer_ = std::thread([this] { sealer_loop(); });
    }

    void seal_all(const char* reason) {
        try {
            for (auto& [key, slot] : slots_) {
                if (slot.raw)
                    rotate(slot, slot.raw->stream == "rest_observations", reason);
            }
        } catch (...) {
            if (async_seal_) {
                stop_sealer();
                if (seal_error_)
                    std::rethrow_exception(seal_error_);
            }
            throw;
        }
        if (async_seal_) {
            stop_sealer();
            if (seal_error_)
                std::rethrow_exception(seal_error_);
        }
    }

    void switch_partition(std::int64_t now_ns) {
        if (!partitioned_ || partition_closed_)
            return;
        const auto last = last_receive_utc_ns();
        const auto prior_catalog = root_ / "state/catalog.sqlite";
        seal_all("partition");
        partition_layout::close_partition(root_, last, now_ns);
        catalog_->checkpoint();
        root_ = partition_layout::create_partition(container_, now_ns);
        partition_id_ = root_.filename().string();
        partition_day_ = partition_layout::utc_day(now_ns);
        partition_created_ns_ = now_ns;
        partition_closed_ = false;
        catalog_.reset();
        catalog_.emplace(root_ / "state/catalog.sqlite");
        catalog_->copy_archive_targets_from(prior_catalog);
        rotations["partition"] = rotations.value("partition", 0) + 1;
        start_sealer();
    }

    void maybe_rotate_partition(bool allow_time_rotation) {
        if (!partitioned_ || !allow_time_rotation || partition_closed_)
            return;
        const auto now = ns<system_clock>();
        const auto day = partition_layout::utc_day(now);
        if (day != partition_day_)
            switch_partition(now);
    }

    RecorderStorage(fs::path root, std::string run, milliseconds period,
                    std::uint64_t bytes, milliseconds sync_period,
                    DepthRequests* depth_requests = nullptr,
                    bool async_seal = false, bool partitioned = false)
        : root_(std::move(root)), partitioned_(partitioned), run_(std::move(run)),
          period_(period), byte_limit_(bytes), sync_period_(sync_period),
          depth_(depth_requests), requests_(depth_requests) {
        async_seal_ = async_seal;
        next_sync_ = steady_clock::now() + sync_period_;
        if (partitioned_) {
            container_ = root_;
            fs::create_directories(container_ / "partitions");
            fs::create_directories(container_ / "state");
            const auto now = ns<system_clock>();
            auto id = partition_layout::read_current(container_);
            bool reuse = false;
            if (!id.empty()) {
                const auto candidate = container_ / "partitions" / id;
                if (!fs::is_directory(candidate))
                    throw std::runtime_error("CURRENT points to a missing partition");
                const auto metadata = partition_layout::read_json(
                    candidate / "partition.json");
                const auto state = metadata.value("state", std::string{});
                const auto created =
                    metadata.value("created_at_utc_ns", std::int64_t{0});
                if (state == "open" &&
                    partition_layout::utc_day(created) ==
                        partition_layout::utc_day(now)) {
                    root_ = candidate;
                    partition_id_ = id;
                    partition_day_ = partition_layout::utc_day(created);
                    partition_created_ns_ = created;
                    reuse = true;
                } else {
                    partition_layout::close_partition(candidate, now, now);
                }
            }
            if (!reuse) {
                root_ = partition_layout::create_partition(container_, now);
                partition_id_ = root_.filename().string();
                partition_day_ = partition_layout::utc_day(now);
                partition_created_ns_ = now;
            }
            partition_layout::ensure_partition_dirs(root_);
        }
        catalog_.emplace(root_ / "state/catalog.sqlite");
        catalog_->startup(run_);
        start_sealer();
    }

    ~RecorderStorage() {
        if (async_seal_)
            stop_sealer();
    }

    double take_sync_ms() {
        return static_cast<double>(std::exchange(sync_ns_, 0)) / 1e6;
    }
    double take_seal_ms() {
        return static_cast<double>(seal_ns_.exchange(0)) / 1e6;
    }

    void tick(bool allow_time_rotation = true) {
        const auto now = steady_clock::now();
        if (now >= next_sync_) {
            const auto sync_start = ns<steady_clock>();
            bool synced = false;
#ifdef __linux__
            if (sync_period_ >= milliseconds(5000)) {
                int descriptor = -1;
                for (auto& [key, slot] : slots_) {
                    if (slot.raw && slot.raw->has_pending_sync()) {
                        descriptor = slot.raw->descriptor();
                        break;
                    }
                }
                if (descriptor >= 0) {
                    if (::syncfs(descriptor) != 0)
                        throw std::runtime_error("Raw syncfs failed");
                    for (auto& [key, slot] : slots_) {
                        if (slot.raw)
                            note_durability(slot.raw->syncfs_complete());
                    }
                    synced = true;
                }
            }
#endif
            if (!synced) {
                for (auto& [key, slot] : slots_) {
                    if (slot.raw)
                        note_durability(slot.raw->sync());
                }
            }
            sync_ns_ += ns<steady_clock>() - sync_start;
            next_sync_ = now + sync_period_;
        }
        if (!allow_time_rotation)
            return;
        auto oldest = slots_.end();
        for (auto it = slots_.begin(); it != slots_.end(); ++it) {
            const auto& slot = it->second;
            if (slot.raw && now - slot.opened >= period_ &&
                (oldest == slots_.end() || slot.opened < oldest->second.opened))
                oldest = it;
        }
        if (oldest != slots_.end()) {
            auto& slot = oldest->second;
            rotate(slot, slot.raw->stream == "rest_observations", "time");
        }
        maybe_rotate_partition(allow_time_rotation);
    }

    void append(Message& message) {
        if (async_seal_ && seal_failed_.load(std::memory_order_relaxed))
            throw std::runtime_error("background sealer failed");
        if (std::any_of(message.flags.begin(), message.flags.end(),
                        [](const auto& flag) { return flag == "ingress_drop"; })) {
            const auto event_id =
                run_ + "-ingress-drop-" + std::to_string(++ingress_drop_sequence_);
            writer_catalog(
                [&] { catalog_->ingress_drop(event_id, run_, message); });
        }
        if (requests_ && !requests_->children.empty() &&
            (message.stream == "diff_depth" ||
             message.source_stream == "depth_snapshot")) {
            auto& bridge = bridges_[message.symbol];
            if (!bridge)
                bridge = std::make_unique<DepthBridge>(
                    &requests_->for_symbol(message.symbol));
            bridge->observe(message);
        } else if (!requests_ || requests_->children.empty()) {
            depth_.observe(message);
        }
        const std::string symbol =
            message.rest && !message.symbol_scoped ? "__GLOBAL__" : message.symbol;
        auto& slot = slots_[message.market + "/" + symbol +
                            (message.rest ? "/rest" : "/" + message.stream)];
        const bool first = !slot.raw;
        if (first) {
            slot.raw = std::make_unique<RawFile>();
            slot.active =
                fs::path("data/active") / (hex(slot.raw->identity) + ".bmdr.partial");
            slot.opened = steady_clock::now();
        }
        slot.raw->append(root_ / slot.active, message, run_, "c1", &scratch_,
                         !async_seal_);
        if (first)
            writer_catalog([&] { catalog_->active(*slot.raw, slot.active); });
        const auto append_ns = ns<steady_clock>() - message.mono;
        slot.last_append = append_ns + message.mono;
        max_append_ns = std::max(max_append_ns, append_ns);
        const auto bucket = static_cast<std::size_t>((append_ns + 999999) / 1000000);
        ++append_histogram_[std::min(bucket, append_histogram_.size() - 1)];
        ++append_count_;
        if (slot.raw->bytes >= byte_limit_)
            rotate(slot, message.rest, "size");
    }

    Json source_status() const {
        Json rows = Json::object();
        for (const auto& [key, slot] : slots_) if (slot.raw) {
            const auto& raw = *slot.raw;
            rows[key] = {{"active_chunk_records", raw.records},
                         {"last_receive_utc_ns", raw.utc_range.at("max")},
                         {"last_append_monotonic_ns", slot.last_append},
                         {"fsync_completed_utc_ns", raw.fsync_utc},
                         {"capture_flags", raw.flags}};
        }
        return rows;
    }

    void mark_overload(bool rest, bool symbol_scoped, const std::string& market,
                       bool configured = false) {
        writer_catalog([&] {
            catalog_->overload(run_, rest, symbol_scoped, market, configured);
        });
        for (auto& [key, slot] : slots_) {
            if (slot.raw && (slot.raw->stream == "rest_observations") == rest) {
                slot.overload_gap = true;
                slot.raw->flags.insert("sequence_gap");
            }
        }
    }

    void record_tail_drop(const DropSnapshot& drop) {
        const auto event_id =
            run_ + "-ingress-tail-" + std::to_string(++ingress_drop_sequence_);
        catalog_->ingress_drop_summary(event_id, run_, drop);
        const auto found = slots_.find(drop.slot_key);
        if (found != slots_.end() && found->second.raw) {
            found->second.raw->flags.insert("sequence_gap");
            found->second.raw->flags.insert("ingress_drop");
            found->second.overload_gap = true;
        }
    }

    void finish() {
        const auto now = ns<system_clock>();
        const auto last = last_receive_utc_ns();
        seal_all("shutdown");
        if (partitioned_ && !partition_closed_) {
            partition_layout::close_partition(root_, last, now);
            partition_closed_ = true;
        }
    }
};
