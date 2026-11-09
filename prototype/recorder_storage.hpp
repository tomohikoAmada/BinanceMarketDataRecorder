// One homogeneous Raw v1 chunk per market/symbol/stream, owned by the writer.
namespace fs = std::filesystem;

class Catalog {
    std::unique_ptr<sqlite3, decltype(&sqlite3_close)> db_{nullptr, sqlite3_close};

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

    void startup(const std::string& run) {
        statement("INSERT INTO operational_events(event_id,event_type,occurred_at_utc_ns,evidence_json)"
                  " VALUES(?,'PROCESS_START',?,?)",
                  Json::array({run, ns<system_clock>(), Json{{"collector_instance_id", run}}.dump()}));
    }

    void overload(const std::string& run, bool rest, bool symbol_scoped, const std::string& market, bool configured) {
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
        const auto& first = message.sequence.at("ingressDropFirstUtcNs");
        const auto& last = message.sequence.at("ingressDropLastUtcNs");
        statement("INSERT INTO operational_events(event_id,event_type,occurred_at_utc_ns,"
                  "evidence_json) VALUES(?,'INGRESS_DROP',?,?)",
                  Json::array({event_id, ns<system_clock>(),
                               Json{{"collector_instance_id", run},
                                    {"market", message.market},
                                    {"symbol", message.symbol},
                                    {"stream", message.stream},
                                    {"dropped", message.sequence.at("ingressDropped")},
                                    {"first_drop_utc_ns", first},
                                    {"last_drop_utc_ns", last},
                                    {"first_drop_monotonic_ns",
                                     message.sequence.at("ingressDropFirstMonotonicNs")},
                                    {"last_drop_monotonic_ns",
                                     message.sequence.at("ingressDropLastMonotonicNs")}}.dump()}));
    }

    void active(const RawFile& raw, const fs::path& path) {
        const auto now = ns<system_clock>();
        exec("BEGIN IMMEDIATE");
        statement("INSERT INTO chunks(chunk_id,state,partial_path,created_at_utc_ns,updated_at_utc_ns)"
                  " VALUES(?,'ACTIVE',?,?,?)",
                  Json::array({raw.chunk_id, path.string(), raw.created, now}));
        transition(raw.chunk_id, nullptr, "ACTIVE", now, "{}");
        exec("COMMIT");
    }

    void sealing(const RawFile& raw, bool process_boundary, bool overload_gap) {
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
    fs::path root_;
    Catalog catalog_;
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
    std::int64_t sync_ns_ = 0, seal_ns_ = 0;
    std::uint64_t ingress_drop_sequence_ = 0;

    std::array<std::uint64_t, 1252> append_histogram_{};
    std::uint64_t append_count_ = 0;

    void rotate(Slot& slot, bool rest, const char* reason) {
        (void)rest;
        const auto rotate_start = ns<steady_clock>();
        auto& raw = *slot.raw;
        max_durability_ns = std::max(max_durability_ns, raw.finish());
#ifdef SBMDR_DIAGNOSTICS
        const auto key = rest ? "rest" : "ws";
        diagnostics[key] = {
            {"records", raw.records},
            {"rest_preparation_ns", raw.rest_preparation_ns},
            {"preparation_ns", raw.preparation_ns},
            {"framing_ns", raw.framing_ns},
            {"digest_write_ns", raw.digest_write_ns},
            {"receive_to_enqueue_ns_total", raw.receive_to_enqueue_ns},
            {"enqueue_to_append_ns_total", raw.enqueue_to_append_ns},
            {"fsync_count", raw.sync_count},
            {"fsync_ns_total", raw.sync_total_ns},
            {"worst_sync", raw.worst_sync},
        };
#endif
        catalog_.sealing(raw, slot.first, slot.overload_gap);
        const auto basename = hex(raw.identity);
        const auto sealed = fs::path("data/sealed") / (basename + ".bmdr.zst");
        const auto manifest_path = fs::path("data/manifests") / (basename + ".manifest.json");
        const auto compressed_tmp = root_ / (sealed.string() + ".partial");
        const auto manifest_tmp = root_ / (manifest_path.string() + ".partial");
        auto [stored_bytes, stored_hash] =
            compress_active(root_ / slot.active, compressed_tmp, raw.bytes);
        const auto sealed_at = ns<system_clock>();
        Json manifest = {
            {"manifest_schema_version", "raw-chunk-manifest.v1"},
            {"chunk_schema_version", "raw-chunk.v1"},
            {"envelope_schema_version", "event-envelope.v1"},
            {"chunk_id", raw.chunk_id},
            {"collector_instance_ids", {run_}},
            {"collector_version", "c1"},
            {"connection_ids", raw.connections},
            {"created_at_utc_ns", raw.created},
            {"sealed_at_utc_ns", sealed_at},
            {"fsync_completed_at_utc_ns", raw.fsync_utc},
            {"market", raw.market},
            {"symbol", raw.symbol},
            {"stream", raw.stream},
            {"receive_time_utc_range_ns", raw.utc_range},
            {"receive_monotonic_range_ns", raw.mono_range},
            {"exchange_time_ranges", raw.exchange_ranges},
            {"sequence_ranges", raw.sequence_ranges},
            {"capture_flags", raw.flags},
            {"gap", raw.flags.contains("sequence_gap")},
            {"overlap", false},
            {"resync", raw.flags.contains("depth_bridge")},
            {"complete", !raw.flags.contains("sequence_gap")},
            {"recovered", false},
            {"recovery", nullptr},
            {"record_count", raw.records},
            {"uncompressed_bytes", raw.bytes},
            {"uncompressed_sha256", raw.digest.finish()},
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
        if (slot.first)
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
        catalog_.sealed(manifest, manifest_path);
        fs::remove(root_ / slot.active);
        sync_directory((root_ / slot.active).parent_path());
        slot.raw.reset();
        slot.first = false;
        ++sealed_count;
        rotations[reason] = rotations.value(reason, 0) + 1;
        seal_ns_ += ns<steady_clock>() - rotate_start;
    }

  public:
    std::uint64_t sealed_count = 0;
    std::int64_t max_durability_ns = 0, max_append_ns = 0;
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

    RecorderStorage(fs::path root, std::string run, milliseconds period, std::uint64_t bytes,
                    milliseconds sync_period, DepthRequests* depth_requests = nullptr)
        : root_(std::move(root)), catalog_(root_ / "state/catalog.sqlite"), run_(std::move(run)),
          period_(period), byte_limit_(bytes), sync_period_(sync_period), depth_(depth_requests),
          requests_(depth_requests) {
        next_sync_ = steady_clock::now() + sync_period_;
        catalog_.startup(run_);
    }

    double take_sync_ms() { return static_cast<double>(std::exchange(sync_ns_, 0)) / 1e6; }
    double take_seal_ms() { return static_cast<double>(std::exchange(seal_ns_, 0)) / 1e6; }

    void tick(bool allow_time_rotation = true) {
        const auto now = steady_clock::now();
        if (now >= next_sync_) {
            const auto sync_start = ns<steady_clock>();
            for (auto& [key, slot] : slots_) {
                if (slot.raw)
                    max_durability_ns = std::max(max_durability_ns, slot.raw->sync());
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
    }

    void append(Message& message) {
        if (std::any_of(message.flags.begin(), message.flags.end(),
                        [](const auto& flag) { return flag == "ingress_drop"; })) {
            const auto event_id =
                run_ + "-ingress-drop-" + std::to_string(++ingress_drop_sequence_);
            catalog_.ingress_drop(event_id, run_, message);
        }
        if (requests_ && !requests_->children.empty() && (message.stream == "diff_depth" || message.source_stream == "depth_snapshot")) {
            auto& bridge = bridges_[message.symbol];
            if (!bridge) bridge = std::make_unique<DepthBridge>(&requests_->for_symbol(message.symbol));
            bridge->observe(message);
        } else if (!requests_ || requests_->children.empty()) depth_.observe(message);
        const std::string symbol = message.rest && !message.symbol_scoped ? "__GLOBAL__" : message.symbol;
        auto& slot = slots_[message.market + "/" + symbol + (message.rest ? "/rest" : "/" + message.stream)];
        const bool first = !slot.raw;
        if (first) {
            slot.raw = std::make_unique<RawFile>();
            slot.active = fs::path("data/active") / (hex(slot.raw->identity) + ".bmdr.partial");
            slot.opened = steady_clock::now();
        }
        slot.raw->append(root_ / slot.active, message, run_, "c1", &scratch_);
        if (first)
            catalog_.active(*slot.raw, slot.active);
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
            rows[key] = {{"active_chunk_records", raw.records}, {"last_receive_utc_ns", raw.utc_range.at("max")},
                         {"last_append_monotonic_ns", slot.last_append}, {"fsync_completed_utc_ns", raw.fsync_utc}, {"capture_flags", raw.flags}};
        }
        return rows;
    }

    void mark_overload(bool rest, bool symbol_scoped, const std::string& market, bool configured = false) {
        catalog_.overload(run_, rest, symbol_scoped, market, configured);
        for (auto& [key, slot] : slots_) {
            if (slot.raw && (slot.raw->stream == "rest_observations") == rest) {
                slot.overload_gap = true;
                slot.raw->flags.insert("sequence_gap");
            }
        }
    }

    void finish() {
        for (auto& [key, slot] : slots_) {
            if (slot.raw)
                rotate(slot, slot.raw->stream == "rest_observations", "shutdown");
        }
    }
};
