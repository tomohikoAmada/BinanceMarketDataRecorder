// Local C1 recorder entry point. Network fixtures expose the Binance payloads on loopback.
static_assert(std::atomic<bool>::is_always_lock_free);
std::atomic<bool> interrupted{false};

void interrupt(int) { interrupted.store(true, std::memory_order_relaxed); }

std::int64_t peak_rss() {
    struct rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) != 0)
        throw std::runtime_error("getrusage failed");
#ifdef __APPLE__
    return usage.ru_maxrss;
#else
    return usage.ru_maxrss * 1024;
#endif
}

class AttemptWindow {
    std::deque<steady_clock::time_point> attempts_;
    std::size_t limit_;
    seconds period_;

  public:
    AttemptWindow(std::size_t limit, seconds period) : limit_(limit), period_(period) {}

    steady_clock::time_point ready_at(steady_clock::time_point now) {
        while (!attempts_.empty() && attempts_.front() + period_ <= now)
            attempts_.pop_front();
        return attempts_.size() < limit_ ? now : attempts_.front() + period_;
    }

    void charge(steady_clock::time_point now) { attempts_.push_back(now); }
};

void status_line(const Json& event) { std::fprintf(stderr, "%s\n", event.dump().c_str()); }

int record(int argc, char** argv) {
    const bool public_capture = argc == 6 && std::string_view(argv[2]) == "--public";
    if (!public_capture && argc != 7 && argc != 9 && argc != 10) {
        throw std::runtime_error(
            "usage: sbmdr-c0 record WS_PORT HTTP_PORT WS_COUNT OUTPUT_DIR REST_INTERVAL_MS "
            "[ROTATE_MS ROTATE_BYTES [TEST_PAUSE_MS]] | record --public OUTPUT_DIR WS_COUNT CA_FILE");
    }
    const std::string ws_port = public_capture ? "" : argv[2], http_port = public_capture ? "" : argv[3];
    const auto ws_count = std::stoull(argv[4]);
    const fs::path root(public_capture ? argv[3] : argv[5]);
    const auto ca = public_capture ? std::optional<std::string>(argv[5]) : std::nullopt;
    const auto rest_period = public_capture ? milliseconds(3600000) : milliseconds(std::stoll(argv[6]));
    const auto rotation = argc >= 9 ? milliseconds(std::stoll(argv[7])) : milliseconds(900000);
    const auto rotation_bytes = argc >= 9 ? std::stoull(argv[8]) : 128ULL * 1024 * 1024;
    const auto test_pause = argc == 10 ? milliseconds(std::stoll(argv[9])) : milliseconds(0);
    if (test_pause.count() < 0 || test_pause > seconds(5))
        throw std::runtime_error("test pause must be between zero and five seconds");
    if (ws_count == 0 || rest_period.count() <= 0 || rotation.count() <= 0 ||
        rotation_bytes < 1024 * 1024) {
        throw std::runtime_error("invalid recorder limits");
    }
    if (public_capture) {
        const auto home = fs::path(std::getenv("HOME") ? std::getenv("HOME") : "");
#ifdef __APPLE__
        const auto interactive = home / "Library/Application Support/BinanceMarketDataRecorder";
#else
        const auto interactive = home / ".local/share/BinanceMarketDataRecorder";
#endif
        const auto destination = fs::weakly_canonical(root);
        const auto internal = fs::weakly_canonical(interactive);
        const auto system = fs::path("/var/lib/binance-market-data-recorder");
        const auto reserved_subtree = [](const fs::path& relative) {
            return !relative.empty() && *relative.begin() == "S-BMDR";
        };
        const auto inside = [&](const fs::path& base) {
            const auto relative = destination.lexically_relative(base);
            return reserved_subtree(relative);
        };
        bool allowed = home.is_absolute() && inside(internal);
#ifndef __APPLE__
        allowed = allowed || inside(system);
#else
        (void)system;
#endif
        if (!allowed)
            throw std::runtime_error("public capture requires a dedicated internal S-BMDR data root");
        fs::create_directories(root);
        if (fs::space(root).available < 18ULL * 1024 * 1024 * 1024)
            throw std::runtime_error("public capture requires at least 18 GiB available at startup");
    }
    for (auto directory : {"state", "data/active", "data/sealed", "data/manifests"})
        fs::create_directories(root / directory);
    std::unique_ptr<FILE, decltype(&fclose)> lock(fopen((root / "state/c1.lock").c_str(), "a"), fclose);
    if (!lock || flock(fileno(lock.get()), LOCK_EX | LOCK_NB) != 0)
        throw std::runtime_error("recorder root is already locked");
    if (!fs::is_empty(root / "data/active"))
        throw std::runtime_error("active recovery is required before reuse of this recorder root");
    for (const auto* directory : {"data/sealed", "data/manifests"}) {
        for (const auto& entry : fs::directory_iterator(root / directory)) {
            if (entry.path().extension() == ".partial")
                throw std::runtime_error("seal recovery is required before reuse of this recorder root");
        }
    }
    if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK)
        throw std::runtime_error("curl global initialization failed");
    struct CurlCleanup {
        ~CurlCleanup() { curl_global_cleanup(); }
    } cleanup;
    std::signal(SIGINT, interrupt);
    std::signal(SIGTERM, interrupt);
    const auto run_id = "c1-" + uuid_text(uuid_bytes());
    const auto start = steady_clock::now();
    const auto cpu_start = std::clock();
    const auto allocation_start = allocations.load();
    const auto allocated_start = allocated_bytes.load();
    Handoff queue;
    std::stop_source stop;
    std::atomic<unsigned> overloads{0};
    std::size_t records = 0;
    auto writer = std::async(std::launch::async, [&] {
        try {
            RecorderStorage storage(root, run_id, rotation, rotation_bytes);
            std::vector<Message> batch;
            batch.reserve(32);
            auto next_status = start + seconds(1);
            auto previous_status = start;
            auto previous_cpu = cpu_start;
            double max_status_cpu = 0;
            bool disk_stop = false;
            std::int64_t pause_start = 0, pause_finish = 0, pause_drained = 0;
            while (queue.take(batch)) {
                if (!public_capture && !batch.empty() && test_pause.count() && !pause_start) {
                    pause_start = ns<steady_clock>();
                    std::this_thread::sleep_for(test_pause);
                    pause_finish = ns<steady_clock>();
                }
                storage.tick();
                for (auto& message : batch) {
                    storage.append(message);
                    ++records;
                    storage.tick();
                }
                if (pause_finish && !pause_drained && queue.snapshot().first == 0)
                    pause_drained = ns<steady_clock>();
                batch.clear();
                const auto now = steady_clock::now();
                if (now >= next_status) {
                    if (public_capture && !disk_stop &&
                        fs::space(root).available <= 13ULL * 1024 * 1024 * 1024) {
                        disk_stop = true;
                        stop.request_stop();
                    }
                    const auto [queued_records, queued_bytes] = queue.snapshot();
                    const auto cpu = std::clock();
                    const double percent = 100.0 * (cpu - previous_cpu) / CLOCKS_PER_SEC /
                                           duration<double>(now - previous_status).count();
                    max_status_cpu = std::max(max_status_cpu, percent);
                    status_line(
                        Json{{"records", records},
                             {"queued_records", queued_records},
                             {"queued_payload_bytes", queued_bytes},
                             {"sealed", storage.sealed_count},
                             {"rss_bytes", peak_rss()},
                             {"cpu_percent", percent},
                             {"max_durability_ns", storage.max_durability_ns},
                             {"elapsed_seconds", duration<double>(now - start).count()},
                             {"cpu_seconds", static_cast<double>(cpu - cpu_start) / CLOCKS_PER_SEC}});
                    previous_status = now;
                    previous_cpu = cpu;
                    next_status = now + seconds(1);
                }
            }
            const auto affected = overloads.load();
            if (affected & 1U)
                storage.mark_overload(false, public_capture);
            if (affected & 2U)
                storage.mark_overload(true, public_capture);
            storage.finish();
            Json result{{"records", records},
                        {"test_pause_start_monotonic_ns", pause_start},
                        {"test_pause_finish_monotonic_ns", pause_finish},
                        {"test_pause_backlog_drain_ns",
                         pause_drained ? Json(pause_drained - pause_finish) : Json(nullptr)},
                        {"sealed", storage.sealed_count},
                        {"rotations", storage.rotations},
                        {"max_durability_ns", storage.max_durability_ns},
                        {"max_receive_to_append_ns", storage.max_append_ns},
                        {"receive_to_append_p99_ms", storage.append_p99_ms()},
                        {"max_status_cpu_percent", max_status_cpu},
                        {"stop_reason", disk_stop ? "DISK_EMERGENCY_STOP" : "INPUT_COMPLETE"}};
#ifdef SBMDR_DIAGNOSTICS
            result["diagnostics_last_chunk_per_source"] = storage.diagnostics;
#endif
            return result;
        } catch (...) {
            stop.request_stop();
            queue.close();
            throw;
        }
    });
    const auto token = stop.get_token();
    auto ws = std::async(std::launch::async, [&] {
        try {
            std::size_t accepted = 0;
            unsigned generation = 0, consecutive = 0;
            std::mutex mutex;
            std::condition_variable_any wake;
            std::unique_lock lock(mutex);
            AttemptWindow budget(180, seconds(300));
            while (accepted < ws_count && !token.stop_requested()) {
                wake.wait_until(lock, token, budget.ready_at(steady_clock::now()), [] { return false; });
                if (token.stop_requested())
                    break;
                budget.charge(steady_clock::now());
                const auto before = accepted;
                try {
                    receive_ws(ws_port, ws_count - accepted, queue, token, ca, &accepted, generation);
                    break;
                } catch (const boost::system::system_error& error) {
                    if (token.stop_requested())
                        break;
                    if (error.code().category() == asio::error::get_ssl_category() &&
                        ERR_GET_REASON(error.code().value()) == SSL_R_CERTIFICATE_VERIFY_FAILED)
                        throw;
                    consecutive = before == accepted ? std::min(consecutive + 1, 5U) : 1;
                    ++generation;
                    const auto delay =
                        milliseconds(250 * (1U << (consecutive - 1)) +
                                     (std::hash<std::string>{}(run_id) + generation * 37) % 150);
                    status_line(Json{{"event", "WS_RECONNECT"},
                                     {"generation", generation},
                                     {"accepted", accepted},
                                     {"error", error.code().message()}});
                    wake.wait_for(lock, token, delay, [] { return false; });
                }
            }
        } catch (const IngressOverload&) {
            overloads.fetch_or(1U);
            stop.request_stop();
            throw;
        } catch (...) {
            stop.request_stop();
            throw;
        }
    });
    auto rest = std::async(std::launch::async, [&] {
        try {
            std::mutex mutex;
            std::condition_variable_any wake;
            std::unique_lock lock(mutex);
            auto due = steady_clock::now();
            unsigned failures = 0;
            AttemptWindow budget(100, seconds(60));
            const auto multi = make_rest_multi();
            while (!token.stop_requested()) {
                wake.wait_until(lock, token, std::max(due, budget.ready_at(steady_clock::now())),
                                [] { return false; });
                if (token.stop_requested())
                    break;
                if (!queue.rest_admissible()) {
                    due = steady_clock::now() + milliseconds(100);
                    continue;
                }
                const auto dispatch = steady_clock::now();
                budget.charge(dispatch);
                try {
                    const auto result = receive_rest(multi.get(), http_port, queue, ca, true, run_id);
                    if (result.status == 418 || result.status == 429) {
                        std::uint64_t requested = 0;
                        const auto* end = result.retry_after.data() + result.retry_after.size();
                        const auto parsed = std::from_chars(result.retry_after.data(), end, requested);
                        if (parsed.ec == std::errc::result_out_of_range || requested > 86400)
                            throw std::overflow_error(
                                "REST Retry-After exceeds one day; operator restart required");
                        if (parsed.ec != std::errc{} || parsed.ptr != end) {
                            const auto date = curl_getdate(result.retry_after.c_str(), nullptr);
                            const auto now = std::time(nullptr);
                            if (date > now && date - now > 86400)
                                throw std::overflow_error(
                                    "REST Retry-After exceeds one day; operator restart required");
                            requested = date > now ? static_cast<std::uint64_t>(date - now)
                                                   : (result.status == 418 ? 3600 : 300);
                        }
                        const auto delay = seconds(std::max<std::uint64_t>(1, requested));
                        due = steady_clock::now() +
                              std::max(rest_period, duration_cast<milliseconds>(delay));
                        status_line(Json{{"event", "REST_COOLDOWN"},
                                         {"status", result.status},
                                         {"retry_after_seconds", delay.count()}});
                        if (!result.transfer_error.empty())
                            status_line(Json{{"event", "REST_TRANSPORT_FAILURE"},
                                             {"status", result.status},
                                             {"error", result.transfer_error}});
                        failures = 0;
                    } else {
                        failures = result.valid ? 0 : failures + 1;
                        due = failures && failures <= 3
                                  ? steady_clock::now() + seconds(1U << (failures - 1))
                                  : std::max(steady_clock::now(), dispatch + rest_period);
                        if (failures > 3)
                            failures = 0;
                    }
                } catch (const IngressOverload&) {
                    overloads.fetch_or(2U);
                    throw;
                } catch (const std::overflow_error&) {
                    throw;
                } catch (const std::runtime_error& error) {
                    ++failures;
                    due = steady_clock::now() +
                          (failures <= 3 ? duration_cast<milliseconds>(seconds(1U << (failures - 1)))
                                         : rest_period);
                    if (failures > 3)
                        failures = 0;
                    status_line(Json{{"event", "REST_TRANSPORT_FAILURE"}, {"error", error.what()}});
                }
            }
        } catch (...) {
            stop.request_stop();
            throw;
        }
    });
    while (!interrupted.load(std::memory_order_relaxed) && !token.stop_requested() &&
           ws.wait_for(milliseconds(50)) != std::future_status::ready) {
    }
    stop.request_stop();
    std::exception_ptr failure;
    for (auto* network : {&ws, &rest}) {
        try {
            network->get();
        } catch (...) {
            if (!failure)
                failure = std::current_exception();
        }
    }
    queue.close();
    auto report = writer.get();
    if (failure)
        std::rethrow_exception(failure);
    report["peak_queue_payload_bytes"] = queue.peak_bytes;
    report["peak_queue_records"] = queue.peak_records;
    report["wall_seconds"] = duration<double>(steady_clock::now() - start).count();
    report["cpu_seconds"] = static_cast<double>(std::clock() - cpu_start) / CLOCKS_PER_SEC;
    report["peak_rss_bytes"] = peak_rss();
    report["allocation_counter_enabled"] = allocation_counter_enabled;
    report["new_calls"] =
        allocation_counter_enabled ? Json(allocations.load() - allocation_start) : Json(nullptr);
    report["new_requested_bytes"] =
        allocation_counter_enabled ? Json(allocated_bytes.load() - allocated_start) : Json(nullptr);
    report["interrupted"] = interrupted.load(std::memory_order_relaxed);
    if (report["interrupted"] == true && report["stop_reason"] == "INPUT_COMPLETE")
        report["stop_reason"] = "SIGNAL";
    std::cout << report.dump() << '\n';
    return 0;
}
