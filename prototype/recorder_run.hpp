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
    std::deque<std::pair<steady_clock::time_point, unsigned>> attempts_;
    std::size_t limit_, used_ = 0;
    seconds period_;

  public:
    AttemptWindow(std::size_t limit, seconds period) : limit_(limit), period_(period) {}
    steady_clock::time_point ready_at(steady_clock::time_point now, unsigned weight = 1) {
        while (!attempts_.empty() && attempts_.front().first + period_ <= now) {
            used_ -= attempts_.front().second;
            attempts_.pop_front();
        }
        return used_ + weight <= limit_ ? now : attempts_.front().first + period_;
    }
    void charge(steady_clock::time_point now, unsigned weight = 1) {
        if (weight) {
            attempts_.emplace_back(now, weight);
            used_ += weight;
        }
    }
};

void status_line(const Json& event) { std::fprintf(stderr, "%s\n", event.dump().c_str()); }

seconds cooldown_delay(const RestResult& result) {
    std::uint64_t requested = 0;
    const auto* end = result.retry_after.data() + result.retry_after.size();
    const auto parsed = std::from_chars(result.retry_after.data(), end, requested);
    if (parsed.ec == std::errc::result_out_of_range || requested > 86400)
        throw std::overflow_error("REST Retry-After exceeds one day; operator restart required");
    if (parsed.ec != std::errc{} || parsed.ptr != end) {
        const auto date = curl_getdate(result.retry_after.c_str(), nullptr);
        const auto now = std::time(nullptr);
        if (date > now && date - now > 86400)
            throw std::overflow_error("REST Retry-After exceeds one day; operator restart required");
        requested = date > now ? static_cast<std::uint64_t>(date - now)
                              : (result.status == 418 ? 3600 : 300);
    }
    return seconds(std::max<std::uint64_t>(1, requested));
}

void run_core_rest(const std::string& port, Handoff& queue, const std::optional<std::string>& ca,
                   const std::string& run, std::stop_token stop, DepthRequests& depth, bool usdm,
                   milliseconds period, const CaptureConfig* config = nullptr) {
    struct Poll { RestSource source; steady_clock::time_point due; milliseconds period; };
    const auto start = steady_clock::now();
    const auto symbols = config ? config->symbols : std::vector<std::string>{"BTCUSDT"};
    std::vector<Poll> polls;
    auto directory = directory_source(usdm);
    if (config) directory.parameters = usdm ? Json::object() : Json{{"symbols", config->symbols}};
    polls.push_back({directory, config ? start + hours(24) : start, duration_cast<milliseconds>(hours(24))});
    const auto sources = periodic_sources();
    if (usdm) {
        std::size_t index = 0;
        for (const auto& symbol : symbols) for (auto source : sources) {
            source.parameters["symbol"] = symbol;
            polls.push_back({source, start + period * (++index) / (symbols.size()*sources.size()+1), period});
        }
    }
    std::vector<std::uint64_t> completed(symbols.size(), 0);
    std::vector<steady_clock::time_point> retry(symbols.size(), start);
    auto cooldown = start;
    auto epochs = [&] {
        std::vector<std::uint64_t> result;
        for (const auto& symbol : symbols) result.push_back(depth.for_symbol(symbol).epoch.load());
        return result;
    };
    AttemptWindow requests(100, seconds(60)), weights(usdm ? 1200 : 2000, seconds(60)),
                  statistics(800, seconds(300));
    const auto multi = make_rest_multi();
    if (config) {
        // The preflight directory request already consumed this market's IP budget.
        requests.charge(start);
        weights.charge(start, directory.weight);
    }
    while (!stop.stop_requested()) {
        const auto observed = epochs();
        auto next = std::min_element(polls.begin(), polls.end(), [](const auto& a, const auto& b) { return a.due < b.due; });
        std::size_t selected = symbols.size();
        for (std::size_t i = 0; i < symbols.size(); ++i)
            if (observed[i] != completed[i] && (selected == symbols.size() || retry[i] < retry[selected])) selected = i;
        const bool take_snapshot = selected != symbols.size();
        auto source = take_snapshot ? depth_source(usdm) : next->source;
        const auto epoch = take_snapshot ? observed[selected] : 0;
        if (take_snapshot) source.parameters["symbol"] = symbols[selected];
        const auto now = steady_clock::now();
        auto due = std::max({take_snapshot ? retry[selected] : next->due, cooldown,
                             requests.ready_at(now), weights.ready_at(now, source.weight)});
        if (source.statistics)
            due = std::max(due, statistics.ready_at(now));
        depth.wake.until(stop, due, [&] { return epochs() != observed; });
        if (stop.stop_requested())
            break;
        if (epochs() != observed)
            continue;
        if (!queue.rest_admissible()) {
            depth.wake.until(stop, steady_clock::now() + milliseconds(100));
            continue;
        }
        const auto dispatch = steady_clock::now();
        requests.charge(dispatch);
        weights.charge(dispatch, source.weight);
        if (source.statistics)
            statistics.charge(dispatch);
        status_line(Json{{"event", "REST_DISPATCH"}, {"source", source.stream},
                         {"snapshot_epoch", take_snapshot ? epoch : 0}, {"weight", source.weight}});
        RestResult result{0, false, {}, {}};
        try {
            result = receive_rest(multi.get(), port, queue, ca, true, run, &source, usdm,
                                  take_snapshot ? epoch : 0, source.stream == "exchange_info" ? config : nullptr);
        } catch (const IngressOverload&) { throw; }
          catch (const std::runtime_error& error) {
            status_line(Json{{"event", "REST_TRANSPORT_FAILURE"}, {"source", source.stream}, {"error", error.what()}});
        }
        if (result.status == 418 || result.status == 429) {
            const auto delay = cooldown_delay(result);
            cooldown = steady_clock::now() + delay;
            status_line(Json{{"event", "REST_COOLDOWN"}, {"status", result.status}, {"retry_after_seconds", delay.count()}});
        }
        if (!result.transfer_error.empty())
            status_line(Json{{"event", "REST_TRANSPORT_FAILURE"}, {"source", source.stream},
                             {"status", result.status}, {"error", result.transfer_error}});
        if (take_snapshot) {
            if (result.valid)
                completed[selected] = epoch;
            retry[selected] = steady_clock::now() + seconds(1);
        } else {
            // A valid old/empty bucket is retained once. No catch-up or old-bucket retries.
            next->due += next->period;
            if (next->due <= steady_clock::now())
                next->due = steady_clock::now() + next->period;
        }
    }
}

int record(int argc, char** argv) {
    const bool public_capture = (argc == 6 || argc == 7 || argc == 8) && std::string_view(argv[2]) == "--public";
    const bool core = public_capture || argc == 11 || argc == 12;
    const std::string market = core ? (public_capture ? (argc >= 7 ? argv[6] : "spot") : argv[10]) : "spot";
    if (market != "spot" && market != "usdm")
        throw std::runtime_error("market must be spot or usdm");
    const bool usdm = market == "usdm";
    const auto config = (public_capture && argc == 8) || (!public_capture && argc == 12) ? std::optional<CaptureConfig>(CaptureConfig::load(public_capture ? argv[7] : argv[11], usdm)) : std::nullopt;
    if (!public_capture && argc != 7 && argc != 9 && argc != 10 && argc != 11 && argc != 12) {
        throw std::runtime_error(
            "usage: sbmdr-c0 record WS_PORT HTTP_PORT WS_COUNT OUTPUT_DIR REST_INTERVAL_MS "
            "[ROTATE_MS ROTATE_BYTES [TEST_PAUSE_MS [spot|usdm]]] | record --public OUTPUT_DIR WS_COUNT CA_FILE [spot|usdm]");
    }
    const std::string ws_port = public_capture ? "" : argv[2], http_port = public_capture ? "" : argv[3];
    const auto ws_count = std::stoull(argv[4]);
    const fs::path root(public_capture ? argv[3] : argv[5]);
    const auto ca = public_capture ? std::optional<std::string>(argv[5]) : std::nullopt;
    const auto rest_period = public_capture ? milliseconds(300000) : milliseconds(std::stoll(argv[6]));
    const auto rotation = argc >= 9 ? milliseconds(std::stoll(argv[7])) : milliseconds(900000);
    const auto rotation_bytes = argc >= 9 ? std::stoull(argv[8]) : (public_capture ? 32ULL : 128ULL) * 1024 * 1024;
    const auto test_pause = !public_capture && argc >= 10 ? milliseconds(std::stoll(argv[9])) : milliseconds(0);
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
    DepthRequests depth_requests;
    if (config) {
        depth_requests.initialize(*config);
        auto directory = directory_source(usdm);
        directory.parameters = usdm ? Json::object() : Json{{"symbols", config->symbols}};
        auto multi = make_rest_multi();
        receive_rest(multi.get(), http_port, queue, ca, true, run_id, &directory, usdm, 0, &*config);
    }
    std::stop_source stop;
    std::atomic<unsigned> overloads{0};
    std::size_t records = 0;
    auto writer = std::async(std::launch::async, [&] {
        try {
            RecorderStorage storage(root, run_id, rotation, rotation_bytes, core ? &depth_requests : nullptr);
            std::vector<Message> batch;
            batch.reserve(32);
            auto next_status = start + seconds(1);
            auto next_sources = start + seconds(60);
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
                // Drain ingress between seals; a market-wide rotation must not monopolize the writer.
                storage.tick(queue.snapshot().first == 0);
                for (auto& message : batch) {
                    storage.append(message);
                    ++records;
                    storage.tick(false);
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
                    if (config && now >= next_sources) {
                        status_line(Json{{"event", "SOURCE_STATUS"}, {"sources", storage.source_status()}});
                        next_sources = now + seconds(60);
                    }
                    previous_status = now;
                    previous_cpu = cpu;
                    next_status = now + seconds(1);
                }
            }
            const auto affected = overloads.load();
            if (affected & 1U)
                storage.mark_overload(false, public_capture, usdm ? "um_perpetual" : "spot", config.has_value());
            if (affected & 2U)
                storage.mark_overload(true, public_capture, usdm ? "um_perpetual" : "spot", config.has_value());
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
    auto launch_ws = [&](std::string target) { return std::async(std::launch::async, [&, target] {
        try {
            std::size_t accepted = 0;
            unsigned generation = 0, consecutive = 0;
            RecorderWake wake;
            AttemptWindow budget(config && usdm ? 90 : 180, seconds(300));
            while (accepted < ws_count && !token.stop_requested()) {
                wake.until(token, budget.ready_at(steady_clock::now()));
                if (token.stop_requested())
                    break;
                budget.charge(steady_clock::now());
                const auto before = accepted;
                try {
                    receive_ws(ws_port, ws_count - accepted, queue, token, ca, &accepted, generation, usdm, config ? &*config : nullptr, target);
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
                    wake.until(token, steady_clock::now() + delay);
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
    };
    auto ws = launch_ws(config ? config->target(usdm, false) : "");
    std::optional<std::future<void>> ws_market;
    if (config && usdm) ws_market.emplace(launch_ws(config->target(true, true)));
    auto rest = std::async(std::launch::async, [&] {
        try {
            if (core) {
                run_core_rest(http_port, queue, ca, run_id, token, depth_requests, usdm, rest_period, config ? &*config : nullptr);
                return;
            }
            RecorderWake wake;
            auto due = steady_clock::now();
            unsigned failures = 0;
            AttemptWindow budget(100, seconds(60));
            const auto multi = make_rest_multi();
            while (!token.stop_requested()) {
                wake.until(token, std::max(due, budget.ready_at(steady_clock::now())));
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
    if (ws_market) {
        try { ws_market->get(); } catch (...) { if (!failure) failure = std::current_exception(); }
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
