package tools

import (
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/binance"
	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/config"
	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/recorder"
)

func mergeSymbols(symbols []string, single string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, s := range symbols {
		u := strings.ToUpper(strings.TrimSpace(s))
		if u != "" && !seen[u] {
			seen[u] = true
			out = append(out, u)
		}
	}
	if u := strings.ToUpper(strings.TrimSpace(single)); u != "" && !seen[u] {
		out = append(out, u)
	}
	return out
}

func clampLimit(limit, defVal, maxVal int) int {
	if limit <= 0 {
		return defVal
	}
	if limit > maxVal {
		return maxVal
	}
	return limit
}

func parseNSBounds(startRaw, endRaw string) (int64, int64, error) {
	startNS, err := recorder.ParseNSString(startRaw)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid start_utc_ns: %w", err)
	}
	endNS, err := recorder.ParseNSString(endRaw)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid end_utc_ns: %w", err)
	}
	if startNS > 0 && endNS > 0 && endNS <= startNS {
		return 0, 0, fmt.Errorf("end_utc_ns (%d) must be greater than start_utc_ns (%d)", endNS, startNS)
	}
	return startNS, endNS, nil
}

func makeResultFilePath(cfg *config.Config, prefix, ext string) string {
	var rnd [4]byte
	_, _ = rand.Read(rnd[:])
	stamp := time.Now().UTC().Format("20060102T150405Z")
	if ext == "" {
		ext = "jsonl"
	}
	name := fmt.Sprintf("%s_%s_%s.%s", prefix, stamp, hex.EncodeToString(rnd[:]), strings.TrimPrefix(ext, "."))
	return filepath.Join(cfg.DataDir, "results", name)
}

func writeRecordsFile(path, format string, rows []map[string]any) (string, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	switch strings.ToLower(format) {
	case "csv":
		f, err := os.Create(path)
		if err != nil {
			return "", err
		}
		defer f.Close()
		if len(rows) == 0 {
			return path, nil
		}
		var headers []string
		for k := range rows[0] {
			headers = append(headers, k)
		}
		w := csv.NewWriter(f)
		if err := w.Write(headers); err != nil {
			return "", err
		}
		for _, r := range rows {
			rec := make([]string, len(headers))
			for i, h := range headers {
				rec[i] = formatScalar(r[h])
			}
			if err := w.Write(rec); err != nil {
				return "", err
			}
		}
		w.Flush()
		if wErr := w.Error(); wErr != nil {
			_ = f.Close()
			return "", wErr
		}
		return path, f.Close()
	case "json":
		encoded, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			return "", err
		}
		return path, os.WriteFile(path, encoded, 0o644)
	default:
		f, err := os.Create(path)
		if err != nil {
			return "", err
		}
		for _, r := range rows {
			line, err := json.Marshal(r)
			if err != nil {
				_ = f.Close()
				return "", err
			}
			if _, err := f.Write(append(line, '\n')); err != nil {
				_ = f.Close()
				return "", err
			}
		}
		return path, f.Close()
	}
}

func formatScalar(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case bool:
		return strconv.FormatBool(x)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

// applyResponseBudget enforces <= ResponseBudgetBytes (~64 KiB) by spilling full rows to disk
// and trimming the preview slice cleanly without truncating mid-row or breaking JSON.
func applyResponseBudget(cfg *config.Config, toolPrefix string, payload map[string]any, rowsKey string, fullRows []map[string]any, saveToFile bool, outputFormat string, limit int) (map[string]any, error) {
	budget := cfg.ResponseBudgetBytes
	if budget <= 0 {
		budget = 64 * 1024
	}
	if limit <= 0 {
		limit = cfg.PreviewDefaultRows
	}

	previewCount := len(fullRows)
	if previewCount > limit {
		previewCount = limit
	}
	payload["total_rows"] = len(fullRows)
	payload[rowsKey] = fullRows[:previewCount]
	payload["returned_rows"] = previewCount
	payload["truncated"] = previewCount < len(fullRows)

	encoded, err := json.Marshal(payload)
	mustSpill := saveToFile || previewCount < len(fullRows) || (err == nil && len(encoded) > budget)

	existingFile, _ := payload["result_file"].(string)
	if mustSpill && existingFile == "" && (saveToFile || len(fullRows) > 0) {
		ext := "jsonl"
		switch strings.ToLower(outputFormat) {
		case "csv":
			ext = "csv"
		case "json":
			ext = "json"
		}
		outPath := makeResultFilePath(cfg, toolPrefix, ext)
		written, wErr := writeRecordsFile(outPath, ext, fullRows)
		if wErr != nil {
			return nil, fmt.Errorf("write result file %s: %w", outPath, wErr)
		}
		payload["result_file"] = written
	}

	// Iteratively shrink preview rows if serialized JSON still exceeds 64 KiB budget.
	for previewCount > 0 {
		encoded, err = json.Marshal(payload)
		if err == nil && len(encoded) <= budget-512 {
			break
		}
		if previewCount == 1 {
			previewCount = 0
		} else {
			previewCount = previewCount / 2
		}
		payload[rowsKey] = fullRows[:previewCount]
		payload["returned_rows"] = previewCount
		payload["truncated"] = previewCount < len(fullRows)
	}

	if encoded, err = json.Marshal(payload); err == nil {
		payload["response_bytes"] = len(encoded)
	}
	return payload, nil
}

// applyWorkerResponseBudget applies 64 KiB budget cleanly to responses already written by python/worker.py.
func applyWorkerResponseBudget(cfg *config.Config, toolPrefix string, resp map[string]any, limit int) (map[string]any, error) {
	budget := cfg.ResponseBudgetBytes
	if budget <= 0 {
		budget = 64 * 1024
	}
	if wErrMsg, ok := resp["output_file_error"].(string); ok && wErrMsg != "" {
		return nil, fmt.Errorf("worker write failed: %s", wErrMsg)
	}
	if outFile, ok := resp["output_file"].(string); ok && outFile != "" {
		resp["result_file"] = outFile
		delete(resp, "output_file")
	}
	rowsRaw, hasRows := resp["rows"].([]any)
	if !hasRows {
		return resp, nil
	}
	totalRows := len(rowsRaw)
	if tr, ok := resp["total_rows"].(float64); ok && int(tr) > totalRows {
		totalRows = int(tr)
	}
	resp["total_rows"] = totalRows
	n := len(rowsRaw)
	if limit > 0 && n > limit {
		n = limit
	}
	resp["rows"] = rowsRaw[:n]
	resp["returned_rows"] = n
	resp["truncated"] = n < totalRows

	for n > 0 {
		b, err := json.Marshal(resp)
		if err == nil && len(b) <= budget-512 {
			break
		}
		n = n / 2
		resp["rows"] = rowsRaw[:n]
		resp["returned_rows"] = n
		resp["truncated"] = n < totalRows
	}
	if b, err := json.Marshal(resp); err == nil {
		resp["response_bytes"] = len(b)
	}
	return resp, nil
}

func parseRESTKlineArray(raw json.RawMessage, symbol string, m binance.Market, klineType string, source string) ([]map[string]any, error) {
	qtyUnit := "base_asset"
	if klineType == "mark" || klineType == "index" || klineType == "premium" {
		qtyUnit = "price_only"
	} else if m == binance.MarketCOINM {
		qtyUnit = "contracts"
	}
	// Spot/USDM/COINM klines are [[openTime, open, high, low, close, volume, closeTime, quoteVolOrBaseVol, count, takerBuyBase, takerBuyQuote, ignore], ...]
	// Options klines may be object list [{"openTime":...}] OR arrays. Handle both!
	var arrRows [][]any
	if err := json.Unmarshal(raw, &arrRows); err == nil {
		out := make([]map[string]any, 0, len(arrRows))
		for _, r := range arrRows {
			if len(r) < 9 {
				continue
			}
			openMS, _ := toInt64Any(r[0])
			closeMS, _ := toInt64Any(r[6])
			cnt, _ := toInt64Any(r[8])
			row := map[string]any{
				"symbol":            symbol,
				"open_time_utc_ns":  strconv.FormatInt(openMS*1_000_000, 10),
				"close_time_utc_ns": strconv.FormatInt(closeMS*1_000_000, 10),
				"open":              formatScalar(r[1]),
				"high":              formatScalar(r[2]),
				"low":               formatScalar(r[3]),
				"close":             formatScalar(r[4]),
				"volume":            formatScalar(r[5]),
				"trade_count":       cnt,
				"qty_unit":          qtyUnit,
				"source":            source,
			}
			if qtyUnit != "price_only" {
				if m == binance.MarketCOINM {
					row["base_volume"] = formatScalar(r[7])
					if len(r) > 9 {
						row["taker_buy_volume"] = formatScalar(r[9])
					}
					if len(r) > 10 {
						row["taker_buy_base_volume"] = formatScalar(r[10])
					}
				} else {
					row["quote_volume"] = formatScalar(r[7])
					if len(r) > 9 {
						row["taker_buy_volume"] = formatScalar(r[9])
					}
				}
			}
			out = append(out, row)
		}
		return out, nil
	}

	var objRows []map[string]any
	if err := json.Unmarshal(raw, &objRows); err == nil {
		out := make([]map[string]any, 0, len(objRows))
		for _, item := range objRows {
			openMS, _ := toInt64Any(item["openTime"])
			closeMS, _ := toInt64Any(item["closeTime"])
			out = append(out, map[string]any{
				"symbol":            symbol,
				"open_time_utc_ns":  strconv.FormatInt(openMS*1_000_000, 10),
				"close_time_utc_ns": strconv.FormatInt(closeMS*1_000_000, 10),
				"open":              formatScalar(item["open"]),
				"high":              formatScalar(item["high"]),
				"low":               formatScalar(item["low"]),
				"close":             formatScalar(item["close"]),
				"volume":            formatScalar(item["volume"]),
				"amount":            formatScalar(item["amount"]),
				"trade_count":       item["tradeCount"],
				"qty_unit":          qtyUnit,
				"source":            source,
			})
		}
		return out, nil
	}
	return nil, fmt.Errorf("unexpected klines payload format")
}

func toInt64Any(v any) (int64, bool) {
	switch x := v.(type) {
	case float64:
		return int64(x), true
	case int64:
		return x, true
	case int:
		return int64(x), true
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func datesForNSWindow(startNS, endNS int64, maxDays int) (days []string, truncated bool, nextStartNS int64) {
	if startNS <= 0 {
		return nil, false, 0
	}
	if endNS <= startNS {
		endNS = startNS + int64(24*time.Hour)
	}
	if maxDays <= 0 {
		maxDays = 31
	}
	startDay := time.Unix(0, startNS).UTC().Truncate(24 * time.Hour)
	endDay := time.Unix(0, endNS-1).UTC().Truncate(24 * time.Hour)
	for d := startDay; !d.After(endDay); d = d.Add(24 * time.Hour) {
		if len(days) >= maxDays {
			return days, true, d.UnixNano()
		}
		days = append(days, d.Format("2006-01-02"))
	}
	return days, false, 0
}

func planSummaryMap(plan *recorder.QueryPlan) map[string]any {
	if plan == nil {
		return nil
	}
	partial := plan.PendingOmittedCount > 0 ||
		plan.PendingUnknownStreamCount > 0 ||
		plan.GapCount > 0 ||
		plan.QuarantinedCount > 0 ||
		plan.DiscontinuityCount > 0 ||
		len(plan.UnrecordedSymbols) > 0
	m := map[string]any{
		"revision_id":                  plan.RevisionID,
		"partitions_matched":           plan.PartitionsMatched,
		"ready_chunks_matched":         plan.ReadyChunksMatched,
		"already_cached_chunks":        plan.AlreadyCachedChunks,
		"download_chunks":              plan.DownloadChunks,
		"estimated_download_bytes":     plan.EstimatedDownloadBytes,
		"estimated_uncompressed_bytes": plan.EstimatedUncompressedBytes,
		"pending_omitted_count":        plan.PendingOmittedCount,
		"pending_unknown_stream_count": plan.PendingUnknownStreamCount,
		"gap_count":                    plan.GapCount,
		"quarantined_count":            plan.QuarantinedCount,
		"discontinuity_count":          plan.DiscontinuityCount,
		"partial_coverage":             partial,
	}
	if len(plan.RequestedSymbols) > 0 {
		m["requested_symbols"] = plan.RequestedSymbols
	}
	if len(plan.RecordedSymbols) > 0 {
		m["recorded_symbols"] = plan.RecordedSymbols
	}
	if len(plan.UnrecordedSymbols) > 0 {
		m["unrecorded_symbols"] = plan.UnrecordedSymbols
	}
	if len(plan.PendingReasons) > 0 {
		m["pending_reasons"] = plan.PendingReasons
	}
	return m
}

func buildRecorderCoverage(plan *recorder.QueryPlan, requestedSymbols []string, workerStats map[string]any, orderbookMode bool, unsyncReason string) map[string]any {
	if plan == nil {
		return map[string]any{
			"status":            "unknown",
			"requested_symbols": requestedSymbols,
			"disclosure":        "No Recorder query plan available.",
		}
	}
	var reasons []string
	if len(plan.UnrecordedSymbols) > 0 {
		reasons = append(reasons, "unrecorded_symbols")
	}
	if plan.GapCount > 0 {
		reasons = append(reasons, "gaps_present")
	}
	if plan.QuarantinedCount > 0 {
		reasons = append(reasons, "quarantined_chunks")
	}
	if plan.DiscontinuityCount > 0 {
		reasons = append(reasons, "discontinuity_events")
	}
	if plan.PendingOmittedCount > 0 {
		reasons = append(reasons, "pending_chunks_omitted")
	}
	if plan.PendingUnknownStreamCount > 0 {
		reasons = append(reasons, "pending_unknown_stream")
	}

	status := "complete"
	if plan.ReadyChunksMatched == 0 {
		status = "unavailable"
		reasons = append(reasons, "no_ready_chunks")
	}

	if orderbookMode && workerStats != nil {
		relEmissions := intFromAny(workerStats["reliable_emissions"])
		winResyncs := intFromAny(workerStats["window_resync_count"])
		gapFrames := intFromAny(workerStats["gap_count"])
		if gapFrames > 0 && plan.GapCount == 0 {
			reasons = append(reasons, "worker_gap_detected")
		}
		if winResyncs > 0 {
			reasons = append(reasons, "window_resync_occurred")
		}
		if relEmissions == 0 {
			status = "unavailable"
			if unsyncReason != "" {
				reasons = append(reasons, unsyncReason)
			} else {
				reasons = append(reasons, "no_reliable_orderbook_frames")
			}
		}
	}

	if status != "unavailable" && len(reasons) > 0 {
		status = "partial"
	}

	var notes []string
	if len(plan.UnrecordedSymbols) > 0 {
		notes = append(notes, fmt.Sprintf("Requested symbols not in Recorder history (%d omitted): %s (recorded: %s)", len(plan.UnrecordedSymbols), strings.Join(plan.UnrecordedSymbols, ","), strings.Join(plan.RecordedSymbols, ",")))
	}
	if plan.GapCount > 0 || plan.DiscontinuityCount > 0 {
		notes = append(notes, fmt.Sprintf("Recorder partition reported gaps=%d, discontinuities=%d", plan.GapCount, plan.DiscontinuityCount))
	}
	if plan.PendingUnknownStreamCount > 0 {
		notes = append(notes, fmt.Sprintf("%d pending chunks have stream=null and could not be cleanly excluded from stream coverage", plan.PendingUnknownStreamCount))
	}
	if unsyncReason != "" {
		notes = append(notes, "Orderbook reconstruction unsynchronized: "+unsyncReason)
	}

	out := map[string]any{
		"status":                       status,
		"requested_symbols":            requestedSymbols,
		"recorded_symbols":             plan.RecordedSymbols,
		"unrecorded_symbols":           plan.UnrecordedSymbols,
		"missing_symbols_count":        len(plan.UnrecordedSymbols),
		"gaps_count":                   plan.GapCount,
		"quarantined_count":            plan.QuarantinedCount,
		"discontinuity_count":          plan.DiscontinuityCount,
		"pending_omitted_count":        plan.PendingOmittedCount,
		"pending_unknown_stream_count": plan.PendingUnknownStreamCount,
	}
	if len(reasons) > 0 {
		out["partial_reasons"] = reasons
	}
	if len(notes) > 0 {
		out["disclosure"] = strings.Join(notes, "; ")
	}
	return out
}

func intFromAny(v any) int {
	n, ok := toInt64Any(v)
	if !ok {
		return 0
	}
	return int(n)
}
