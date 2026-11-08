"""Exact arithmetic and boundary/duplicate/gap behavior, independent of live input."""

from candles import aggregate


def trade(t, a, p, q):
    return (
        "spot",
        "BTCUSDT",
        {"T": t, "a": a, "p": p, "q": q, "f": a * 2, "l": a * 2 + 1},
        [],
    )


def main():
    events = [
        trade(0, 1, "1.00000001", "0.1"),
        trade(59999, 2, "2.00000002", "0.2"),
        trade(59999, 2, "2.00000002", "0.2"),
        trade(60000, 4, "0.5", "3"),
    ]
    rows = list(aggregate(events, 60000, 0, 120000))
    assert (
        len(rows) == 2
        and rows[0]["close_time_ms"] == 59999
        and rows[1]["open_time_ms"] == 60000
    )
    assert rows[0]["volume"] == "0.3" and rows[0]["quote_volume"] == "0.500000005"
    assert rows[0]["trade_count"] == 4 and rows[0]["agg_trades"] == 2
    assert rows[0]["open"] == "1.00000001" and rows[0]["close"] == "2.00000002"
    assert rows[1]["capture_gap"] and rows[1]["volume"] == "3"
    print("offline candle precision, boundary, duplicate, and gap: PASS")


if __name__ == "__main__":
    main()
