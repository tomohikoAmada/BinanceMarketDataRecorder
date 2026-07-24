# DO NOT PUBLISH — Binance Spot local order-book bootstrap clarification

Target repository (manual decision only):
`binance/binance-spot-api-docs`

## Draft title

Clarify whether the first Spot diff after a REST snapshot must contain
`lastUpdateId` or `lastUpdateId + 1`

## Draft body

The current Global Spot “How to manage a local order book correctly” procedure
says to discard buffered events with `u <= lastUpdateId` and then says the
first remaining event should contain `lastUpdateId` in `[U,u]`.

The official Binance toolbox example at
`binance/binance-toolbox-python@51547845a9e3725b98e5a1bc55d4895c69ca0ca2`,
`manage_local_order_book.py`, instead accepts:

```python
U <= last_update_id + 1 <= u
```

In one credential-free BTCUSDT observation, a REST snapshot returned
`lastUpdateId=97799318619`; after covered events were discarded, the first
remaining diff had `U=97799318620`, `u=97799318630`. It contains
`lastUpdateId + 1`, but not `lastUpdateId`.

Could maintainers clarify the normative initial bridge?

1. Should the first applicable diff satisfy
   `U <= lastUpdateId + 1 <= u`?
2. If so, should the Global documentation's containment sentence be updated?
3. Is `u <= lastUpdateId` still the intended stale-event discard rule?

This report concerns only public Spot market data and includes no account,
credential, order, or trading operation.

## Local evidence

- Global Markdown retrieved: `2026-07-24T08:44:29.365584+00:00`
- Global Markdown SHA-256:
  `193aa07cd537b2ccc94662474fb3dda3cb774d550b1e117825919d99f91b725f`
- Toolbox GitHub page SHA-256:
  `d7617137b6f192bf5509d1182dbbc9daf7e85b72a4bf37f0229d59a031caac7c`
- Extracted toolbox source SHA-256:
  `993498520ae240ccae03bc54ad451091e8e0a10c8ef5ec9447f9876058cb9f61`

This file is a local draft. M17 explicitly forbids publication without a later
human decision.
