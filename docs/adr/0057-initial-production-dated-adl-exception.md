# ADR-0057: Explicit initial-production USD-M dated ADL omission

Status: owner accepted in the P4 chat, 2026-11-01; implementation candidate.

P4 preparation on the real GreenCloud host observed HTTP400/-1121 for both
`BTCUSDT_261225` and `BTCUSDT_270326` at public `GET /fapi/v1/symbolAdlRisk`.
A separate unsigned all-symbol response succeeded with800 rows, including the
five selected BTC perpetuals but neither dated product. Current official ADL
documentation and installed official SDK do not establish dated applicability.
These observations do not prove that Binance never offers dated ADL.

The owner explicitly approved an initial release without **USD-M dated-contract
public ADL risk**, with disclosure, and directed P4 to continue. This is additional
to ADR-0055's dated-premium exception. Perpetual ADL, other public information,
and the complete target remain unchanged. No account ADL endpoint is an alternative.

`coverage.capture_dated_adl_risk` defaults to true for compatibility. Setting it
false omits only D28 for `um_delivery` before scheduling, including newly discovered
dated products. The existing source-selection report retains each omitted source as
`OWNER_APPROVED_UNCAPTURED`, `official_applicability=UNKNOWN`, outside the prospective
enabled-source denominator. The full-target count still includes it. Never relabel
the omission `EXCHANGE_NOT_OFFERED` or rewrite earlier Raw, failures or denominators.

The P4 configuration freezes this false setting and the existing ADR-0055 exception
before a new timing window. P0–P3 acceptance is not reopened, and failed preparation
attempts earn no2h/24h credit. The [P4 result](../milestones/P4/README.md) retains
host evidence and tests. Official applicability remains an open follow-up in the
risk register; an official clarification is not a first-release acceptance gate
under this newly approved exception. No public message or issue is authorized here.
