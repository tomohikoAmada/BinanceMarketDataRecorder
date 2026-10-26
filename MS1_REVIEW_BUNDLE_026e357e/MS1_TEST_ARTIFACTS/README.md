# MS1 migration/test artifacts

The dedicated candidate test artifact is copied below at the same repository
relative path:

`tests/integration/test_ms1_multisymbol_durable_identity.py`

It contains its own SQLite seed helpers; there are no separate fixture files
for the legacy, malformed, partial, or cross-symbol cases. The test covers:

- legacy discontinuity and side-data cursor migration to `BTCUSDT`;
- idempotent reopen and preservation checks;
- malformed and partial migration fail-closed behavior;
- Spot/USD-M cross-symbol discontinuity isolation; and
- `(kind, symbol)` cursor isolation and explicit-symbol API enforcement.

The complete candidate patch contains all other source and test changes. The
copied test below is taken from the candidate tree, not from the dirty working
tree.
