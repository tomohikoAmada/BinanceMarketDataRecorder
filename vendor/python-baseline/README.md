# Frozen Python baseline

This directory is a vendored copy of the Python Recorder baseline from `main`
at commit `38acb1d0ee1ed7c5343024aead45d3a6a3f70a85`. It exists so `S-BMDR` can build, test, and package its
Python-backed verification/recovery/archive tools without pointing at another
branch or checkout.

Included paths:

- `src`
- `tests/fixtures/binance`
- `tests/golden`
- `pyproject.toml`
- `requirements`

Do not edit these files by hand. Refresh them from a clean `main` checkout with:

```bash
python3 scripts/vendor_python_baseline.py --source /path/to/main-checkout
```

`BASELINE.json` records the commit, included scope, and a deterministic tree
SHA-256. `prototype/verify.py` refuses to run if the vendored tree changes.
