# Dependency Policy

Status: M1 baseline.

## Principles

- Runtime dependencies must solve an immediate accepted milestone requirement.
- Development tools stay in the `dev` extra and are never imported at runtime.
- Direct dependencies use a tested lower bound and exclude the next major
  release. Lock files may be added for deployment/reproducibility in a later
  milestone when the installation workflow is selected; library metadata keeps
  compatible ranges so security and bug-fix releases are possible.
- Transitive dependencies are not imported as project APIs.
- Every dependency change requires tests, rationale here, and milestone scope.

## M1 runtime dependency

| Dependency | Range | Immediate purpose |
| --- | --- | --- |
| `pydantic` | `>=2.10,<3` | Strict typed configuration, forbidden extra fields, validated serialization |

Python's standard library supplies argparse CLI parsing, TOML reading,
structured JSON logging, paths, platform checks, package metadata, and Git
commit probing. M1 therefore does not add a CLI framework or platform-directory
library.

## M1 development dependencies

| Dependency | Range | Purpose |
| --- | --- | --- |
| `pytest` | `>=8.3,<10` | Offline deterministic tests |
| `ruff` | `>=0.9,<1` | Linting and import/style checks |
| `mypy` | `>=1.14,<2` | Strict static type checking |

The build backend is `setuptools>=75,<82`; it is isolated build tooling, not a
runtime dependency.

## Explicitly absent in M1

No Binance SDK, WebSocket client, FastAPI, Qt, database, Parquet, dataframe,
machine-learning, archive, or storage-service dependency is present. Binance
SDK/transport evaluation belongs to M2.
