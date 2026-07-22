# Dependency Policy

Status: M9 macOS external-volume discovery and registration.

## Principles

- Runtime dependencies must solve an immediate accepted milestone requirement.
- Development tools stay in the `dev` extra and are never imported at runtime.
- Stable project APIs use a tested lower bound and exclude the next major
  release. Dependencies whose exact implementation is part of an M2 capability
  decision are exact-pinned.
- Transitive dependencies are not imported as project APIs.
- Every dependency change requires tests, rationale here, and milestone scope.

## Runtime dependencies

| Dependency | Range | Immediate purpose |
| --- | --- | --- |
| `pydantic` | `>=2.10,<3` | Strict typed configuration, forbidden extra fields, validated serialization |
| `binance-sdk-spot` | `==10.0.0` | Official Spot public REST depth snapshots; its WebSocket portion is rejected by ADR-0009 |
| `binance-sdk-derivatives-trading-usds-futures` | `==14.0.0` | Official USD-M public REST depth snapshots; its WebSocket portion is rejected by ADR-0009 |
| `websockets` | `==15.0.1` | Selected WebSocket transport after M2 raw-payload, lifecycle, and backpressure probes |
| `cbor2` | `==6.1.3` | Deterministic canonical CBOR for Raw chunk headers and EventEnvelope bodies |
| `google-crc32c` | `==1.8.0` | Castagnoli per-header/per-frame integrity checks with bounded scan performance |
| `zstandard` | `==0.25.0` | Streaming immutable sealed Raw artifacts with checksum/readback verification |
| `pyobjc-framework-DiskArbitration` | `==12.2.1` on macOS | Bridge to Apple's Disk Arbitration callbacks and descriptions; includes matching PyObjC core/Cocoa bridge wheels |

Python's standard library supplies argparse CLI parsing, TOML reading,
structured JSON logging, paths, platform checks, package metadata, and Git
commit probing. No separate CLI framework or platform-directory library is
needed.

## M1 development dependencies

| Dependency | Range | Purpose |
| --- | --- | --- |
| `pytest` | `>=8.3,<10` | Offline deterministic tests |
| `ruff` | `>=0.9,<1` | Linting and import/style checks |
| `mypy` | `>=1.14,<2` | Strict static type checking |

The build backend is `setuptools>=75,<82`; it is isolated build tooling, not a
runtime dependency.

## M9 resolution policy

Pydantic retains a compatible major-version range. The generated official SDKs,
`websockets`, CBOR, CRC32C, and Zstandard packages are exact-pinned because
their concrete transport or byte-format behavior is certified by ADR-0008,
ADR-0009, and ADR-0010. The complete certified macOS 12+ arm64/Python 3.12
resolution and wheel hashes are recorded in
`requirements/macos-arm64-python312.lock`.

The Disk Arbitration bridge and its PyObjC core/Cocoa runtime are exact-pinned
to 12.2.1 as one tested bridge set. The dependency is macOS-conditional in
project metadata: non-macOS tooling can import platform-neutral storage models,
while attempts to use Disk Arbitration fail explicitly. M9 tested session
creation, startup enumeration and callback registration on the certified host.

The deprecated Futures connector, third-party `python-binance`, unverified
Binance MCPs, FastAPI, Qt, database, Parquet, dataframe, machine-learning,
archive, and storage-service dependencies remain absent.
