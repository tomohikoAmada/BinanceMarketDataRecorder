# Dependency Policy

Status: M16 deterministic replay and generic consumer API.

## M19 dependency decision

No new runtime dependency was added. Current RSS uses macOS `libproc` and a
Linux `/proc` adapter point, so `psutil` was unnecessary. Historical
normalization reuses pinned `pyarrow==25.0.0`; HTTPS downloads use the standard
library. Official modular Spot and USD-M SDK pins remain unchanged.

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
| `pyarrow` | `==25.0.0` | Explicit-schema Parquet 2.6 writer/reader with Zstandard compression and logical readback verification |
| `pyobjc-framework-Cocoa` | `==12.2.1` on macOS | Direct AppKit/Foundation binding for official NSWorkspace sleep/wake notifications |
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
| `duckdb` | `==1.5.5` | Development-only independent Parquet/Hive-partition smoke query |

The build backend is `setuptools>=75,<82`; it is isolated build tooling, not a
runtime dependency.

## M15 resolution policy

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

M14 promotes the already locked Cocoa wheel to a direct macOS-conditional
dependency because Recorder now imports AppKit/Foundation as a project API for
NSWorkspace sleep/wake observation. It remains on the exact same 12.2.1 PyObjC
bridge set; no resolved wheel or lock hash changes. launchctl, plist generation,
flock, atomic state, signals, resource metrics, and scoped `/usr/bin/caffeinate`
use the standard library or macOS itself, so no service framework, process
manager, or power-management package is added.

M15 exact-pins PyArrow 25.0.0 because its concrete Parquet encoding, metadata,
compression, page-checksum and readback behavior form `bmdr-parquet.v1`.
Changing the writer requires compatibility review and a new profile/schema
version rather than an unconstrained upgrade. The certified macOS arm64 wheel
hash is in the runtime lock.

DuckDB 1.5.5 is exact-pinned in the `dev` extra only. It independently queries
published Parquet paths with Hive partitioning during acceptance and is never
imported by production Recorder code, used as Catalog, or used to store Raw.
No pandas/dataframe layer is needed.

M16 adds no runtime or development dependency. Replay reuses the exact-pinned
PyArrow reader, standard-library JSON/hash/tempfile/heap primitives, and the
published M15 manifests. The distribution includes a zero-byte `py.typed`
marker as package data so consumers can type-check the public replay API; this
is metadata, not another dependency.

The deprecated Futures connector, third-party `python-binance`, unverified
Binance MCPs, FastAPI, Qt, general-purpose database, dataframe,
machine-learning, archive, and storage-service dependencies remain absent.
