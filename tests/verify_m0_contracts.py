"""Dependency-free M0 contract verifier, also collected by pytest.

M0 intentionally has no production package or dependency configuration. This
script keeps the acceptance gate runnable with plain Python 3.12; pytest will
also collect its single test when pytest is present.
"""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = (
    "AGENTS.md",
    "README.md",
    "docs/project_contract.md",
    "docs/architecture.md",
    "docs/milestone_plan.md",
    "docs/data_contract.md",
    "docs/storage_contract.md",
    "docs/macos_operations.md",
    "docs/binance_sources.md",
    "docs/risk_register.md",
    "docs/requirements_traceability.md",
    "docs/repository_audit.md",
    "docs/milestone_acceptance/M0.md",
    "docs/adr/0001-independent-recorder-repository.md",
    "docs/adr/0002-framed-raw-chunk-format.md",
    "docs/adr/0003-registered-directory-archive.md",
    "docs/adr/0004-clock-and-replay-semantics.md",
    "docs/adr/0005-binance-transport-evidence-gate.md",
)

REQUIRED_TRACE_IDS = (
    "WF-01",
    "WF-04",
    "BND-01",
    "SRC-01",
    "SRC-04",
    "DAT-01",
    "DAT-09",
    "STO-01",
    "STO-09",
    "SPC-01",
    "SPC-03",
    "MET-01",
    "MAC-01",
    "OPS-01",
    "OPS-04",
    "NRM-01",
    "CON-01",
    "FAI-01",
    "REL-02",
    "FUT-01",
)

FORBIDDEN_PRODUCTION_ENTRIES = (
    ROOT / "src",
    ROOT / "tools" / "update_binance_docs.py",
    ROOT / "configs",
)


def verify() -> None:
    missing = [path for path in REQUIRED_FILES if not (ROOT / path).is_file()]
    assert not missing, f"missing M0 files: {missing}"

    plan = (ROOT / "docs/milestone_plan.md").read_text(encoding="utf-8")
    for number in range(19):
        assert f"## M{number} " in plan, f"M{number} missing from milestone plan"
    for heading in ("Scope", "Non-scope", "Dependencies", "Acceptance", "Rollback"):
        assert plan.count(f"- {heading}:") == 19, f"expected 19 {heading} sections"

    trace = (ROOT / "docs/requirements_traceability.md").read_text(encoding="utf-8")
    for requirement_id in REQUIRED_TRACE_IDS:
        assert requirement_id in trace, f"traceability missing {requirement_id}"

    agents = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    for phrase in (
        "Never place trades",
        "Do not add deprecated `binance-futures-connector-python`",
        "Exactly one milestone",
        "Alpha101Crypto",
        "llms-full.txt",
    ):
        assert phrase in agents, f"AGENTS missing rule: {phrase}"

    raw_adr = (ROOT / "docs/adr/0002-framed-raw-chunk-format.md").read_text(
        encoding="utf-8"
    )
    for candidate in ("NDJSON", "MessagePack", "CBOR", "Zstandard", "CRC32C"):
        assert candidate in raw_adr, f"raw ADR missing candidate/decision {candidate}"

    for forbidden in FORBIDDEN_PRODUCTION_ENTRIES:
        assert not forbidden.exists(), f"M0 must not create future production entry: {forbidden}"

    tracked_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in ROOT.rglob("*.md")
        if ".git" not in path.parts
    )
    assert "python-binance" in tracked_text  # its prohibition must be explicit
    assert "No GUI" in tracked_text or "no GUI" in tracked_text


def test_m0_contracts() -> None:
    verify()


if __name__ == "__main__":
    verify()
    print("M0 contract verification passed")
