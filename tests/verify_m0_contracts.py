"""Dependency-free M0/M0.1 contract verifier, also collected by pytest.

M0/M0.1 intentionally have no production package or dependency configuration.
This script keeps the acceptance gate runnable with plain Python 3.12; pytest
will also collect its single test when pytest is present.
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
    "docs/milestone_acceptance/M0.1.md",
    "docs/adr/0001-independent-recorder-repository.md",
    "docs/adr/0002-framed-raw-chunk-format.md",
    "docs/adr/0003-registered-directory-archive.md",
    "docs/adr/0004-clock-and-replay-semantics.md",
    "docs/adr/0005-binance-transport-evidence-gate.md",
    "docs/adr/ADR-0006-project-identity-and-workspace.md",
)

REQUIRED_TRACE_IDS = (
    "WF-01",
    "WF-04",
    "IDN-01",
    "IDN-02",
    "BND-01",
    "BND-03",
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

EXPECTED_ROOT = Path(
    "/Users/amada/Documents/Development/Crypto/CryptoMarketDataRecorder"
)

IDENTITY_VALUES = (
    "Crypto Market Data Recorder",
    "CryptoMarketDataRecorder",
    "crypto-market-data-recorder",
    "crypto_market_data_recorder",
    "crypto-market-recorder",
    "~/Library/Application Support/CryptoMarketDataRecorder/",
)

LEGACY_RECORDER_IDENTITIES = (
    "Alpha101Crypto" + "Recorder",
    "Alpha101 Crypto " + "Recorder",
    "alpha101crypto_" + "recorder",
    "alpha101crypto-" + "recorder",
    "/Users/amada/Documents/Development/Alpha101/" + "Alpha101Crypto" + "Recorder",
)

LEGACY_HISTORY_ALLOWLIST = {
    "docs/adr/ADR-0006-project-identity-and-workspace.md",
    "docs/milestone_acceptance/M0.1.md",
}

ALPHA_REFERENCE_ALLOWLIST = LEGACY_HISTORY_ALLOWLIST | {
    "AGENTS.md",
    "docs/adr/0001-independent-recorder-repository.md",
    "docs/milestone_acceptance/M0.md",
    "docs/milestone_plan.md",
    "docs/project_contract.md",
    "docs/repository_audit.md",
    "docs/requirements_traceability.md",
    "docs/risk_register.md",
    "tests/verify_m0_contracts.py",
}


def verify() -> None:
    assert ROOT == EXPECTED_ROOT, f"unexpected M0.1 workspace: {ROOT}"

    missing = [path for path in REQUIRED_FILES if not (ROOT / path).is_file()]
    assert not missing, f"missing M0 files: {missing}"

    plan = (ROOT / "docs/milestone_plan.md").read_text(encoding="utf-8")
    for number in range(19):
        assert f"## M{number} " in plan, f"M{number} missing from milestone plan"
    assert "## M0.1 " in plan, "M0.1 missing from milestone plan"
    for heading in ("Scope", "Non-scope", "Dependencies", "Acceptance", "Rollback"):
        assert plan.count(f"- {heading}:") == 20, f"expected 20 {heading} sections"
    assert "## M16 — Replay interface and generic consumer data contract" in plan
    assert "named consumer is" in plan and "required for V1 completion" in plan

    trace = (ROOT / "docs/requirements_traceability.md").read_text(encoding="utf-8")
    for requirement_id in REQUIRED_TRACE_IDS:
        assert requirement_id in trace, f"traceability missing {requirement_id}"

    agents = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    for phrase in (
        "Never place trades",
        "Do not add deprecated `binance-futures-connector-python`",
        "Exactly one milestone",
        "arbitrary research, backtest, and monitoring consumers",
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

    text_files = {
        path.relative_to(ROOT).as_posix(): path.read_text(encoding="utf-8")
        for path in ROOT.rglob("*")
        if path.is_file()
        and ".git" not in path.parts
        and ".pytest_cache" not in path.parts
        and path.suffix in {".md", ".py"}
    }
    tracked_text = "\n".join(text_files.values())

    identity_contract = "\n".join(
        text_files[name]
        for name in (
            "AGENTS.md",
            "README.md",
            "docs/adr/ADR-0006-project-identity-and-workspace.md",
        )
    )
    for identity in IDENTITY_VALUES:
        assert identity in identity_contract, f"missing frozen identity: {identity}"

    for name, content in text_files.items():
        if name not in LEGACY_HISTORY_ALLOWLIST:
            for legacy in LEGACY_RECORDER_IDENTITIES:
                assert legacy not in content, f"legacy identity {legacy!r} in current file {name}"
        if name not in ALPHA_REFERENCE_ALLOWLIST:
            assert "Alpha101" not in content, f"unclassified Alpha reference in {name}"

    assert "python-binance" in tracked_text  # its prohibition must be explicit
    assert "No GUI" in tracked_text or "no GUI" in tracked_text
    assert ("`" + "recorder ") not in tracked_text


def test_m0_contracts() -> None:
    verify()


if __name__ == "__main__":
    verify()
    print("M0 contract verification passed")
