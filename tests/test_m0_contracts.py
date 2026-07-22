"""Pytest entry point for the dependency-free M0 verifier."""

from __future__ import annotations

import runpy
from pathlib import Path


def test_m0_contracts() -> None:
    namespace = runpy.run_path(str(Path(__file__).with_name("verify_m0_contracts.py")))
    namespace["verify"]()
