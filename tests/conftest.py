"""Cross-platform pytest roots that don't violate persistent-data safeguards."""

from __future__ import annotations

import os
from pathlib import Path

import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Keep Linux tmp_path fixtures outside forbidden persistent /tmp."""

    if config.option.basetemp is None:
        config.option.basetemp = Path("/var/tmp") / f"bmdr-pytest-{os.getpid()}"
