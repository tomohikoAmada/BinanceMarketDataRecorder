from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("mode", ["denied", "removed"])
def test_installed_console_version_from_unusable_cwd(tmp_path: Path, mode: str) -> None:
    entry = Path(sys.executable).parent / "binance-market-recorder"
    assert entry.is_file(), "test environment must install the Recorder console entry"
    directory = tmp_path / "cwd"
    directory.mkdir()
    script = """
import os, runpy, sys
directory, entry, mode = sys.argv[1:]
sys.path[0] = os.path.dirname(entry)  # Match Python's console-script import path.
os.chdir(directory)
if mode == 'removed':
    os.rmdir(directory)
else:
    os.chmod(directory, 0)
try:
    sys.argv = [entry, '--version']
    runpy.run_path(entry, run_name='__main__')
finally:
    if mode == 'denied':
        os.chmod(directory, 0o700)
"""
    # Resolve before entering the unusable cwd: imports must use the tested source.
    project = Path(__file__).resolve().parents[2]
    environment = {**os.environ, "PYTHONPATH": str(project / "src")}
    result = subprocess.run(
        [sys.executable, "-c", script, str(directory), str(entry), mode],
        env=environment, capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("binance-market-data-recorder ")
