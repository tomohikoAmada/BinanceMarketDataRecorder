#!/usr/bin/env python3
"""Test-only OpenSSH argv/stream shim that invokes the real hidden CLI."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
import time


def main() -> int:
    if len(sys.argv) < 7:
        print("invalid ssh shim argv", file=sys.stderr)
        return 99
    if sys.argv[1:3] != ["-o", "BatchMode=yes"]:
        print("BatchMode missing", file=sys.stderr)
        return 98
    if sys.argv[3] != "-o" or not sys.argv[4].startswith("ConnectTimeout="):
        print("ConnectTimeout missing", file=sys.stderr)
        return 97
    command = shlex.split(sys.argv[-1])
    operation = command[command.index("_remote") + 1]
    stdin_bytes = sys.stdin.buffer.read()
    capture_path = os.environ.get("BMDR_SSH_SHIM_STDIN_CAPTURE")
    if capture_path and operation == "authorize":
        with open(capture_path, "wb") as capture:
            capture.write(stdin_bytes)
    mode = os.environ.get("BMDR_SSH_SHIM_FAULT", "")
    if mode == "delete_response_loss_pending" and operation == "delete":
        once_path = os.environ.get("BMDR_SSH_SHIM_ONCE_FILE")
        if once_path and not os.path.exists(once_path):
            with open(once_path, "xb"):
                pass
            return 255
    remote_environment = os.environ.copy()
    remote_environment.pop("BINANCE_MARKET_RECORDER_DATA_ROOT", None)
    if mode == "catalog_snapshot_kill" and operation == "catalog-snapshot":
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=remote_environment,
        )
        stage_root = os.environ.get("BMDR_SSH_SHIM_REMOTE_STAGE_ROOT")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and process.poll() is None:
            if stage_root and os.path.isdir(stage_root) and os.listdir(stage_root):
                process.kill()
                break
            time.sleep(0.001)
        stdout, stderr = process.communicate(input=stdin_bytes)
        sys.stdout.buffer.write(stdout)
        sys.stdout.buffer.flush()
        sys.stderr.buffer.write(stderr)
        return process.returncode
    completed = subprocess.run(
        command,
        input=stdin_bytes,
        capture_output=True,
        check=False,
        env=remote_environment,
    )
    if operation == "raw":
        body = completed.stdout
        if mode == "raw_partial":
            body = body[: max(1, len(body) // 2)]
            completed = subprocess.CompletedProcess(command, 55, body, completed.stderr)
        elif mode == "raw_extra":
            body += b"x"
        elif mode == "raw_modified" and body:
            body = bytes([body[0] ^ 1]) + body[1:]
        elif mode == "raw_full_nonzero":
            completed = subprocess.CompletedProcess(command, 56, body, completed.stderr)
        elif mode == "raw_nonzero_before":
            completed = subprocess.CompletedProcess(command, 57, b"", completed.stderr)
        elif mode == "raw_stderr_pressure":
            sys.stderr.buffer.write(b"e" * (2 * 1024 * 1024))
            sys.stderr.buffer.flush()
        sys.stdout.buffer.write(body)
        sys.stdout.buffer.flush()
        sys.stderr.buffer.write(completed.stderr)
        return completed.returncode
    if operation == "catalog-snapshot":
        body = completed.stdout
        if mode == "catalog_snapshot_partial":
            body = body[: max(1, len(body) // 2)]
        elif mode == "catalog_snapshot_full_nonzero":
            completed = subprocess.CompletedProcess(command, 58, body, completed.stderr)
        elif mode == "catalog_snapshot_stderr_pressure":
            sys.stderr.buffer.write(b"s" * (2 * 1024 * 1024))
            sys.stderr.buffer.flush()
        sys.stdout.buffer.write(body)
        sys.stdout.buffer.flush()
        sys.stderr.buffer.write(completed.stderr)
        return completed.returncode
    if (mode == "authorize_response_loss" and operation == "authorize") or (
        mode == "delete_response_loss" and operation == "delete"
    ):
        sys.stderr.buffer.write(completed.stderr)
        return 255 if completed.returncode == 0 else completed.returncode
    sys.stdout.buffer.write(completed.stdout)
    sys.stdout.buffer.flush()
    sys.stderr.buffer.write(completed.stderr)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
