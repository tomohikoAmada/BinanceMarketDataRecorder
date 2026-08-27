"""Thin CLI wrapper for the installed reconnect-boundary audit engine."""

from __future__ import annotations

from binance_market_data_recorder.audit.reconnect_boundaries import (
    BLUE_GREEN_OVERLAP,
    EXPLICIT_SEQUENCE_GAP,
    UNKNOWN,
    UNMARKED_RECONNECT,
    audit_data_root,
    load_manifest_chunks,
    main,
    scan_chunk_frames,
)

__all__ = [
    "BLUE_GREEN_OVERLAP",
    "EXPLICIT_SEQUENCE_GAP",
    "UNKNOWN",
    "UNMARKED_RECONNECT",
    "audit_data_root",
    "load_manifest_chunks",
    "main",
    "scan_chunk_frames",
]


if __name__ == "__main__":
    raise SystemExit(main())
