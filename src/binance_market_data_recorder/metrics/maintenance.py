"""Single short-lived report process with its own read-only Catalog connection."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout
from .report import DailyReporter


def main() -> int:
    layout = StorageLayout.from_root(Path(sys.argv[1]))
    try:
        with Catalog.open_live_read_only(layout.catalog) as catalog:
            result = DailyReporter(
                catalog=catalog, daily_directory=layout.daily_reports,
            ).write_one_pending()
    except Exception as exc:
        print(json.dumps({
            "operation": "daily_report", "status": "FAILED",
            "error_type": type(exc).__name__, "error": str(exc),
        }), file=sys.stderr, flush=True)
        return 1
    print(json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
