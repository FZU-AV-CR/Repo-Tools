"""
FRAM (robotic telescope network) adapter for the download pipeline.

Sets the FRAM-specific constants (model name, default date field) and
serves as the CLI entry point: run `python3 fram_download.py [flags]`
rather than bulk_download.py directly. Everything else -- record
selection, concurrency, resume, retries -- is handled by the shared
engine (bulk_download.py / async_download.py).

API token: never stored here. Resolved from --token, the INVENIO_TOKEN
environment variable, or an interactive prompt, in that order.
"""

from __future__ import annotations

import os
from pathlib import Path

# Must match this adapter's key in download_adapters.ADAPTERS.
ADAPTER_NAME = "fram"

# Confirmed to match the FRAM community slug.
MODEL_NAME = "fram"

# No default query -- selection is always bounded at run time via
# --ids/--ids-file/--query/--filter/--filter-range/...
DEFAULT_QUERY = None

# FRAM uses observation_night (an integer YYYYMMDD, e.g. 20210221) as the
# canonical night identifier -- matching astronomical convention and the
# web UI. A night starting on Feb 21 carries observation_night=20210221
# even if the exposure timestamp (observation_time) rolls into Feb 22 UTC.
# Use --filter-range metadata.observation_night=YYYYMMDD:YYYYMMDD to
# select by night; --year is not meaningful for this integer field.
DEFAULT_DATE_FIELD = "metadata.observation_night"

# No default -- always pass --output-dir on the command line.
DEFAULT_OUTPUT_DIR = None

# Assumed layout: the shared engine lives one directory up from this file.
#   Download/
#     async_download.py
#     bulk_download.py
#     download_adapters.py
#     FRAM/fram_download.py   <- this file
# Adjust ENGINE_DIR if the layout differs.
ENGINE_DIR = Path(__file__).resolve().parent.parent


def _run_via_bulk_download() -> None:
    import importlib
    import sys

    if str(ENGINE_DIR) not in sys.path:
        sys.path.insert(0, str(ENGINE_DIR))

    # importlib rather than a bare `import bulk_download` so that editors
    # cannot rewrite it to a dotted path that breaks sys.path resolution.
    bulk_download = importlib.import_module("bulk_download")

    os.environ.setdefault("PHYSICS_DOWNLOAD_ADAPTER", ADAPTER_NAME)
    sys.argv = [sys.argv[0], "--adapter", ADAPTER_NAME] + sys.argv[1:]
    bulk_download.main()


if __name__ == "__main__":
    _run_via_bulk_download()

# Usage examples (--output-dir is always required):
#
#   # All object frames from cta-n on the night of 2021-02-21, altitude >= 20 deg
#   python3 fram_download.py --environment test1 --output-dir "/home/xyx/data/fram" \
#     --filter metadata.site=cta-n \
#     --filter metadata.type=object \
#     --filter-range metadata.observation_night=20210220:20210221 \
#     --filter-range metadata.alt_az.altitude=20:90 \
#     --dry-run
#
#   # Explicit record IDs
#   python3 fram_download.py --environment test1 --output-dir /data/fram --ids 3g7r6-bx383,saejn-6wb44
#
#   # Production run with higher concurrency
#   python3 fram_download.py --environment production --output-dir /data/fram \
#     --filter metadata.site=cta-n \
#     --filter-range metadata.observation_night=20210221:20210221 \
#     --max-concurrency 4
