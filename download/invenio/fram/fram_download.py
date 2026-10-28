"""
FRAM (robotic telescope network) adapter for the generic async/bulk
download pipeline (async_download.py / bulk_download.py).

Companion to fram_upload.py. Since downloading needs none of the
metadata-extraction/validation/building logic an upload adapter provides
(see async_download.py's module docstring), this file is intentionally
just the handful of constants bulk_download.py needs plus the same
CLI-entry-point boilerplate as fram_upload.py.

Which adapter is active is resolved at runtime by download_adapters.py,
not hardcoded in async_download.py/bulk_download.py: this file passes
--adapter fram to bulk_download.py's CLI (and sets the
PHYSICS_DOWNLOAD_ADAPTER env var as a fallback), so `python3
fram_download.py ...` just works, including side-by-side with `python3
sipm_download.py ...` / `python3 itk_download.py ...` in separate
processes.

The API token is not stored in this file. bulk_download.py /
async_download.py resolve it from the --token flag, the INVENIO_TOKEN
environment variable, or an interactive prompt, in that order.
"""

from __future__ import annotations

import os
from pathlib import Path

# Must match this adapter's key in download_adapters.ADAPTERS.
ADAPTER_NAME = "fram"

# Repository model slug -- confirmed to match the community slug used by
# fram_upload.py (FRAM_COMMUNITY = "fram").
MODEL_NAME = "fram"

# No default query: every FRAM record's FITS header is its own metadata,
# so an empty selection means "the whole community" (bounded at run time
# via --ids/--ids-file/--query/--filter/--year/...).
DEFAULT_QUERY = None

# Filter on the real observation date (from the FITS DATE-OBS header),
# not the repository ingestion timestamp. Records are typically uploaded
# well after the fact, so --year against the ingestion date would match
# nothing for a given observation year.
# If a --year/--created-from/--created-to run returns zero results,
# confirm the indexed field path against a live search response and try
# --date-field metadata.dates.date or --date-field created as fallbacks.
DEFAULT_DATE_FIELD = "metadata.observation_time"

DEFAULT_OUTPUT_DIR = "/mnt/data/fram"


# ============================================================
# CLI ENTRY POINT
#
# bulk_download.py itself stays fully generic and errors out if run
# directly -- this file is the actual entry point, same pattern as
# fram_upload.py.
#
# Assumed layout: async_download.py / bulk_download.py /
# download_adapters.py (the shared engine) live one directory up:
#     Download/async_download.py
#     Download/bulk_download.py
#     Download/download_adapters.py
#     Download/FRAM/fram_download.py   <- this file
# If the layout differs, adjust ENGINE_DIR below.
# ============================================================

ENGINE_DIR = Path(__file__).resolve().parent.parent


def _run_via_bulk_download() -> None:
    import importlib
    import sys

    # async_download.py / bulk_download.py aren't in this file's own
    # directory, so Python won't find them via the default sys.path.
    # Add the shared engine's directory explicitly before importing.
    if str(ENGINE_DIR) not in sys.path:
        sys.path.insert(0, str(ENGINE_DIR))

    # Loaded via importlib.import_module() rather than a literal import
    # statement to avoid editors rewriting unresolved imports into a
    # fully-qualified dotted path that breaks this sys.path-based
    # resolution (same reasoning as fram_upload.py).
    bulk_download = importlib.import_module("bulk_download")

    os.environ.setdefault("PHYSICS_DOWNLOAD_ADAPTER", ADAPTER_NAME)

    default_flags = {"--adapter": ADAPTER_NAME}
    injected = []
    for flag, value in default_flags.items():
        injected += [flag, value]

    sys.argv = [sys.argv[0]] + injected + sys.argv[1:]
    bulk_download.main()


if __name__ == "__main__":
    _run_via_bulk_download()

# Usage examples:
#
#   python3 fram_download.py --environment test1 --dry-run
#   python3 fram_download.py --environment test1 --ids 3g7r6-bx383,saejn-6wb44
#   python3 fram_download.py --environment test1 --filter metadata.site=LaPalma --year 2025
#   python3 fram_download.py --environment production --max-concurrency 4 --output-dir /mnt/data/fram
