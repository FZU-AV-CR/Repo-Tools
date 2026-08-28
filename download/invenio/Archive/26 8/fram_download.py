"""
FRAM (robotic telescope network) adapter for the generic async/bulk
download pipeline (async_download.py / bulk_download.py).

Companion to fram_upload.py's adapter. Since downloading needs none of the
metadata-extraction/validation/building logic an upload adapter provides
(see async_download.py's module docstring), this file is intentionally
just the handful of constants bulk_download.py needs plus the same
CLI-entry-point boilerplate as fram_upload.py -- there is no FITS-specific
download logic to write.

MODEL_NAME: best-confirmed guess, NOT yet verified against a live
`.well-known/repository` response. fram_upload.py sets community="fram"
(FRAM_COMMUNITY) when *creating* records, which is a different concept
from the `model` search parameter used to pick a search/scan URL out of
that endpoint's info.models dict (see download_adapters.py's module
docstring). Confirm the real model slug with:
    nrp-cmd repository read physica-test1
or by inspecting https://test1.physics.du.cesnet.cz/.well-known/repository
and update MODEL_NAME below if it differs from "fram".

Which adapter is active is resolved at runtime by download_adapters.py,
not hardcoded in async_download.py/bulk_download.py: this file passes
--adapter fram to bulk_download.py's CLI (and sets the
PHYSICS_DOWNLOAD_ADAPTER env var as a fallback) so `python3
fram_download.py ...` just works, including side-by-side with `python3
sipm_download.py ...` / `python3 itk_download.py ...` in separate
processes.

Token handling mirrors fram_upload.py: nothing is hardcoded here.
bulk_download.py / async_download.py resolve it the same way
(--token flag -> INVENIO_TOKEN env var -> interactive prompt).
"""

from __future__ import annotations

import os
from pathlib import Path

# Must match this adapter's key in download_adapters.ADAPTERS.
ADAPTER_NAME = "fram"

# TODO confirm against a live .well-known/repository response -- see
# module docstring above. fram_upload.py's confirmed community slug is
# "fram" (FRAM_COMMUNITY); using the same string here as a first guess.
MODEL_NAME = "fram"

# Every FRAM record's own FITS header *is* its metadata (see
# fram_upload.py) -- there's no natural extra filter to default to, so
# an empty selection means "the whole community" (bounded at run time via
# --ids/--ids-file/--query/--filter/--year/...).
DEFAULT_QUERY = None

DEFAULT_OUTPUT_DIR = "/home/erutherford/Python WSL/download/invenio/fram/Download/Data"


# ============================================================
# CLI ENTRY POINT
#
# bulk_download.py itself stays fully generic and errors out if run
# directly (see its __main__ guard) -- this file is the actual entry
# point, same pattern as fram_upload.py.
#
# Assumed layout: async_download.py / bulk_download.py / download_adapters.py
# (the shared engine) live one directory up from this adapter, e.g.:
#     Download/async_download.py
#     Download/bulk_download.py
#     Download/download_adapters.py
#     Download/FRAM/fram_download.py   <- this file
# If that's not where they end up, adjust ENGINE_DIR below.
#
#   python3 fram_download.py --environment local
#   python3 fram_download.py --environment test1 --dry-run
#   python3 fram_download.py --environment test1 --ids 3g7r6-bx383,saejn-6wb44
#   python3 fram_download.py --environment test1 --filter metadata.site=LaPalma --year 2025
#   python3 fram_download.py --environment production --max-concurrency 4 --output-dir "/mnt/data/fram"
# ============================================================

ENGINE_DIR = Path(__file__).resolve().parent.parent


def _run_via_bulk_download() -> None:
    import importlib
    import sys

    # async_download.py / bulk_download.py aren't in this file's own
    # directory, so Python won't find them via the default sys.path
    # (which only includes the directory of whatever script was actually
    # run). Add the shared engine's directory explicitly, before importing it.
    if str(ENGINE_DIR) not in sys.path:
        sys.path.insert(0, str(ENGINE_DIR))

    # Loaded via importlib.import_module(), not a literal `import
    # bulk_download` statement, on purpose -- see fram_upload.py's
    # identical comment: some editors' auto-import rewrites unresolved
    # top-level imports into a fully-qualified dotted path guessed from
    # the workspace layout, which breaks this sys.path-based resolution.
    bulk_download = importlib.import_module("bulk_download")

    # Fallback for any code path that reads the env var instead of the
    # CLI flag; the --adapter flag injected below takes precedence for
    # bulk_download.py itself since it's an explicit CLI arg.
    os.environ.setdefault("PHYSICS_DOWNLOAD_ADAPTER", ADAPTER_NAME)

    default_flags = {"--adapter": ADAPTER_NAME}
    injected = []
    for flag, value in default_flags.items():
        injected += [flag, value]

    sys.argv = [sys.argv[0]] + injected + sys.argv[1:]
    bulk_download.main()


if __name__ == "__main__":
    _run_via_bulk_download()


#   cd download/invenio/fram
#   python3 fram_download.py --environment test1 --dry-run --ids 81t0n-1ns76
#   python3 fram_download.py --environment test1 --filter metadata.site=LaPalma --year 2025
#   python3 fram_download.py --environment production --max-concurrency 4 --output-dir "/mnt/data/fram"
