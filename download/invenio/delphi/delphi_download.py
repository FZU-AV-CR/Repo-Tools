"""
Delphi (CERN particle-physics) adapter for the generic async/bulk download
pipeline. See fram_download.py for the full pattern explanation; this file
only differs in its constants below.

MODEL_NAME = "particles", NOT "delphi": async_upload.py's docstring notes
the original Delphi script passes model="particles" to
client.records.create(); the repository's registered model slug for this
metadata schema is "particles" even though this pipeline's adapter short
name (and upload/download filenames) are "delphi". Keep these straight --
using "delphi" as MODEL_NAME here would silently return zero records.

This is also the model main.py/stats.ipynb originally downloaded from the
CERN Open Data REST API (with the CERN-specific identifiers now stripped);
delphi_download.py via bulk_download.py is what replaces that flow for the
Physics repository.
"""

from __future__ import annotations

import os
from pathlib import Path

ADAPTER_NAME = "delphi"
MODEL_NAME = "particles"  # confirmed by async_upload.py's docstring; NOT "delphi"
DEFAULT_QUERY = None
DEFAULT_OUTPUT_DIR = "/home/erutherford/Python WSL/Delphi/Download/Data"

ENGINE_DIR = Path(__file__).resolve().parent.parent


def _run_via_bulk_download() -> None:
    import importlib
    import sys

    if str(ENGINE_DIR) not in sys.path:
        sys.path.insert(0, str(ENGINE_DIR))
    bulk_download = importlib.import_module("bulk_download")
    os.environ.setdefault("PHYSICS_DOWNLOAD_ADAPTER", ADAPTER_NAME)

    sys.argv = [sys.argv[0], "--adapter", ADAPTER_NAME] + sys.argv[1:]
    bulk_download.main()


if __name__ == "__main__":
    _run_via_bulk_download()


#   python3 delphi_download.py --environment test1 --dry-run --ids 3g7r6-bx383,saejn-6wb44
#   python3 delphi_download.py --environment test1 --filter metadata.experiment=Delphi --year 2025
#   python3 delphi_download.py --environment production --max-concurrency 8
#
# Note: unlike delphi_upload.py's --file-concurrency (per-record, since
# upload discovers work item-by-record), download items are already
# file-level (see async_download.py's module docstring) -- --max-concurrency
# alone controls how many files across all records download at once.
