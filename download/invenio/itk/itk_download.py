"""
ATLAS ITk adapter for the generic async/bulk download pipeline. See
fram_download.py for the full pattern explanation; this file only differs
in its constants below.

MODEL_NAME: NOT yet confirmed -- same caveat as sipm_download.py. Confirm
the real model slug with `nrp-cmd repository read <alias>` (or inspect
`.well-known/repository` on the target environment) and update MODEL_NAME
below before relying on this adapter.
"""

from __future__ import annotations

import os
from pathlib import Path

ADAPTER_NAME = "itk"
MODEL_NAME = "itk"  # TODO confirm against a live .well-known/repository response
DEFAULT_QUERY = None
DEFAULT_OUTPUT_DIR = "/home/erutherford/Python WSL/ITk/Download/Data"

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


#   python3 itk_download.py --environment test1 --dry-run --ids <record-pid>
#   python3 itk_download.py --environment test1 --filter metadata.experiment=ITk
