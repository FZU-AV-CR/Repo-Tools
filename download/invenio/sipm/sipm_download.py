"""
DUNE SiPM adapter for the generic async/bulk download pipeline. See
fram_download.py for the full pattern explanation; this file only differs
in its constants below.

MODEL_NAME: NOT yet confirmed. sipm_upload.py sets records up via a manual
"communities" dict (see async_upload.py's "COMMUNITY / WORKFLOW" docstring
section) rather than a model= kwarg, so there is no existing upload-side
reference value to copy the way delphi_download.py could. Confirm the real
model slug with `nrp-cmd repository read <alias>` (or inspect
`.well-known/repository` on the target environment) and update MODEL_NAME
below before relying on this adapter.
"""

from __future__ import annotations

import os
from pathlib import Path

ADAPTER_NAME = "sipm"
MODEL_NAME = "sipm"  # TODO confirm against a live .well-known/repository response
DEFAULT_QUERY = None
DEFAULT_OUTPUT_DIR = "/home/erutherford/Python WSL/SiPM/Download/Data"

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


#   python3 sipm_download.py --environment test1 --dry-run --ids <record-pid>
#   python3 sipm_download.py --environment test1 --filter metadata.experiment=SiPM
