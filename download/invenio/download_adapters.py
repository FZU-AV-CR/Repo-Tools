"""
Registry of metadata-model adapters for the download engine.

Each adapter is a module (fram_download.py, delphi_download.py, ...) that
sets three constants:

    MODEL_NAME         str        nrp-cmd `model` search parameter, used to
                                  pick the right search/scan URL from the
                                  repository's .well-known/repository endpoint.
    DEFAULT_OUTPUT_DIR str|None   where files land if --output-dir is not given.
    DEFAULT_QUERY      str|None   optional default OpenSearch filter, ANDed with
                                  any --query/--filter/--year/... the user adds.

The active adapter is selected via --adapter (bulk_download.py's CLI flag)
or the PHYSICS_DOWNLOAD_ADAPTER environment variable. Each model's entry-
point script sets the latter before calling bulk_download.main(), so
`python3 fram_download.py ...` just works.

To add a new metadata model:
  1. Create <model>_download.py setting the three constants above, plus the
     same _run_via_bulk_download() boilerplate as fram_download.py.
  2. Add one line to ADAPTERS below.
No changes to async_download.py or bulk_download.py are needed.
"""

from __future__ import annotations

import importlib
import os
from types import ModuleType

# short name -> importable module name (the module's *file*, not a class)
ADAPTERS: dict[str, str] = {
    "fram": "fram_download",
    "delphi": "delphi_download",
    "sipm": "sipm_download",
    "itk": "itk_download",
}

ENV_VAR = "PHYSICS_DOWNLOAD_ADAPTER"


def available() -> list[str]:
    """Short names of every registered adapter, for --help / error text."""
    return sorted(ADAPTERS)


def resolve_name(explicit: str | None = None) -> str:
    """Resolve which adapter to use: explicit arg -> PHYSICS_DOWNLOAD_ADAPTER
    env var -> raise. No silent fallback: downloading the wrong model's
    records to the wrong folder is a worse failure mode than a clear error.
    """
    name = explicit or os.environ.get(ENV_VAR)
    if not name:
        raise RuntimeError(
            f"No adapter selected. Pass --adapter (one of {available()}) or set "
            f"the {ENV_VAR} environment variable, e.g. {ENV_VAR}=fram."
        )
    if name not in ADAPTERS:
        raise ValueError(f"Unknown adapter {name!r}; expected one of {available()}")
    return name


def load(explicit: str | None = None) -> ModuleType:
    """Resolve + import the adapter module for the given name (or the
    PHYSICS_DOWNLOAD_ADAPTER env var if name is None)."""
    name = resolve_name(explicit)
    return importlib.import_module(ADAPTERS[name])
