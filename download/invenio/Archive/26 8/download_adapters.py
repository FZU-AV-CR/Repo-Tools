"""
Registry of metadata-model "adapters" for the shared async_download.py /
bulk_download.py download engine.

Much thinner than upload's adapters.py, because downloading doesn't need
per-model metadata extraction/validation/building -- see async_download.py's
module docstring for why. All a download adapter provides is:

    MODEL_NAME          -> str   # nrp-cmd `model` search param for this
                                  # metadata model (passed to
                                  # published_records.search()/scan()/read())
    DEFAULT_OUTPUT_DIR  -> str   # where files land if --output-dir isn't given
    DEFAULT_QUERY       -> str | None   # optional default OpenSearch filter,
                                  # ANDed with anything the user passes via
                                  # --query/--filter/--year/... on top

Selecting the adapter at runtime works exactly like upload's:
  - via the --adapter CLI flag (see bulk_download.py), or
  - via the PHYSICS_DOWNLOAD_ADAPTER environment variable, which each
    model's own entry-point script (fram_download.py, delphi_download.py,
    sipm_download.py, itk_download.py) sets for itself before handing off
    to bulk_download.py.

Adding a new metadata model:
  1. Write <model>_download.py setting MODEL_NAME / DEFAULT_OUTPUT_DIR /
     DEFAULT_QUERY, plus the same _run_via_bulk_download() CLI-entry-point
     boilerplate as fram_download.py.
  2. Add one line to ADAPTERS below mapping a short name to the module name.
That's it -- async_download.py and bulk_download.py need no further changes.

NOTE on MODEL_NAME vs. upload's community/model kwargs: async_upload.py's
adapters mostly identify their target via a *community* slug (e.g. FRAM's
"community": "fram") rather than nrp-cmd's `model` search parameter, and
the one adapter that does set upload's model= kwarg (Delphi) uses
model="particles", not "delphi". Repository "model" (used to pick the
right search/scan URL out of the well-known endpoint's info.models dict)
and community slug are related but not always identical strings -- the
MODEL_NAME values below are this pipeline's best-confirmed guess per model;
each one is flagged where it hasn't been verified against a live
`.well-known/repository` response yet.
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
    """Figure out which adapter to use: explicit arg -> PHYSICS_DOWNLOAD_ADAPTER
    env var -> raise. Deliberately no silent fallback -- downloading the
    wrong model's records to the wrong folder is a worse failure mode than
    an explicit, immediate error.
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
