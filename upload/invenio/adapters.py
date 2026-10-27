"""
Registry of metadata-model adapters for the shared async_upload.py /
bulk_async.py upload engine.

The engine (async_upload.py + bulk_async.py) is generic across every
Physics-repository metadata model; only the adapter differs. Before this
file existed, swapping models meant hand-editing an `import itk_upload as
adapter` line in both async_upload.py and bulk_async.py -- workable for
one model at a time, but awkward once more than one is in active use.

This registry lets the adapter be chosen at *runtime* instead:
  - via the --adapter CLI flag (see bulk_async.py), or
  - via the PHYSICS_ADAPTER environment variable, which each model's own
    entry-point script (itk_upload.py, sipm_upload.py, fram_upload.py,
    delphi_upload.py, ...) sets for itself before handing off to
    bulk_async.py -- so `python3 sipm_upload.py ...` and
    `python3 itk_upload.py ...` both work unmodified, and can even run
    concurrently in separate processes without stepping on each other.

Adding a new metadata model:
  1. Write <model>_upload.py implementing the adapter interface documented
     at the top of async_upload.py:
         discover_items(metadata_dir, data_root, readme_file=None) -> list[item]
             (each item needs a unique, stable string `.key` attribute)
         extract_metadata(item)             -> dict                (sync)
         validate_metadata(extracted)       -> list[str]           (sync)
         build_invenio_metadata(extracted)  -> dict  (with "metadata",
                                                "files", "access" keys,
                                                and optionally
                                                "communities", "community",
                                                "workflow" -- see
                                                async_upload.py's
                                                "COMMUNITY / WORKFLOW"
                                                docstring section)
         get_upload_files(item, extracted)  -> list[(file_key, Path, description)]
     plus the constants DEFAULT_SCHEMA_URL, DEFAULT_METADATA_DIR,
     DEFAULT_DATA_ROOT, DEFAULT_README_FILE, and the same
     _run_via_bulk_async() / ENGINE_DIR CLI-entry-point boilerplate as
     itk_upload.py / sipm_upload.py.
  2. Add one line to ADAPTERS below mapping a short name to the module
     name.
That's it -- async_upload.py and bulk_async.py need no further changes.
"""

from __future__ import annotations

import importlib
import os
from types import ModuleType

# short name -> importable module name (the module's *file*, not a class)
ADAPTERS: dict[str, str] = {
    "itk": "itk_upload",
    "sipm": "sipm_upload",
    "fram": "fram_upload",
    "delphi": "delphi_upload",
}

ENV_VAR = "PHYSICS_ADAPTER"


def available() -> list[str]:
    """Short names of every registered adapter, for --help / error text."""
    return sorted(ADAPTERS)


def resolve_name(explicit: str | None = None) -> str:
    """Figure out which adapter to use: explicit arg -> PHYSICS_ADAPTER env
    var -> raise. Deliberately no silent fallback to e.g. "itk" -- running
    the wrong model's adapter against the wrong data would be a much worse
    failure mode than an explicit, immediate error.
    """
    name = explicit or os.environ.get(ENV_VAR)
    if not name:
        raise RuntimeError(
            f"No adapter selected. Pass --adapter (one of {available()}) or set "
            f"the {ENV_VAR} environment variable, e.g. {ENV_VAR}=sipm."
        )
    if name not in ADAPTERS:
        raise ValueError(f"Unknown adapter {name!r}; expected one of {available()}")
    return name


def load(explicit: str | None = None) -> ModuleType:
    """Resolve + import the adapter module for the given name (or the
    PHYSICS_ADAPTER env var if name is None). Raises the same errors as
    resolve_name() for an unset/unknown name; import errors from the
    adapter module itself propagate unchanged.
    """
    name = resolve_name(explicit)
    return importlib.import_module(ADAPTERS[name])
