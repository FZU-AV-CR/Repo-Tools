"""
ATLAS ITk (silicon-sensor test data) adapter for the generic async/bulk
upload pipeline (async_upload.py / bulk_async.py).

This replaces the old three-step ITk_upload2/3/4 flow:
  - ITk_upload2_create_metadata.py parsed *.txt files into per-record *.json
    metadata files on disk.
  - ITk_upload3_pyfile_generator.py read those JSON files and generated one
    standalone upload_itk_<title>.py script per record from a string
    template.
  - ITk_upload4_py_runner.py ran each generated script as a subprocess,
    sequentially, stopping on the first failure.

That flow is replaced by: discover_items() reads the *.txt files directly
(no intermediate JSON/py-file generation), extract_metadata() +
build_invenio_metadata() below reproduce the exact same metadata mapping as
the old ITk_upload2_create_metadata.py, and bulk_async.py drives everything
concurrently with resume/retry/circuit-breaker support -- same as the FRAM
pipeline -- instead of the old runner's sequential subprocess loop.

NOTE: the *.txt metadata step (whatever currently generates those files) is
expected to eventually be folded into this script so metadata is derived
in-memory with no *.txt file in between -- but that's a later step. For
now this file still reads pre-existing *.txt files, same as before.

Token handling mirrors FRAM: nothing is hardcoded here. bulk_async.py /
async_upload.py resolve it the same way as fram_bulk_async.py did
(--token flag -> INVENIO_TOKEN env var -> interactive prompt).

COMMUNITY HANDLING: like fram_upload.py, this adapter sets "community"/
"workflow" keys in build_invenio_metadata(); async_upload.py's
upload_record_async() passes them through automatically as the
community=/workflow= keyword arguments of client.records.create() (see
its "COMMUNITY / WORKFLOW" docstring section), so no engine change is
needed. ITK_COMMUNITY / ITK_WORKFLOW below are TO_FILL placeholders --
update them once Cesnet/FZU finalizes the community/access workflow for
this model.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

# ============================================================
# DEFAULTS (used by async_upload.py's smoke test; override via CLI flags
# for real runs)
# ============================================================

DEFAULT_METADATA_DIR = "/home/[xyz]/Python WSL/ITk/Upload/Metadata"
DEFAULT_DATA_ROOT = "/home/[xyz]/Python WSL/ITk/Upload/Data to upload"
DEFAULT_README_FILE = None  # README is optional -- pass --readme-file to include one

# Must match this adapter's key in adapters.ADAPTERS.
ADAPTER_NAME = "itk"

# Confirmed for local only so far; verify before using with test1/production
# (same caveat as FRAM's DEFAULT_SCHEMA_URL).
DEFAULT_SCHEMA_URL = "local://atlas_itk-v1.0.0.json"

# TO_FILL -- community/workflow not yet decided for ITk. Update once
# Cesnet/FZU finalizes the community/access workflow for this model (see
# fram_upload.py's FRAM_COMMUNITY for the equivalent, already-resolved
# case, and async_upload.py's "COMMUNITY / WORKFLOW" docstring section
# for the mechanism).
ITK_COMMUNITY = "TO_FILL"
ITK_WORKFLOW = "TO_FILL"

# ============================================================
# FIXED METADATA  (unchanged from ITk_upload2_create_metadata.py)
# ============================================================

CREATORS = [
    {
        "person_or_org": {
            "name": "FZU Institute of Physics of the Czech Academy of Sciences",
            "type": "organizational",
        }
    },
]

CONTRIBUTORS = [
    {
        "person_or_org": {
            "name": "ATLAS-ITk",
            "type": "organizational",
        },
        "role": {"id": "ResearchGroup"},
    }
]

SUBJECTS = [
    "Silicon sensor",
    "Particle physics",
    "CERN",
    "Physics",
    "Detector",
    "ATLAS-ITk",
]

# Fields (in the dict returned by extract_metadata) every record must have a
# usable value for before upload -- minimal defensive check, mirrors FRAM's
# REQUIRED_METADATA_FIELDS / validate_extracted_metadata() pattern.
REQUIRED_EXTRACTED_FIELDS = ("batch", "creation_date")


# ============================================================
# WORK ITEM
# ============================================================


@dataclass
class ItkWorkItem:
    key: str                    # resume/dedup key, e.g. "ATLAS-ITk__VPA56032"
    txt_path: Path               # source *.txt metadata file
    zip_path: Path                # matching *.zip data file in data_root
    readme_path: Path | None      # shared README -- optional, may be None


# ============================================================
# HELPERS  (unchanged from ITk_upload2_create_metadata.py)
# ============================================================


def sanitize_filename(name: str) -> str:
    """Make a safe key/title string, e.g. for stats rows and record titles."""
    return re.sub(r"[^\w\-_.()]", "", name.replace(" ", "_"))


def parse_txt(path: Path) -> dict:
    """Extract key-value pairs from a *.txt metadata file."""
    result = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            if "Checksum" in key:
                continue
            result[key] = value
    return result


def to_list(value) -> list:
    """Ensure value is always a list, filtering out empty strings."""
    if isinstance(value, list):
        return value
    if value == "" or value is None:
        return []
    return [value]


# ============================================================
# DISCOVERY
# ============================================================


def discover_items(
    metadata_dir: Path, data_root: Path, readme_file: Path | None = None
) -> list[ItkWorkItem]:
    """Find every *.txt metadata file in metadata_dir and pair it with its
    matching *.zip file in data_root (same stem, e.g. VPA56032.txt ->
    VPA56032.zip). A TXT file with no matching ZIP is skipped with a
    warning rather than raising, so one bad batch doesn't stop discovery
    for the rest.

    readme_file is optional -- pass None (the default) to upload records
    without any README attached.
    """
    resolved_readme = readme_file if (readme_file and readme_file.exists()) else None
    if readme_file and not resolved_readme:
        print(f"WARNING: --readme-file given but not found: {readme_file} -- uploading without it")

    items: list[ItkWorkItem] = []
    for txt_path in sorted(metadata_dir.glob("*.txt")):
        zip_path = data_root / txt_path.with_suffix(".zip").name
        if not zip_path.exists():
            print(f"WARNING: no matching ZIP for {txt_path.name} (expected {zip_path}); skipping")
            continue

        data = parse_txt(txt_path)
        batch = data.get("Batch", txt_path.stem)
        title = f"ATLAS-ITk_ {batch}"
        key = sanitize_filename(title)

        items.append(
            ItkWorkItem(key=key, txt_path=txt_path, zip_path=zip_path, readme_path=resolved_readme)
        )
    return items


# ============================================================
# METADATA EXTRACTION
# (unchanged mapping from ITk_upload2_create_metadata.py, just returned as
# a dict instead of being written straight into the record payload)
# ============================================================


def extract_metadata(item: ItkWorkItem) -> dict:
    data = parse_txt(item.txt_path)

    batch = data.get("Batch")
    wafer = to_list(data.get("Wafer", ""))
    files = to_list(data.get("Files", ""))
    components = to_list(data.get("Component", ""))
    run_numbers = [str(int(r)) for r in to_list(data.get("RunNumber", ""))]
    test_types = to_list(data.get("TestType", ""))
    component_types = to_list(data.get("Type", ""))

    creation_date = None
    date_created_txt = data.get("Date", "")
    if date_created_txt:
        try:
            creation_date = datetime.strptime(date_created_txt, "%d %b %Y").date().isoformat()
        except ValueError:
            creation_date = None

    publication_date = date.today().isoformat()
    title = f"ATLAS-ITk_ {batch}"

    return {
        "key": item.key,
        "title": title,
        "batch": batch,
        "wafer": wafer,
        "files": files,
        "components": components,
        "run_numbers": run_numbers,
        "test_types": test_types,
        "component_types": component_types,
        "creation_date": creation_date,
        "publication_date": publication_date,
    }


def validate_metadata(extracted: dict) -> list[str]:
    """Minimal defensive check before upload -- not a substitute for
    validating the TXT files themselves upstream."""
    problems = []
    for field in REQUIRED_EXTRACTED_FIELDS:
        if not extracted.get(field):
            problems.append(f"missing or unparseable required field: {field}")
    return problems


# ============================================================
# METADATA -> INVENIORDM JSON
# (unchanged shape from ITk_upload2_create_metadata.py)
# ============================================================


def build_invenio_metadata(extracted: dict) -> dict:
    title = extracted["title"].replace(" ", "_")
    return {
        "metadata": {

            "related_resources": [{"title": "ITK_2022", "identifiers": [{"identifier": "https://127.0.0.1:5000/atlas_itk/records/5xgdm-9ev10", "scheme": "url"}], "relation_type": {"id": "IsPartOf"}},
                                {"title": "ITk", "identifiers": [{"identifier": "https://127.0.0.1:5000/atlas_itk/records/5xgdm-9ev77", "scheme": "url"}], "relation_type": {"id": "IsPartOf"}}],            
            "resource_type": {"id": "c_ddb1"},
            "creators": CREATORS,
            "contributors": CONTRIBUTORS,
            "file_types": ["dat"],
            "title": title,
            "files": extracted["files"],
            "publication_date": extracted["publication_date"],
            "publisher": "FZU Institute of Physics of the Czech Academy of Science",
            "additional_descriptions": [
                {
                    "lang": {"id": "ENG"},
                    "type": {"id": "abstract"},
                    "description": (
                        "This dataset contains experimental data from silicon sensor "
                        "module tests performed as part of the CERN ATLAS ITk project."
                    ),
                }
            ],
            "identifiers": [{"identifier": "", "scheme": "url"}],
            "subjects": [{"subject": s} for s in SUBJECTS],
            "rights": [{"id": "4-BY"}],
            "dates": [{"date": extracted["creation_date"], "type": {"id": "Created"}}],
            "experiment": {"id": "ATLAS_ITk"},
            "manufacturer": "Hamamatsu Photonics",
            "batch": extracted["batch"],
            "wafer": extracted["wafer"],
            "components": extracted["components"],
            "run_numbers": extracted["run_numbers"],
            "test_types": extracted["test_types"],
            "component_types": extracted["component_types"],
        },
        "files": {"enabled": False},
        "access": {
            "record": "public",
            "files": "restricted",
            "embargo": {"active": "false", "reason": "null"},
            "status": "restricted",
        },
        # See ITK_COMMUNITY / ITK_WORKFLOW above and the "COMMUNITY
        # HANDLING" section of the module docstring. async_upload.py
        # passes these through as client.records.create()'s community=/
        # workflow= keyword arguments automatically.
        "community": ITK_COMMUNITY,
        "workflow": ITK_WORKFLOW,
    }


# ============================================================
# FILES TO UPLOAD PER RECORD
# ============================================================


def get_upload_files(item: ItkWorkItem, extracted: dict) -> list[tuple[str, Path, str]]:
    """Return (file_key, path, description) tuples for every file that
    should be attached to the record: the data ZIP, plus the README only
    if one was actually supplied via --readme-file and found on disk.
    """
    uploads = [(item.zip_path.name, item.zip_path, "Measurement data")]
    if item.readme_path is not None:
        uploads.append(("README.txt", item.readme_path, "General description"))
    return uploads


# ============================================================
# CLI ENTRY POINT
#
# bulk_async.py itself stays fully generic -- it always requires
# --metadata-dir/--data-root explicitly and knows nothing about ITk's
# default paths. Running it directly means typing those out every time.
# It also errors out if run directly at all (see its __main__ guard) --
# this file is the actual entry point.
#
# Assumed layout: async_upload.py / bulk_async.py (the shared engine) live
# one directory up from this adapter, not alongside it, e.g.:
#     Upload/async_upload.py
#     Upload/bulk_async.py
#     Upload/ITk/itk_upload.py   <- this file
# If that's not where they end up, adjust ENGINE_DIR below.
#
#   python3 itk_upload.py --environment local
#   python3 itk_upload.py --environment test1 --dry-run
#   python3 itk_upload.py --environment local --metadata-dir "/some/other/Metadata"
#
# A future model's adapter would live in its own sibling folder (e.g.
# Upload/FRAM/fram_metadata.py) with the same sys.path adjustment and an
# equivalent CLI entry point; bulk_async.py/async_upload.py need no changes
# either way.
# ============================================================

ENGINE_DIR = Path(__file__).resolve().parent.parent


def _run_via_bulk_async() -> None:
    import importlib
    import os
    import sys

    # async_upload.py / bulk_async.py aren't in this file's own directory,
    # so Python won't find them via the default sys.path (which only
    # includes the directory of whatever script was actually run). Add the
    # shared engine's directory explicitly, before importing it.
    if str(ENGINE_DIR) not in sys.path:
        sys.path.insert(0, str(ENGINE_DIR))

    # Loaded via importlib.import_module(), not a literal `import
    # bulk_async` statement, on purpose: some editors' "organize imports" /
    # auto-import features rewrite unresolved top-level imports into a
    # fully-qualified dotted path guessed from the workspace layout (e.g.
    # `import ITk.Scripts.upload.bulk_async as bulk_async`), which breaks
    # this sys.path-based resolution. A function call isn't touched by
    # those tools.
    bulk_async = importlib.import_module("bulk_async")

    # Fallback for any code path that reads the env var instead of the CLI
    # flag (e.g. a direct async_upload.main_async() smoke test); the
    # --adapter flag injected below takes precedence for bulk_async.py
    # itself since it's an explicit CLI arg.
    os.environ.setdefault("PHYSICS_ADAPTER", ADAPTER_NAME)

    default_flags = {
        "--adapter": ADAPTER_NAME,
        "--metadata-dir": DEFAULT_METADATA_DIR,
        "--data-root": DEFAULT_DATA_ROOT,
    }
    if DEFAULT_README_FILE:
        default_flags["--readme-file"] = DEFAULT_README_FILE

    injected = []
    for flag, value in default_flags.items():
        injected += [flag, value]

    sys.argv = [sys.argv[0]] + injected + sys.argv[1:]
    bulk_async.main()


if __name__ == "__main__":
    _run_via_bulk_async()


#   cd upload/invenio/itk
#   python3 itk_upload.py