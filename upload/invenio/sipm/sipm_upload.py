"""
DUNE SiPM (silicon photomultiplier test data) adapter for the generic
async/bulk upload pipeline (async_upload.py / bulk_async.py).

This replaces the old three-step SiPM_upload2/3/4 flow:
  - SiPM_upload2_create_metadata.py parsed *.txt files into per-record
    *.json metadata files on disk.
  - SiPM_upload3_pyfile_generator.py read those JSON files and generated
    one standalone upload_sipm_<Prg>.py script per record from a string
    template, discovering that record's data folder as
    DATA_ROOT / title.removeprefix("SiPM_").
  - SiPM_upload4_py_runner.py ran a hand-picked subset of those generated
    scripts as subprocesses, sequentially, stopping on the first failure.

That flow is replaced by: discover_items() reads the *.txt files directly
(no intermediate JSON/py-file generation), extract_metadata() +
build_invenio_metadata() below reproduce the exact same metadata mapping
as the old SiPM_upload2_create_metadata.py, and bulk_async.py drives
everything concurrently with resume/retry/circuit-breaker support --
same as the ITk and FRAM pipelines -- instead of the old runner's
manual, sequential subprocess list.

SiPM_upload1_data_copy.py (zipping tray folders + writing the *.txt
metadata files) is unchanged and still runs upstream of this script.

KEY DIFFERENCE FROM itk_upload.py: an ITk record has exactly one ZIP.
A SiPM record's data lives in a *folder* of several tray ZIPs
(Tray*.zip), matching SiPM_upload1's `destination_path` /
SiPM_upload3's `data_dir = DATA_ROOT / title.removeprefix("SiPM_")`.
So the work item here carries a `data_dir`, not a single `zip_path`,
and get_upload_files() uploads every ZIP found inside it.

SiPM records need a `communities` block (old script set
`{"ids": ["SiPM"]}`); async_upload.py's upload_record_async() includes it
in the records.create() payload automatically whenever an adapter's
build_invenio_metadata() sets one (ITk's doesn't, SiPM's does -- both work
unmodified).

Which adapter is active is resolved at runtime by adapters.py, not
hardcoded in async_upload.py/bulk_async.py: this file passes
--adapter sipm to bulk_async.py's CLI (and sets the PHYSICS_ADAPTER env
var as a fallback) so `python3 sipm_upload.py ...` just works, including
side-by-side with `python3 itk_upload.py ...` in a separate process. See
adapters.py for the full mechanism.

Token handling mirrors ITk/FRAM: nothing is hardcoded here.
bulk_async.py / async_upload.py resolve it the same way
(--token flag -> INVENIO_TOKEN env var -> interactive prompt).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

# ============================================================
# DEFAULTS (used by async_upload.py's smoke test; override via CLI flags
# for real runs)
# ============================================================

DEFAULT_METADATA_DIR = "/home/[XX]/Python WSL/SiPM/Upload/Metadata"
DEFAULT_DATA_ROOT = "/home/[XX]/Python WSL/SiPM/Upload/Data to upload"
DEFAULT_README_FILE = "/home/[XX]/Python WSL/SiPM/Upload/README.txt"

# Must match this adapter's key in adapters.ADAPTERS.
ADAPTER_NAME = "sipm"

# Confirmed for local only so far; verify before using with test1/production
# (same caveat as ITk's / FRAM's DEFAULT_SCHEMA_URL).
DEFAULT_SCHEMA_URL = "local://sipm-v1.0.0.json"

# ============================================================
# FIXED METADATA  (unchanged from SiPM_upload2_create_metadata.py)
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
            "name": "FERMILAB-CZ",
            "type": "organizational",
        },
        "role": {"id": "ResearchGroup"},
    }
]

SUBJECTS = [
    "Silicon photomultiplier",
    "Photon detector",
    "Particle physics",
    "Deep Underground Neutrino Experiment (DUNE)",
    "Breakdown voltage",
    "Dark count rate (DCR)",
    "Physics",
    "FERMILAB-CZ",
]

MEASUREMENT_TYPES = [
    "DCR",
    "RoomT forward",
    "RoomT reverse",
    "LN2 1st cycle forward",
    "LN2 1st cycle reverse",
    "LN2 3rd cycle forward",
    "LN2 3rd cycle reverse",
    "LN2 3rd cycle extended",
]

ARDU_UNITS = ["ARDU 0", "ARDU 1", "ARDU 2", "ARDU 3"]

# Fields (in the dict returned by extract_metadata) every record must have a
# usable value for before upload -- minimal defensive check, mirrors ITk's
# REQUIRED_EXTRACTED_FIELDS / validate_metadata() pattern.
REQUIRED_EXTRACTED_FIELDS = ("title", "creation_date")


# ============================================================
# WORK ITEM
# ============================================================


@dataclass
class SipmWorkItem:
    key: str                    # resume/dedup key, e.g. "SiPM_Prg6_Hamamatsu_Photonics_S01"
    txt_path: Path                # source *.txt metadata file
    data_dir: Path                 # folder holding this record's Tray*.zip files
    readme_path: Path | None       # shared README -- optional, may be None


# ============================================================
# HELPERS  (unchanged from SiPM_upload2_create_metadata.py)
# ============================================================


def sanitize_filename(name: str) -> str:
    """Make a safe key string, e.g. for stats rows and record titles."""
    return re.sub(r"[^\w\-_.()]", "", name.replace(" ", "_"))


def extract_last_date(date_string: str) -> str:
    """Return the last date in a 'YYYY-MM-DD - YYYY-MM-DD' range string."""
    parts = re.findall(r"\d{4}-\d{2}-\d{2}", date_string)
    return parts[-1] if parts else date_string.strip()


def parse_csv_list(text: str) -> list[str]:
    """Convert comma-separated values into a clean list."""
    return [item.strip() for item in text.split(",") if item.strip()]


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


# ============================================================
# DISCOVERY
# ============================================================


def discover_items(
    metadata_dir: Path, data_root: Path, readme_file: Path | None = None
) -> list[SipmWorkItem]:
    """Find every *.txt metadata file in metadata_dir and pair it with its
    matching data folder in data_root: title.removeprefix("SiPM_"), same
    lookup SiPM_upload3_pyfile_generator.py used
    (data_dir = DATA_ROOT / title.removeprefix("SiPM_")). A TXT file with
    no matching data folder is skipped with a warning rather than raising,
    so one bad batch doesn't stop discovery for the rest.

    readme_file is optional -- pass None to upload records without any
    README attached.
    """
    resolved_readme = readme_file if (readme_file and readme_file.exists()) else None
    if readme_file and not resolved_readme:
        print(f"WARNING: --readme-file given but not found: {readme_file} -- uploading without it")

    items: list[SipmWorkItem] = []
    for txt_path in sorted(metadata_dir.glob("*.txt")):
        data = parse_txt(txt_path)
        title = data.get("Title")
        if not title:
            print(f"WARNING: no Title field in {txt_path.name}; skipping")
            continue

        data_dir = data_root / title.removeprefix("SiPM_")
        if not data_dir.is_dir():
            print(f"WARNING: no matching data folder for {txt_path.name} (expected {data_dir}); skipping")
            continue

        key = sanitize_filename(title)
        items.append(
            SipmWorkItem(key=key, txt_path=txt_path, data_dir=data_dir, readme_path=resolved_readme)
        )
    return items


# ============================================================
# METADATA EXTRACTION
# (unchanged mapping from SiPM_upload2_create_metadata.py, just returned
# as a dict instead of being written straight into a JSON file)
# ============================================================


def extract_metadata(item: SipmWorkItem) -> dict:
    data = parse_txt(item.txt_path)

    title = data.get("Title")
    manufacturer = data.get("Manufacturer")
    box = data.get("Box")
    requestor = data.get("Requestor")

    trays = parse_csv_list(data.get("Tray", ""))
    tray_numbers = parse_csv_list(data.get("Tray numbers", ""))
    qr_list = parse_csv_list(data.get("Strip_ID", ""))

    date_created_txt = data.get("Date Created", "")
    creation_date = extract_last_date(date_created_txt)
    publication_date = date.today().isoformat()

    return {
        "key": item.key,
        "title": title,
        "manufacturer": manufacturer,
        "box": box,
        "requestor": requestor,
        "trays": trays,
        "tray_numbers": tray_numbers,
        "qr_list": qr_list,
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
# (unchanged shape from SiPM_upload2_create_metadata.py)
# ============================================================


def build_invenio_metadata(extracted: dict) -> dict:
    title = re.sub(r"\s+", "_", extracted["title"])
    return {
        "metadata": {
            "resource_type": {"id": "c_ddb1"},
            "creators": CREATORS,
            "contributors": CONTRIBUTORS,
            "file_types": ["csv", "txt", "pdf"],
            "title": title,
            "publication_date": extracted["publication_date"],
            "publisher": "FZU Instsitute of Physics of the Czech Academy of Science",
            "additional_descriptions": [
                {
                    "lang": {"id": "ENG"},
                    "type": {"id": "abstract"},
                    "description": (
                        "This dataset contains experimental data from silicon photomultiplier (SiPM) "
                        "detector testing performed in the context of the Deep Underground Neutrino "
                        "Experiment (DUNE). The testing is carried out in collaboration between the "
                        "Institute of Physics of the Czech Academy of Sciences (FZU) and Fermilab-CZ."
                    ),
                }
            ],
            "subjects": [{"subject": s} for s in SUBJECTS],
            "rights": [{"id": "4-BY"}],
            "dates": [{"date": extracted["creation_date"], "type": {"id": "Created"}}],
            "experiment": {"id": "DUNE_SiPM"},
            "manufacturer": extracted["manufacturer"],
            "requestor": extracted["requestor"],
            "requestor_search": [extracted["requestor"]],
            "box": extracted["box"],
            "measurement_types": MEASUREMENT_TYPES,
            "ardu_units": ARDU_UNITS,
            "trays": extracted["trays"],
            "tray_numbers": extracted["tray_numbers"],
            "qr_list": extracted["qr_list"],
        },
        "files": {"enabled": False},
        "access": {
            "record": "public",
            "files": "restricted",
            "embargo": {"active": "false", "reason": "null"},
            "status": "restricted",
        },
        "communities": {"ids": ["SiPM"]},
    }


# ============================================================
# FILES TO UPLOAD PER RECORD
# ============================================================


def get_upload_files(item: SipmWorkItem, extracted: dict) -> list[tuple[str, Path, str]]:
    """Return (file_key, path, description) tuples for every file that
    should be attached to the record: every Tray*.zip found in the
    record's data folder (key = "<data_dir.name>__<zip_name>", same
    convention as the old generated per-record scripts), plus the README
    only if one was actually supplied via --readme-file and found on
    disk.
    """
    uploads = [
        (f"{item.data_dir.name}__{zip_path.name}", zip_path, "Measurement data")
        for zip_path in sorted(item.data_dir.glob("*.zip"))
    ]
    if item.readme_path is not None:
        uploads.append(("README.txt", item.readme_path, "General description"))
    return uploads


# ============================================================
# CLI ENTRY POINT
#
# bulk_async.py itself stays fully generic -- it always requires
# --metadata-dir/--data-root explicitly and knows nothing about SiPM's
# default paths. Running it directly means typing those out every time.
# It also errors out if run directly at all (see its __main__ guard) --
# this file is the actual entry point.
#
# Assumed layout: async_upload.py / bulk_async.py (the shared engine) live
# one directory up from this adapter, not alongside it, e.g.:
#     Upload/async_upload.py
#     Upload/bulk_async.py
#     Upload/SiPM/sipm_upload.py   <- this file
# If that's not where they end up, adjust ENGINE_DIR below.
#
#   python3 sipm_upload.py --environment local
#   python3 sipm_upload.py --environment test1 --dry-run
#   python3 sipm_upload.py --environment local --metadata-dir "/some/other/Metadata"
#
# async_upload.py/bulk_async.py need no edits to run this adapter --
# --adapter sipm (injected below) and the PHYSICS_ADAPTER env var both
# resolve to this module via adapters.py.
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
    # fully-qualified dotted path guessed from the workspace layout, which
    # breaks this sys.path-based resolution. A function call isn't touched
    # by those tools.
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


#   cd upload/invenio/sipm
#   python3 sipm_upload.py --enviornment test1
