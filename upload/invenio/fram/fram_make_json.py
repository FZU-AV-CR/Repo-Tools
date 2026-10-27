#!/usr/bin/env python3
"""
fram_make_json.py -- walk a folder recursively for FRAM FITS files and write
one JSON metadata file per FITS file, into a mirrored directory tree.

Each JSON has the keys: metadata, files, access, community, model.

SINGLE SOURCE OF TRUTH: metadata extraction, validation and record building are
NOT duplicated here -- they are imported from fram_upload.py (the same code the
direct uploader uses), so both paths always produce identical metadata. This
script only adds what is specific to bulk JSON generation: the directory walk
and filters, the site-year related-resources table, a fixed publication date,
the "files.entries" block used to pair each record with its file on S3, and
atomic mirrored-tree output.

Requires fram_upload.py and calibrate.py in the same directory, plus astropy,
numpy, healpy, scipy. Optional: sep (star detection for fwhm_arcsec /
fwhm_pixels; without it those two fields are simply left out, or pass
--no-quality to skip the measurement on purpose).

WHAT A RECORD CONTAINS (see metadata.yaml / fram_upload.py)
    Sky-position fields (center, radius, center_geo, footprint, healpix_idx)
    exist only for science frames with a valid WCS; calibration frames carry no
    position. Fields without a value are omitted, never written as null/0.0.
    FWHM is measured on the central --quality-size window (default 2048 px; 0 = whole
    frame). Searchable fields include mean, median (overscan cropped, bias NOT
    subtracted), robust_std, instrument, pixel_scale, ccd_temperature,
    air_temperature, bias_level, fwhm_arcsec, fwhm_pixels. Master calibration
    frames (masterdark/masterflat/bias/dcurrent) are used exactly as stored.
    Unreadable / invalid files get no JSON; they are listed in errors.csv with
    a reason code (E_UNREADABLE, E_BAD_DATE, E_CROP_FAILED,
    E_EMPTY_OR_CONSTANT_IMAGE, E_MISSING_FIELD, E_NO_USABLE_SIZE,
    E_BAD_DATE_RANGE, E_BAD_EXPOSURE, E_BAD_TYPE, E_NO_WCS).

REGENERATING AFTER A SCHEMA CHANGE
    Reruns skip JSON files that already exist. Use a fresh --out folder or
    pass --overwrite to regenerate JSON for files that were already processed.

PATH KEYS
    Every FITS file gets an "anchor-relative" key: the part of its absolute
    path starting at the anchor directory (default "data3"), e.g.
        /mnt/data3/cta-n/2021/20210409/03185/x.fits
        -> data3/cta-n/2021/20210409/03185/x.fits
    This key is used for: the "files" entry in the JSON, the mirrored output
    location (<out>/<key without extension>.json), and for --include-paths /
    --skip-paths matching. If the anchor is not in the path, the key falls
    back to the path relative to the scan root.

EXAMPLES
    # Only two site-years:
    python3 fram_make_json.py --root /mnt --out /data/json \\
        --include-paths data3/cta-n/2021,data3/auger2/2023 --workers 16

    # Everything except site-years that are already uploaded:
    python3 fram_make_json.py --root /mnt/data3 --out /data/json \\
        --skip-paths-file already_uploaded.txt --workers 16

    # One month across all sites, capped batch size:
    python3 fram_make_json.py --root /mnt --out /data/json \\
        --date-pattern "202204*" --max-files 500000 --workers 16

Reruns skip JSON files that already exist (use --overwrite to regenerate),
so a 10M-file job can be split over many runs.

SITE-YEAR RELATED RESOURCES (optional)
    By default related_resources holds only FRAM_FZU_root. To add the
    site-year specific resource (DOI + name), give a CSV and/or inline entries:

        --site-year-resources resources.csv
        --site-year-resource auger,2023,10.83100/xxxx-xxxx,FRAM_2023_auger

    CSV columns: site, year, doi, name, invenio_url  (invenio_url is ignored).
    Lookup is by FOLDER NAMES in the anchor-relative path:
    data3/<site>/<year>/... -> row (site, year). Inline entries override CSV
    rows for the same site+year. A file with no matching row gets the root
    resource only (one warning per site-year); with
    --require-site-year-resource it goes to errors.csv instead.

PATCH MODE
    Update related_resources in ALREADY GENERATED JSON files without reading
    any FITS file (fast). Needs only --out and a resource table:

        python3 fram_make_json.py --patch-related-resources --out ./aug23 \\
            --site-year-resources resources.csv --workers 8

    --include-paths / --skip-paths / --exclude-dirs / --date-pattern apply to
    the JSON tree (same anchor-relative paths). Files already correct are
    left untouched.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import fnmatch
import json
import logging
import math
import os
import re
import sys
import time
import traceback
import warnings
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path

import numpy as np

import fram_upload as F  # shared extraction / validation / record building

# astropy emits a multi-line INFO notice for every header with SIP
# coefficients but a CTYPE without "-SIP". It is harmless and floods the log,
# so keep only WARNING and above. Set at import time so every worker process
# picks it up. (FITSFixedWarning is already silenced inside fram_upload.)
logging.getLogger("astropy").setLevel(logging.WARNING)
logger = logging.getLogger("fram_make_json")

# ============================================================
# CONSTANTS (carried over from fram_upload.py)
# ============================================================

FITS_EXTENSIONS = (".fits", ".fit", ".fts")
DATE_DIR_RE = re.compile(r"^\d{8}$")
DEFAULT_ANCHOR = "data3"
DEFAULT_EXCLUDE_DIRS = ["bad"]

# Always present. A site-year specific resource (from the resource table) is
# placed BEFORE it, as in the original fram_upload.py.
RELATED_RESOURCES = [
    {
        "title": "FRAM_FZU_root",
        "identifiers": [{"identifier": "https://doi.org/10.83100/ddxy-p647", "scheme": "url"}],
        "relation_type": {"id": "IsPartOf"},
    }
]


# ============================================================
# SITE-YEAR RESOURCE TABLE
# ============================================================

RESOURCE_COLUMNS = ("site", "year", "doi", "name")  # invenio_url is read but ignored


def normalize_doi(value: str) -> str:
    """Accept '10.83100/x', 'doi:10.83100/x' or 'https://doi.org/10.83100/x'
    and return the https://doi.org/... URL form."""
    v = re.sub(r"^doi:\s*", "", value.strip(), flags=re.I)
    m = re.match(r"^https?://(?:dx\.)?doi\.org/(.+)$", v, flags=re.I)
    if m:
        v = m.group(1)
    if not re.match(r"^10\.\d{4,9}/\S+$", v):
        raise ValueError(f"not a valid DOI: {value!r}")
    return "https://doi.org/" + v


def make_resource(name: str, doi: str) -> dict:
    return {
        "title": name,
        "identifiers": [{"identifier": normalize_doi(doi), "scheme": "url"}],
        "relation_type": {"id": "IsPartOf"},
    }


def _table_key(site: str, year: str) -> tuple[str, str]:
    return site.strip().lower(), year.strip()


def _check_row(site, year, doi, name) -> str | None:
    if not site:
        return "empty site"
    if not re.fullmatch(r"\d{4}", year or ""):
        return f"year must be 4 digits, got {year!r}"
    if not name:
        return "empty name"
    if not doi:
        return "empty doi"
    try:
        normalize_doi(doi)
    except ValueError as exc:
        return str(exc)
    return None


def load_resource_table(csv_path: str | None, inline: list[str]) -> dict[tuple[str, str], dict]:
    """Build {(site, year): resource}. Raises ValueError listing every problem."""
    table: dict[tuple[str, str], dict] = {}
    problems: list[str] = []

    if csv_path:
        with open(csv_path, encoding="utf-8-sig", newline="") as fh:  # utf-8-sig strips an Excel BOM
            first = fh.readline()
            fh.seek(0)
            if not first.strip():
                raise ValueError(f"{csv_path}: file is empty")
            delim = max(",;\t", key=first.count)  # Czech Excel writes ';'
            reader = csv.DictReader(fh, delimiter=delim)
            fields = {re.sub(r"[\s-]+", "_", f.strip().lower()): f for f in (reader.fieldnames or [])}
            missing = [c for c in RESOURCE_COLUMNS if c not in fields]
            if missing:
                raise ValueError(
                    f"{csv_path}: missing column(s) {missing}; found {list(fields)}. "
                    f"Expected: site, year, doi, name, invenio_url"
                )
            for lineno, row in enumerate(reader, start=2):
                vals = {c: (row.get(fields[c]) or "").strip() for c in RESOURCE_COLUMNS}
                if not any(vals.values()):
                    continue  # blank line
                err = _check_row(vals["site"], vals["year"], vals["doi"], vals["name"])
                if err:
                    problems.append(f"{csv_path} line {lineno}: {err}")
                    continue
                k = _table_key(vals["site"], vals["year"])
                if k in table:
                    problems.append(f"{csv_path} line {lineno}: duplicate site+year {k[0]}/{k[1]}")
                    continue
                table[k] = make_resource(vals["name"], vals["doi"])

    for spec in inline:
        parts = [p.strip() for p in spec.split(",", 3)]  # name may itself contain commas
        if len(parts) != 4:
            problems.append(f"--site-year-resource {spec!r}: expected site,year,doi,name")
            continue
        err = _check_row(*parts)
        if err:
            problems.append(f"--site-year-resource {spec!r}: {err}")
            continue
        table[_table_key(parts[0], parts[1])] = make_resource(parts[3], parts[2])  # overrides CSV

    if problems:
        raise ValueError("Invalid site-year resource input:\n  " + "\n  ".join(problems))
    return table


def site_year_from_key(key: str, anchor: str) -> tuple[str, str] | None:
    """data3/<site>/<year>/... -> (site, year). Needs at least one more
    component (the file or a deeper folder) below the year."""
    parts = _split(key)
    if parts and parts[0] == anchor:
        parts = parts[1:]
    if len(parts) < 3:
        return None
    return _table_key(parts[0], parts[1])


def resolve_resource(key, anchor, table, warned: set, require: bool):
    """Return (resource | None, missing). 'missing' is True only when a table
    is in use, no row matched and --require-site-year-resource is set."""
    if not table:
        return None, False
    sy = site_year_from_key(key, anchor)
    res = table.get(sy) if sy else None
    if res is not None:
        return res, False
    if sy not in warned:
        warned.add(sy)
        logger.warning(
            "No site-year resource row for site/year %s (e.g. %s) -> %s",
            "/".join(sy) if sy else "<not derivable from path>", key,
            "files go to errors.csv" if require else "using FRAM_FZU_root only",
        )
    return None, require


def related_resources_for(resource: dict | None) -> list[dict]:
    return ([resource] if resource else []) + list(RELATED_RESOURCES)


# ============================================================
# PATH KEYS AND DIRECTORY FILTERING
# ============================================================


def make_key(path: Path, root: Path, anchor: str) -> str:
    """Anchor-relative key (see module docstring); falls back to root-relative."""
    parts = path.parts
    if anchor and anchor in parts:
        return "/".join(parts[parts.index(anchor):])
    try:
        rel = path.relative_to(root).as_posix()
    except ValueError:
        rel = path.as_posix().lstrip("/")
    return "" if rel == "." else rel


def _split(key: str) -> list[str]:
    return [p for p in key.split("/") if p]


def _prefix_match(key_parts: list[str], pat_parts: list[str], n: int) -> bool:
    return all(fnmatch.fnmatch(k, p) for k, p in zip(key_parts[:n], pat_parts[:n]))


def inside_any(key_parts: list[str], patterns: list[list[str]]) -> bool:
    """True if the key is AT or BELOW one of the patterns (glob per component)."""
    return any(
        len(key_parts) >= len(p) and _prefix_match(key_parts, p, len(p)) for p in patterns
    )


def on_path_to_any(key_parts: list[str], patterns: list[list[str]]) -> bool:
    """True if the key is at, below, OR an ancestor of one of the patterns,
    i.e. walking into it can still lead to an included directory."""
    return any(_prefix_match(key_parts, p, min(len(key_parts), len(p))) for p in patterns)


def _clean_path_list(values: list[str]) -> list[list[str]]:
    return [_split(v.strip().strip("/")) for v in values if v.strip()]


def _read_list_file(path: str | None) -> list[str]:
    if not path:
        return []
    out = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            out.append(line)
    return out


def _csv_arg(value: str | None) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()] if value else []


def _name_matches_any(name: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(name, p) for p in patterns)


def filter_dirs(dkey, dirnames, include, skip, exclude_dirs, date_patterns):
    """Return (kept_dirnames, pruned_count) for one directory of a walk."""
    kept, pruned = [], 0
    for name in dirnames:
        child = dkey + [name]
        if _name_matches_any(name, exclude_dirs):
            pruned += 1
        elif date_patterns and DATE_DIR_RE.match(name) and not _name_matches_any(name, date_patterns):
            pruned += 1
        elif skip and inside_any(child, skip):
            pruned += 1
        elif include and not on_path_to_any(child, include):
            pruned += 1
        else:
            kept.append(name)
    return sorted(kept), pruned


def discover(root, anchor, include, skip, exclude_dirs, date_patterns):
    """Yield (fits_path, key). Directories are pruned during the walk so
    skipped / non-included subtrees are never traversed."""
    pruned_total = 0
    for dirpath, dirnames, filenames in os.walk(root):
        dkey = _split(make_key(Path(dirpath), root, anchor))
        dirnames[:], pruned = filter_dirs(dkey, dirnames, include, skip, exclude_dirs, date_patterns)
        pruned_total += pruned

        # Files only count when this directory is itself inside an included path
        if include and not inside_any(dkey, include):
            continue

        for name in sorted(filenames):
            if os.path.splitext(name)[1].lower() in FITS_EXTENSIONS:
                p = Path(dirpath) / name
                yield p, make_key(p, root, anchor)
    logger.info("Walk finished; %s directories pruned by filters.", pruned_total)


def discover_json(out_dir, include, skip, exclude_dirs, date_patterns):
    """Patch mode: yield (json_path, key) over the mirrored output tree.
    The key is the JSON's path relative to out_dir (it mirrors the FITS key)."""
    pruned_total = 0
    for dirpath, dirnames, filenames in os.walk(out_dir):
        rel = Path(dirpath).relative_to(out_dir).as_posix()
        dkey = [] if rel == "." else _split(rel)
        dirnames[:], pruned = filter_dirs(dkey, dirnames, include, skip, exclude_dirs, date_patterns)
        pruned_total += pruned
        if include and not inside_any(dkey, include):
            continue
        for name in sorted(filenames):
            if name.endswith(".json"):
                yield Path(dirpath) / name, "/".join(dkey + [name])
    logger.info("Walk finished; %s directories pruned by filters.", pruned_total)


# ============================================================
# RECORD BUILDING (extraction + validation live in fram_upload.py)
# ============================================================


def build_record(
    extracted: dict,
    resource: dict | None = None,
    publication_date: str | None = None,
) -> dict:
    """The record fram_upload.py builds (F.build_invenio_metadata), plus the
    three things specific to bulk JSON generation:
      - related_resources from the site-year table (instead of the two fixed
        entries the direct uploader writes),
      - a fixed publication_date when --publication-date* is given,
      - files.entries (key + anchor-relative path), used to pair the record
        with its file on S3 after the bulk record upload."""
    record = F.build_invenio_metadata(extracted)
    metadata = record["metadata"]
    metadata["related_resources"] = related_resources_for(resource)
    if publication_date is not None:
        metadata["publication_date"] = publication_date
    record["files"] = {
        "enabled": True,
        "entries": [{"key": Path(extracted["filename"]).name, "path": extracted["filename"]}],
    }
    return record


def _clean(obj):
    """Make JSON-safe: numpy scalars -> Python, NaN/inf -> None."""
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_clean(v) for v in obj]
    if isinstance(obj, np.generic):
        obj = obj.item()
    if isinstance(obj, float) and not math.isfinite(obj):
        return None
    return obj


def _write_json_atomic(out: Path, record: dict) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + f".tmp{os.getpid()}")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(record, fh, ensure_ascii=False, indent=2, allow_nan=False)
    os.replace(tmp, out)  # atomic: a crash never leaves a half-written JSON


# ============================================================
# WORKERS
# ============================================================

NO_RESOURCE_MSG = "no site-year resource row matches this path (--require-site-year-resource)"


def process_one(task) -> tuple[str, str, str]:
    """task = (fits_path, key, out_path, site_override, resource, missing, publication_date,
               require_wcs, dry_run).
    Returns (key, status, error); status in ok / dryrun / invalid / error / no_resource.
    In dry-run mode extraction and validation run normally but no JSON file is written."""
    fits_path, key, out_path, site_override, resource, missing, publication_date, require_wcs, dry_run = task
    try:
        if missing:
            return key, "no_resource", NO_RESOURCE_MSG
        site = site_override or F._guess_site(str(fits_path))
        extracted = F.extract_metadata(F.FramWorkItem(key=key, fits_path=Path(fits_path), site=site))
        if require_wcs:
            os.environ[F.REQUIRE_WCS_ENV_VAR] = "1"
        problems = F.validate_metadata(extracted)
        if problems:
            return key, "invalid", "; ".join(problems)
        if dry_run:
            return key, "dryrun", ""
        _write_json_atomic(
            Path(out_path),
            _clean(build_record(extracted, resource, publication_date)),
        )
        return key, "ok", ""
    except Exception as exc:
        tb = traceback.format_exc(limit=3).replace("\n", " | ")
        return key, "error", f"{type(exc).__name__}: {exc} [{tb}]"


def patch_one(task) -> tuple[str, str, str]:
    """Patch mode. task = (json_path, key, resource, missing).
    Rewrites metadata.related_resources only; status in patched / unchanged /
    error / no_resource."""
    json_path, key, resource, missing = task
    try:
        if missing:
            return key, "no_resource", NO_RESOURCE_MSG
        path = Path(json_path)
        with path.open("r", encoding="utf-8") as fh:
            record = json.load(fh)
        new = related_resources_for(resource)
        if record["metadata"].get("related_resources") == new:
            return key, "unchanged", ""
        record["metadata"]["related_resources"] = new
        _write_json_atomic(path, record)
        return key, "patched", ""
    except Exception as exc:
        return key, "error", f"{type(exc).__name__}: {exc}"


# ============================================================
# MAIN
# ============================================================


def out_json_path(out_dir: Path, key: str) -> Path:
    return out_dir / Path(key).with_suffix(".json")


def task_generator(args, root, out_dir, include, skip, table, counters, publication_date):
    date_patterns = _csv_arg(args.date_pattern)
    exclude_dirs = _csv_arg(args.exclude_dirs) if args.exclude_dirs is not None else DEFAULT_EXCLUDE_DIRS
    warned: set = set()
    submitted = 0
    for fits_path, key in discover(root, args.anchor, include, skip, exclude_dirs, date_patterns):
        counters["found"] += 1
        target = out_json_path(out_dir, key)
        if not args.overwrite and target.exists():
            counters["exists"] += 1
            continue
        if args.max_files and submitted >= args.max_files:
            logger.info("--max-files %s reached; stopping.", args.max_files)
            return
        submitted += 1
        resource, missing = resolve_resource(key, args.anchor, table, warned, args.require_site_year_resource)
        yield (
            str(fits_path), key, str(target), args.site, resource, missing,
            publication_date, args.require_wcs, args.dry_run,
        )


def patch_task_generator(args, out_dir, include, skip, table, counters):
    date_patterns = _csv_arg(args.date_pattern)
    exclude_dirs = _csv_arg(args.exclude_dirs) if args.exclude_dirs is not None else DEFAULT_EXCLUDE_DIRS
    warned: set = set()
    submitted = 0
    for json_path, key in discover_json(out_dir, include, skip, exclude_dirs, date_patterns):
        counters["found"] += 1
        if args.max_files and submitted >= args.max_files:
            logger.info("--max-files %s reached; stopping.", args.max_files)
            return
        submitted += 1
        resource, missing = resolve_resource(key, args.anchor, table, warned, args.require_site_year_resource)
        yield (str(json_path), key, resource, missing)


def run_tasks(tasks, func, workers: int, handle) -> None:
    if workers <= 1:
        for task in tasks:
            handle(func(task))
        return
    # Bounded in-flight window: never queues millions of futures at once.
    max_inflight = workers * 4
    with ProcessPoolExecutor(max_workers=workers) as pool:
        pending = set()
        for task in tasks:
            pending.add(pool.submit(func, task))
            if len(pending) >= max_inflight:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for f in done:
                    handle(f.result())
        for f in pending:
            handle(f.result())


def run(args) -> int:
    patch = args.patch_related_resources
    out_dir = Path(args.out).resolve()
    root = Path(args.root).resolve() if args.root else None
    if not patch and not root.is_dir():
        logger.error("Scan root does not exist: %s", root)
        return 2
    if patch and not out_dir.is_dir():
        logger.error("Output folder to patch does not exist: %s", out_dir)
        return 2
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        table = load_resource_table(args.site_year_resources, args.site_year_resource)
    except (ValueError, OSError) as exc:
        logger.error("%s", exc)
        return 2
    if table:
        logger.info("Site-year resource table: %s row(s) loaded.", len(table))
    else:
        logger.info("No site-year resources given -> related_resources = FRAM_FZU_root only.")

    if not patch and args.dry_run:
        logger.info("DRY RUN: extraction and validation will run, but no JSON files will be written.")

    if not patch:
        if args.no_quality:
            # Set before the worker pool is created, so every worker inherits it.
            os.environ[F.QUALITY_DISABLED_ENV_VAR] = "1"
            logger.info("--no-quality: fwhm_arcsec / fwhm_pixels will not be measured.")
        else:
            os.environ[F.QUALITY_SIZE_ENV_VAR] = str(args.quality_size)
        if args.no_quality:
            pass
        elif F.sep is None:
            logger.warning("'sep' is not installed -> fwhm_arcsec / fwhm_pixels will be left out "
                           "(pip install sep, or pass --no-quality to silence this).")

    include = _clean_path_list(_csv_arg(args.include_paths) + _read_list_file(args.include_paths_file))
    skip = _clean_path_list(_csv_arg(args.skip_paths) + _read_list_file(args.skip_paths_file))
    if include:
        logger.info("Include-paths (only these are processed): %s", ["/".join(p) for p in include])
    if skip:
        logger.info("Skip-paths (pruned recursively): %s", ["/".join(p) for p in skip])

    errors_path = Path(args.errors_csv) if args.errors_csv else out_dir / "errors.csv"
    write_header = not errors_path.exists()
    err_fh = errors_path.open("a", encoding="utf-8", newline="")
    err_writer = csv.writer(err_fh)
    if write_header:
        err_writer.writerow(["timestamp", "key", "status", "error"])

    counters: dict = defaultdict(int)
    good = {"ok", "patched", "unchanged", "dryrun"}
    t0 = time.monotonic()
    state = {"last_log": t0, "processed": 0}

    def summary() -> str:
        return " ".join(f"{k}={v}" for k, v in sorted(counters.items()))

    def handle(result):
        key, status, error = result
        counters[status] += 1
        state["processed"] += 1
        if status not in good:
            err_writer.writerow([datetime.datetime.now().isoformat(timespec="seconds"), key, status, error])
            err_fh.flush()
        now = time.monotonic()
        if now - state["last_log"] >= args.progress_interval:
            logger.info("processed=%s | %s | %.1f files/s",
                        state["processed"], summary(), state["processed"] / (now - t0))
            state["last_log"] = now

    if patch:
        logger.info("PATCH MODE: updating related_resources in JSON files under %s (no FITS read).", out_dir)
        tasks = patch_task_generator(args, out_dir, include, skip, table, counters)
        func = patch_one
    else:
        tasks = task_generator(
            args, root, out_dir, include, skip, table, counters,
            args.resolved_publication_date,
        )
        func = process_one

    interrupted = False
    try:
        run_tasks(tasks, func, args.workers, handle)
    except KeyboardInterrupt:
        interrupted = True
        logger.warning("Interrupted. Finished JSON files are safe; rerun the same command to resume.")
    finally:
        err_fh.close()

    logger.info("Done in %.1fs. %s | errors log: %s", time.monotonic() - t0, summary(), errors_path)
    if include and counters["found"] == 0:
        logger.warning(
            "--include-paths was given but no files were found. Check that the paths start at "
            "the anchor ('%s') and that %s is at or above it.", args.anchor,
            "--out" if patch else "--root",
        )
    return 130 if interrupted else 0


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Create one FRAM metadata JSON per FITS file (mirrored output tree).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--root", default=None,
                   help="Folder to scan recursively for FITS files (not needed in patch mode)")
    p.add_argument("--out", required=True, help="Output folder for the JSON tree")
    p.add_argument("--anchor", default=DEFAULT_ANCHOR,
                   help="Directory name where file paths/keys start (default: data3)")
    p.add_argument("--include-paths", default=None,
                   help="Comma-separated anchor-relative paths; ONLY these subtrees are processed, "
                        "e.g. data3/cta-n/2021,data3/auger2/2023 (globs allowed per component)")
    p.add_argument("--include-paths-file", default=None,
                   help="Text file with one include path per line (# comments allowed)")
    p.add_argument("--skip-paths", default=None,
                   help="Comma-separated anchor-relative paths pruned recursively, "
                        "e.g. data3/cta-n/2021 (globs allowed per component). Wins over include.")
    p.add_argument("--skip-paths-file", default=None,
                   help="Text file with one skip path per line (# comments allowed)")
    p.add_argument("--exclude-dirs", default=None,
                   help="Comma-separated directory NAME globs skipped at any depth "
                        "(default: bad; pass \"\" to disable)")
    p.add_argument("--date-pattern", default=None,
                   help="Comma-separated globs for YYYYMMDD day folders, e.g. \"202204*,202205*\"")
    publication_date_group = p.add_mutually_exclusive_group()
    publication_date_group.add_argument(
        "--publication-date",
        metavar="YYYY-MM-DD",
        help="Fixed publication date for generated JSON (default: today)",
    )
    publication_date_group.add_argument(
        "--publication-date-offset-days",
        type=int,
        metavar="DAYS",
        help="Publication date this many days from today (must be 0 or greater)",
    )
    p.add_argument("--site", default=None, help="Override site detection for every file")
    p.add_argument("--site-year-resources", default=None, metavar="CSV",
                   help="CSV with columns site,year,doi,name,invenio_url (invenio_url ignored). "
                        "Adds a site-year related resource before FRAM_FZU_root.")
    p.add_argument("--site-year-resource", action="append", default=[], metavar="SITE,YEAR,DOI,NAME",
                   help="Inline site-year resource; repeatable; overrides a CSV row for the same site+year")
    p.add_argument("--require-site-year-resource", action="store_true",
                   help="Files whose site/year has no resource row go to errors.csv instead of "
                        "getting FRAM_FZU_root only")
    p.add_argument("--patch-related-resources", action="store_true",
                   help="Patch mode: only rewrite related_resources in existing JSON files under --out "
                        "(no FITS files are read)")
    p.add_argument("--workers", type=int, default=1,
                   help="Parallel worker processes (default: 1). Each worker holds one frame in memory at a "
                        "time (a few hundred MB for a 4k x 4k frame), so this also bounds RAM use.")
    p.add_argument("--quality-size", type=int, default=F.DEFAULT_QUALITY_SIZE, metavar="N",
                   help="FWHM is measured on the central N x N pixels (default %d; 0 = whole frame, "
                        "3-6x slower on 4k frames). Frames up to N px are measured entirely. Must be 0 or >= 512." % F.DEFAULT_QUALITY_SIZE)
    p.add_argument("--no-quality", action="store_true",
                   help="Skip star detection: fwhm_arcsec / fwhm_pixels are left out (faster). "
                        "Also the behaviour when the optional 'sep' package is not installed.")
    p.add_argument("--require-wcs", action="store_true",
                   help="Reject object frames that have no valid astrometric solution (WCS): "
                        "write them to errors.csv as E_NO_WCS instead of generating a JSON "
                        "without sky-position fields. Has no effect on calibration frames "
                        "(dark, flat, bias, masterdark, masterflat, dcurrent).")
    p.add_argument("--max-files", type=int, default=0,
                   help="Process at most N new files this run (0 = no limit)")
    p.add_argument("--overwrite", action="store_true", help="Regenerate JSON files that already exist")
    p.add_argument("--dry-run", action="store_true",
                   help="Extract and validate metadata for every FITS file but do not write any JSON. "
                        "Use this to check a new data root for invalid or rejected frames before a "
                        "full run. Incompatible with --patch-related-resources.")
    p.add_argument("--errors-csv", default=None, help="Path of errors CSV (default: <out>/errors.csv)")
    p.add_argument("--progress-interval", type=float, default=30.0, help="Seconds between progress lines")
    args = p.parse_args(argv)

    has_table = bool(args.site_year_resources or args.site_year_resource)
    if not args.patch_related_resources and not args.root:
        p.error("--root is required (except with --patch-related-resources)")
    if args.dry_run and args.patch_related_resources:
        p.error("--dry-run cannot be combined with --patch-related-resources")
    if args.patch_related_resources and not has_table:
        p.error("--patch-related-resources needs --site-year-resources and/or --site-year-resource")
    if args.require_site_year_resource and not has_table:
        p.error("--require-site-year-resource needs --site-year-resources and/or --site-year-resource")
    if args.quality_size != 0 and args.quality_size < 512:
        p.error("--quality-size must be 0 (whole frame) or at least 512")
    if args.publication_date is not None:
        try:
            parsed_date = datetime.date.fromisoformat(args.publication_date)
            if parsed_date.isoformat() != args.publication_date:
                raise ValueError
        except ValueError:
            p.error("--publication-date must be a valid date in YYYY-MM-DD format")
    if args.publication_date_offset_days is not None and args.publication_date_offset_days < 0:
        p.error("--publication-date-offset-days must be 0 or greater")
    if args.publication_date is not None:
        args.resolved_publication_date = args.publication_date
    else:
        offset_days = args.publication_date_offset_days or 0
        args.resolved_publication_date = (
            datetime.date.today() + datetime.timedelta(days=offset_days)
        ).isoformat()
    return args


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sys.exit(run(args))


if __name__ == "__main__":
    main()
