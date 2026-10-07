#!/usr/bin/env python3
"""
fram_make_json.py -- walk a folder recursively for FRAM FITS files and write
one JSON metadata file per FITS file, into a mirrored directory tree.

Each JSON has the keys: metadata, files, access, community, model
(the same content fram_upload.py builds, minus the upload engine).

Requires calibrate.py in the same directory, plus astropy, numpy, healpy, scipy.

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

import healpy as hp
import numpy as np
from astropy.io import fits
from astropy.wcs import WCS, FITSFixedWarning

from calibrate import crop_overscans

warnings.simplefilter("ignore", FITSFixedWarning)
# astropy emits a multi-line INFO notice for every header with SIP
# coefficients but a CTYPE without "-SIP". It is harmless (same behaviour as
# fram_upload.py) and floods the log, so keep only WARNING and above.
# Set at import time so every worker process picks it up.
logging.getLogger("astropy").setLevel(logging.WARNING)
logger = logging.getLogger("fram_make_json")

# ============================================================
# CONSTANTS (carried over from fram_upload.py)
# ============================================================

FITS_EXTENSIONS = (".fits", ".fit", ".fts")
HEALPIX_NSIDE = 64
CALIBRATION_IMAGETYPES = {"masterdark", "masterflat", "bias", "dcurrent"}
SITE_CANDIDATES = ["auger2", "auger", "cta-n", "cta-s0", "cta-s1"]
DATE_DIR_RE = re.compile(r"^\d{8}$")
REQUIRED_METADATA_FIELDS = ("site", "ccd", "camera_serial", "binning")
FRAM_COMMUNITY = "fram"
FRAM_MODEL = "fram"
DEFAULT_ANCHOR = "data3"
DEFAULT_EXCLUDE_DIRS = ["bad"]

CREATORS = [
    {
        "person_or_org": {
            "name": "FZU Institute of Physics of the Czech Academy of Sciences",
            "type": "organizational",
        },
    }
]

CONTRIBUTORS = [
    {
        "person_or_org": {"name": "FRAM collaboration", "type": "organizational"},
        "role": {"id": "ResearchGroup"},
    }
]

SUBJECTS = [
    "Fram",
    "Telescope",
    "Astrophysics",
    "Auger",
    "CTA",
    "Photometric robotic atmosperic monitor",
    "Physics",
    "Paranal",
    "Roque de los Muchachos",
]

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
# METADATA EXTRACTION (from fram_upload.py)
# ============================================================


def _guess_site(path_str: str) -> str | None:
    for candidate in SITE_CANDIDATES:
        if candidate in path_str:
            return candidate
    return None


def _spherical_distance(ra1, dec1, ra2, dec2) -> float:
    ra1_rad, dec1_rad = np.radians(ra1), np.radians(dec1)
    ra2_rad, dec2_rad = np.radians(ra2), np.radians(dec2)
    dlat = dec2_rad - dec1_rad
    dlon = ra2_rad - ra1_rad
    a = np.sin(dlat / 2) ** 2 + np.cos(dec1_rad) * np.cos(dec2_rad) * np.sin(dlon / 2) ** 2
    return float(np.degrees(2 * np.arcsin(np.sqrt(a))))


def _ra_to_lon(ra: float) -> float:
    return ra - 360.0 if ra > 180.0 else ra


def _compute_footprint(wcs, usable_width, usable_height, dec0, radius):
    """GeoJSON footprint for OpenSearch geo_shape; envelope when a celestial
    pole is inside the FOV (see fram_upload.py for the rationale)."""
    try:
        px = [0, usable_width, usable_width, 0, 0]
        py = [0, 0, usable_height, usable_height, 0]
        ras, decs = wcs.all_pix2world(px, py, 0)
        coords = [[_ra_to_lon(float(ra)), float(dec)] for ra, dec in zip(ras, decs)]

        north = (90.0 - dec0) <= radius
        south = (dec0 + 90.0) <= radius
        if north or south:
            corner_decs = [c[1] for c in coords]
            lat_lo, lat_hi = (min(corner_decs), 90.0) if north else (-90.0, max(corner_decs))
            return {"type": "envelope", "coordinates": [[-180.0, lat_hi], [180.0, lat_lo]]}

        lons = [c[0] for c in coords]
        polygon = {"type": "Polygon", "coordinates": [coords]}
        if (max(lons) - min(lons)) > 180.0:
            polygon["orientation"] = "right"
        return polygon
    except Exception:
        return None


def _parse_iso_time(string: str) -> datetime.datetime:
    return datetime.datetime.strptime(string, "%Y-%m-%dT%H:%M:%S.%f")


def _get_night(time_, lon=None, site=None) -> str:
    if lon is None:
        if site == "auger":
            lon = -69.4497
        elif site == "cta-n":
            lon = -17.89
        elif site in ("cta-s0", "cta-s1"):
            lon = -70.32482
        else:
            lon = 0
    shifted = time_ + datetime.timedelta(seconds=lon * 86400 / 360 - 86400 / 2)
    return shifted.strftime("%Y%m%d")


def extract_metadata(fits_path: Path, key: str, site: str | None) -> dict:
    path_str = str(fits_path)
    header = fits.getheader(path_str, -1)

    time_ = _parse_iso_time(header["DATE-OBS"])
    if header.get("LONGITUD") is not None:
        night = _get_night(time_, lon=header["LONGITUD"])
    else:
        night = _get_night(time_, site=site)

    image = fits.getdata(path_str, -1)
    width, height = header["NAXIS1"], header["NAXIS2"]
    image, header = crop_overscans(image, header)
    usable_width, usable_height = image.shape[1], image.shape[0]

    obs_type = header.get("IMAGETYP", "unknown")
    is_science = obs_type == "object"
    is_calibration = obs_type in CALIBRATION_IMAGETYPES

    wcs = None
    if is_science and header.get("CTYPE1"):
        wcs = WCS(header)
        ra, dec = wcs.all_pix2world(
            [0, usable_width, 0.5 * usable_width],
            [0, usable_height, 0.5 * usable_height],
            0,
        )
        radius = 0.5 * _spherical_distance(ra[0], dec[0], ra[1], dec[1])
        ra0, dec0 = float(ra[2]), float(dec[2])
    else:
        ra0, dec0, radius = 0.0, 0.0, 0.0

    if is_calibration or ra0 == 0.0:
        center_geo = footprint = healpix_idx = None
    else:
        center_geo = {"lat": dec0, "lon": _ra_to_lon(ra0)}
        footprint = (
            _compute_footprint(wcs, usable_width, usable_height, dec0, radius)
            if wcs is not None
            else None
        )
        healpix_idx = int(hp.ang2pix(HEALPIX_NSIDE, np.radians(90.0 - dec0), np.radians(ra0)))

    target = header.get("TARGET")
    obj_name = header.get("OBJECT")
    target_display = f"{target} / {obj_name}" if target and obj_name else (target or obj_name)

    return {
        "key": key,
        "filename": key,
        "night": night,
        "observation_time": time_.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "creation_date": time_.date().isoformat(),
        "target": target_display,
        "type": obs_type,
        "filter": header.get("FILTER", "unknown"),
        "ccd": header.get("CCD_NAME"),
        "camera_serial": str(header["PRODUCT_ID"]) if header.get("PRODUCT_ID") is not None else None,
        "site": site,
        "ra0": ra0,
        "dec0": dec0,
        "radius": radius,
        "exposure": header.get("EXPOSURE"),
        "width": int(width),
        "height": int(height),
        "usable_width": int(usable_width),
        "usable_height": int(usable_height),
        "binning": header.get("BINNING"),
        "mean": float(np.mean(image)),
        "median": float(np.median(image)),
        "altitude": header.get("TEL_ALT"),
        "azimuth": header.get("TEL_AZ"),
        "footprint": footprint,
        "center_geo": center_geo,
        "healpix_idx": healpix_idx,
    }


def validate_metadata(extracted: dict) -> list[str]:
    problems = [f"missing required field: {f}" for f in REQUIRED_METADATA_FIELDS if not extracted.get(f)]
    if not extracted.get("usable_width") or not extracted.get("usable_height"):
        problems.append("usable_width/usable_height is zero or missing")
    return problems


def build_record(extracted: dict, resource: dict | None = None) -> dict:
    name = Path(extracted["filename"]).name
    title = (
        "FRAM_" + Path(extracted["site"]).stem + "_" + Path(extracted["ccd"]).stem
        + "_" + Path(extracted["filename"]).stem
    )
    return {
        "metadata": {
            "resource_type": {"id": "c_ddb1"},
            "creators": CREATORS,
            "contributors": CONTRIBUTORS,
            "file_types": ["fits"],
            "title": title,
            "publication_date": datetime.date.today().isoformat(),
            "publisher": "FZU Institute of Physics of the Czech Academy of Sciences",
            "additional_descriptions": [
                {
                    "lang": {"id": "ENG"},
                    "type": {"id": "abstract"},
                    "description": (
                        "This dataset contains observation data in the fits format. "
                        "The observation comes from robotic telescope "
                        "(Phototometric Robotic Atmospheric Monitor - FRAM)."
                    ),
                }
            ],
            "identifiers": [{"identifier": "", "scheme": "url"}],
            "related_resources": related_resources_for(resource),
            "subjects": [{"subject": s} for s in SUBJECTS],
            "rights": [{"id": "4-BY"}],
            "dates": [{"date": extracted["creation_date"], "type": {"id": "Created"}}],
            "experiment": {"id": "FRAM"},
            "target": extracted["target"],
            "type": extracted["type"],
            "observation_time": extracted["observation_time"],
            "observation_night": extracted["night"],
            "exposure": extracted["exposure"],
            "center": {"ra": extracted["ra0"], "dec": extracted["dec0"]},
            "radius": extracted["radius"],
            "site": extracted["site"],
            "ccd": extracted["ccd"],
            "camera_serial": extracted["camera_serial"],
            "filter": extracted["filter"],
            "binning": extracted["binning"],
            "image_size": {
                "width": extracted["width"],
                "height": extracted["height"],
                "usable_width": extracted["usable_width"],
                "usable_height": extracted["usable_height"],
            },
            "declination": extracted["dec0"],
            "alt_az": {"altitude": extracted["altitude"], "azimuth": extracted["azimuth"]},
            "filename": extracted["filename"],
            "footprint": extracted["footprint"],
            "center_geo": extracted["center_geo"],
            "healpix_idx": extracted["healpix_idx"],
        },
        "files": {
            "enabled": True,
            "entries": [{"key": name, "path": extracted["filename"]}],
        },
        "access": {
            "record": "public",
            "files": "public",
            "embargo": {"active": False, "reason": None},
        },
        "community": FRAM_COMMUNITY,
        "model": FRAM_MODEL,
    }


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
    """task = (fits_path, key, out_path, site_override, resource, missing).
    Returns (key, status, error); status in ok / invalid / error / no_resource."""
    fits_path, key, out_path, site_override, resource, missing = task
    try:
        if missing:
            return key, "no_resource", NO_RESOURCE_MSG
        site = site_override or _guess_site(fits_path)
        extracted = extract_metadata(Path(fits_path), key, site)
        problems = validate_metadata(extracted)
        if problems:
            return key, "invalid", "; ".join(problems)
        _write_json_atomic(Path(out_path), _clean(build_record(extracted, resource)))
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


def task_generator(args, root, out_dir, include, skip, table, counters):
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
        yield (str(fits_path), key, str(target), args.site, resource, missing)


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
    good = {"ok", "patched", "unchanged"}
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
        tasks = task_generator(args, root, out_dir, include, skip, table, counters)
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
    p.add_argument("--workers", type=int, default=1, help="Parallel worker processes (default: 1)")
    p.add_argument("--max-files", type=int, default=0,
                   help="Process at most N new files this run (0 = no limit)")
    p.add_argument("--overwrite", action="store_true", help="Regenerate JSON files that already exist")
    p.add_argument("--errors-csv", default=None, help="Path of errors CSV (default: <out>/errors.csv)")
    p.add_argument("--progress-interval", type=float, default=30.0, help="Seconds between progress lines")
    args = p.parse_args(argv)

    has_table = bool(args.site_year_resources or args.site_year_resource)
    if not args.patch_related_resources and not args.root:
        p.error("--root is required (except with --patch-related-resources)")
    if args.patch_related_resources and not has_table:
        p.error("--patch-related-resources needs --site-year-resources and/or --site-year-resource")
    if args.require_site_year_resource and not has_table:
        p.error("--require-site-year-resource needs --site-year-resources and/or --site-year-resource")
    return args


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sys.exit(run(args))


if __name__ == "__main__":
    main()
