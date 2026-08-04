"""
FRAM (robotic telescope network) adapter for the generic async/bulk upload
pipeline (async_upload.py / bulk_async.py).

This replaces the FRAM-specific fram_async_upload.py + fram_bulk_async.py
pair with a thin adapter that plugs into the same shared engine used by
itk_upload.py / sipm_upload.py / delphi_upload.py. All FITS-specific logic
(recursive discovery, header extraction, overscan cropping via
calibrate.py, spherical-index/footprint computation) now lives here;
async_upload.py and bulk_async.py are unchanged.

KEY DIFFERENCE FROM itk_upload.py / sipm_upload.py: FRAM has no *.txt
metadata files at all -- there's exactly one FITS file per record, and its
own header *is* the metadata. So unlike the other adapters, this one does
not really use `metadata_dir`: discover_items() walks `data_root`
recursively for FITS files instead of pairing txt files with data folders.
`metadata_dir` is still accepted (required by the shared discover_items()
interface, and bulk_async.py's preflight checks that it exists), but it is
otherwise ignored by this adapter. By convention, point --metadata-dir at
the same path as --data-root when running fram_upload.py (see
DEFAULT_METADATA_DIR below and the CLI usage notes at the bottom of this
file) so the preflight directory-exists check is trivially satisfied.

Every FITS file (light frame, masterdark, masterflat, bias, dcurrent, ...)
is uploaded as its own independent record. Calibration frames are NOT
linked to light frames at upload time -- that association is done at read
time by the portal, via metadata lookup (site/ccd/camera_serial/binning/
usable_width/usable_height + type-specific fields + nearest timestamp).
This means every record's metadata must carry those fields consistently,
regardless of observation type -- see REQUIRED_METADATA_FIELDS and
validate_metadata() below.

Overscan cropping and bias subtraction (via calibrate.crop_overscans) are
applied when computing metadata (usable dimensions, mean, median).
Linearization is intentionally NOT applied -- mean/median reflect crop+bias
only. The archived FITS file itself is always the untouched original
either way.

COMMUNITY HANDLING: like SiPM, FRAM sets a "communities" block in
build_invenio_metadata(); async_upload.py's upload_record_async() includes
it in the records.create() payload automatically whenever an adapter sets
one (see its "if metadata.get('communities')" check), so no engine change
is needed. FRAM_COMMUNITY_IDS below is a draft placeholder -- update it
once Cesnet finalizes the community/access workflow for this model.

KNOWN OPEN ITEM: camera_serial is read from the CCD_SER header keyword.
calibrate.py's own find_calibration_config() internally keys off a
DIFFERENT header field, header['product_id'] (seen as a HIERARCH keyword
in sample headers so far), to look up airtemp-based bias fallbacks and
linearization curves. That lookup is independent of what we put in the
output metadata, but if header['product_id'] is not resolvable on the
header object crop_overscans() receives -- e.g. because it's only present
in the primary HDU while extract_metadata() reads the header via
fits.getheader(path, -1), the *last* HDU -- find_calibration_config() will
raise a KeyError for any file whose overscan can't be measured directly
from the pixel data. Worth checking against a real file before a
production run. Left unresolved here, same as in the previous
fram_async_upload.py.

Which adapter is active is resolved at runtime by adapters.py, not
hardcoded in async_upload.py/bulk_async.py: this file passes --adapter
fram to bulk_async.py's CLI (and sets the PHYSICS_ADAPTER env var as a
fallback) so `python3 fram_upload.py ...` just works, including
side-by-side with `python3 sipm_upload.py ...` / `python3 itk_upload.py
...` in separate processes. See adapters.py for the full mechanism.

Token handling mirrors ITk/SiPM: nothing is hardcoded here.
bulk_async.py / async_upload.py resolve it the same way
(--token flag -> INVENIO_TOKEN env var -> interactive prompt).
"""

from __future__ import annotations

import datetime
import os
import warnings
from dataclasses import dataclass
from pathlib import Path

import healpy as hp
import numpy as np
from astropy.io import fits
from astropy.wcs import WCS, FITSFixedWarning

from calibrate import crop_overscans

warnings.simplefilter("ignore", FITSFixedWarning)

# ============================================================
# DEFAULTS (used by async_upload.py's smoke test; override via CLI flags
# for real runs)
# ============================================================

# FRAM has no separate metadata directory -- point this at the same root
# as DEFAULT_DATA_ROOT so bulk_async.py's "does metadata_dir exist?"
# preflight check passes trivially. discover_items() below ignores its
# metadata_dir argument entirely.
DEFAULT_DATA_ROOT = "/home/[XX]/Python WSL/FRAM/Upload/Data to upload"
DEFAULT_METADATA_DIR = DEFAULT_DATA_ROOT
DEFAULT_README_FILE = None  # no shared README wired up yet for FRAM (open item)

# Must match this adapter's key in adapters.ADAPTERS.
ADAPTER_NAME = "fram"

# Confirmed for local only so far; verify before using with test1/production
# (same caveat as ITk's / SiPM's DEFAULT_SCHEMA_URL).
DEFAULT_SCHEMA_URL = "local://fram-v1.0.0.json"

FITS_EXTENSIONS = (".fits", ".fit", ".fts")

HEALPIX_NSIDE = 64  # ~0.9 degree cell resolution; 49,152 total pixels

# Master-calibration / non-science IMAGETYP values; these are uploaded as
# their own records too. Kept here only for reference -- no filtering is
# applied based on this set.
CALIBRATION_IMAGETYPES = {"masterdark", "masterflat", "bias", "dcurrent"}

SITE_CANDIDATES = ["auger2", "auger", "cta-n", "cta-s0", "cta-s1"]

# Fields every record must have a usable value for, regardless of observation
# type, since these drive read-time calibration association. NOTE: "target"
# is intentionally NOT required -- calibration frames (masterdark/masterflat/
# bias/dcurrent) legitimately have no astronomical target. This is a minimal
# defensive check, not a substitute for the separate data-cleaning/correction
# tool planned as its own project.
REQUIRED_METADATA_FIELDS = ("site", "ccd", "camera_serial", "binning")


# ============================================================
# FIXED METADATA
# ============================================================

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
        "person_or_org": {
            "name": "FRAM collaboration",
            "type": "organizational",
        },
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

# Draft placeholder -- community/access workflow not finalized yet (see
# Physics repository info.docx: "We wait for Cesnet to provide community
# and access workflows"). Update once available, same pattern as SiPM's
# own community-id constant in sipm_upload.py.
FRAM_COMMUNITY_IDS = ["FRAM"]


# ============================================================
# WORK ITEM
# ============================================================


@dataclass
class FramWorkItem:
    key: str          # resume/dedup key -- FITS path relative to data_root
    fits_path: Path   # absolute path to the FITS file
    site: str | None  # guessed from the path, if a known site name appears in it


# ============================================================
# DISCOVERY
# ============================================================


def _relative_key(fits_path: Path, root: Path) -> str:
    """Return the file path relative to the upload root, or after 'Data to
    upload' if that folder name appears in the path (matches the historic
    FRAM upload-tree convention, so site subfolders don't collide)."""
    if "Data to upload" in fits_path.parts:
        idx = fits_path.parts.index("Data to upload")
        return Path(*fits_path.parts[idx + 1:]).as_posix()
    return fits_path.relative_to(root).as_posix()


def _guess_site(path_str: str) -> str | None:
    for candidate in SITE_CANDIDATES:
        if candidate in path_str:
            return candidate
    return None


def discover_items(
    metadata_dir: Path, data_root: Path, readme_file: Path | None = None
) -> list[FramWorkItem]:
    """Recursively walk data_root for FITS files; each becomes its own
    work item. metadata_dir is accepted for interface compatibility with
    the other adapters but is not used -- FRAM has no separate metadata
    files, since each FITS file's own header is its metadata.

    readme_file is likewise accepted but unused for now -- FRAM's
    per-experiment README referencing scheme is still an open item (see
    Physics repository info.docx).
    """
    items: list[FramWorkItem] = []
    log_every = 50_000
    found = 0
    for dirpath, _dirnames, filenames in os.walk(data_root):
        for name in filenames:
            if os.path.splitext(name)[1].lower() not in FITS_EXTENSIONS:
                continue
            fits_path = Path(dirpath) / name
            key = _relative_key(fits_path, data_root)
            items.append(FramWorkItem(key=key, fits_path=fits_path, site=_guess_site(str(fits_path))))
            found += 1
            if found % log_every == 0:
                print(f"Discovery in progress: {found} FITS files found so far...")
    items.sort(key=lambda it: it.key)
    return items


# ============================================================
# HELPERS (spherical geometry / night computation)
# ============================================================


def _spherical_distance(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
    """Great circle distance between two points on a sphere (in degrees)."""
    ra1_rad, dec1_rad = np.radians(ra1), np.radians(dec1)
    ra2_rad, dec2_rad = np.radians(ra2), np.radians(dec2)
    dlat = dec2_rad - dec1_rad
    dlon = ra2_rad - ra1_rad
    a = np.sin(dlat / 2) ** 2 + np.cos(dec1_rad) * np.cos(dec2_rad) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return np.degrees(c)


def _ra_to_lon(ra: float) -> float:
    """Remap RA (0-360 deg) to OpenSearch longitude (-180 to +180 deg)."""
    return ra - 360.0 if ra > 180.0 else ra


def _compute_footprint(wcs, usable_width: int, usable_height: int) -> dict | None:
    """
    Return a GeoJSON Polygon object describing the image corners for
    OpenSearch geo_shape indexing, with RA remapped to -180..+180 deg.

    Populates the `footprint` metadata field. The original PostgreSQL
    schema had this as a POLYGON type for geo search; here we use a GeoJSON
    object so the Invenio/OpenSearch mapping can ingest it directly.

    Images near RA=0/360 deg straddle the antimeridian in the remapped
    system. Setting GeoJSON orientation='right' tells OpenSearch to take
    the short arc across the antimeridian rather than wrapping around the
    globe.

    Returns None on any WCS computation error.
    """
    try:
        px = [0, usable_width, usable_width, 0, 0]
        py = [0, 0, usable_height, usable_height, 0]
        ras, decs = wcs.all_pix2world(px, py, 0)

        coords = [[_ra_to_lon(float(ra)), float(dec)] for ra, dec in zip(ras, decs)]

        lons = [c[0] for c in coords]
        crosses_antimeridian = (max(lons) - min(lons)) > 180.0

        polygon: dict = {"type": "Polygon", "coordinates": [coords]}
        if crosses_antimeridian:
            polygon["orientation"] = "right"
        return polygon
    except Exception:
        return None


def _parse_iso_time(string: str) -> datetime.datetime:
    return datetime.datetime.strptime(string, "%Y-%m-%dT%H:%M:%S.%f")


def _get_night(time_: datetime.datetime, lon: float | None = None, site: str | None = None) -> str:
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


# ============================================================
# METADATA EXTRACTION
# ============================================================


def extract_metadata(item: FramWorkItem) -> dict:
    """
    Extract metadata from a single FITS file's header, returning a flat
    dict. Kept as a sync function since astropy I/O is not async-native --
    async_upload.py runs this via asyncio.to_thread.

    No filtering is applied here: every IMAGETYP (object, masterdark,
    masterflat, bias, dcurrent, ...) is extracted and returned, since each
    becomes its own record. See validate_metadata() for the minimal
    downstream sanity check.

    Applies overscan cropping + bias subtraction (calibrate.crop_overscans)
    but NOT linearization -- usable_width/usable_height/mean/median reflect
    crop+bias only. The uploaded FITS file itself is always the untouched
    original; this only affects computed metadata.
    """
    path_str = str(item.fits_path)
    site = item.site

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

    # Spherical index fields -- always None for calibration frames or when
    # no valid sky position is available (ra0==0 acts as the sentinel).
    if is_calibration or ra0 == 0.0:
        center_geo = None
        footprint = None
        healpix_idx = None
    else:
        center_geo = {"lat": dec0, "lon": _ra_to_lon(ra0)}
        footprint = _compute_footprint(wcs, usable_width, usable_height) if wcs is not None else None
        theta = np.radians(90.0 - dec0)  # HEALPix co-latitude
        phi = np.radians(ra0)
        healpix_idx = int(hp.ang2pix(HEALPIX_NSIDE, theta, phi))

    target = header.get("TARGET")
    obj_name = header.get("OBJECT")
    target_display = f"{target} / {obj_name}" if target and obj_name else (target or obj_name)

    return {
        "key": item.key,
        "filename": item.key,
        "night": night,
        "observation_time": time_.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "creation_date": time_.date().isoformat(),
        "target": target_display,
        "type": obs_type,
        "filter": header.get("FILTER", "unknown"),
        "ccd": header.get("CCD_NAME"),
        # Sourced from CCD_SER, not header['product_id'] -- see the KNOWN
        # OPEN ITEM note at the top of this file.
        "camera_serial": header.get("CCD_SER"),
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
    """Return a list of problems with extracted FITS metadata. An empty
    list means the record is fine to upload.

    This is a minimal defensive check -- it exists to stop a handful of
    malformed headers from crashing the batch or silently uploading records
    with null fields the read-time association logic depends on (site/ccd/
    camera_serial/binning/usable dimensions). It is NOT a substitute for
    the more thorough corruption-detection/correction tool planned as a
    separate project.
    """
    problems = []
    for field in REQUIRED_METADATA_FIELDS:
        if not extracted.get(field):
            problems.append(f"missing required field: {field}")
    if not extracted.get("usable_width") or not extracted.get("usable_height"):
        problems.append("usable_width/usable_height is zero or missing")
    return problems


# ============================================================
# METADATA -> INVENIORDM JSON
# ============================================================


def build_invenio_metadata(extracted: dict) -> dict:
    title = "FRAM_" + Path(extracted["filename"]).stem
    publication_date = datetime.date.today().isoformat()

    return {
        "metadata": {
            "resource_type": {"id": "c_ddb1"},
            "creators": CREATORS,
            "contributors": CONTRIBUTORS,
            "file_types": ["fits"],
            "title": title,
            "publication_date": publication_date,
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
            "alt_az": {
                "altitude": extracted["altitude"],
                "azimuth": extracted["azimuth"],
            },
            "filename": extracted["filename"],
            "footprint": extracted["footprint"],
            "center_geo": extracted["center_geo"],
            "healpix_idx": extracted["healpix_idx"],
        },
        "files": {"enabled": False},
        "access": {
            "record": "public",
            "files": "restricted",
            "embargo": {"active": "false", "reason": "null"},
            "status": "restricted",
        },
        # Draft placeholder -- see FRAM_COMMUNITY_IDS above. async_upload.py
        # includes this in the records.create() payload automatically.
        "communities": {"ids": FRAM_COMMUNITY_IDS},
    }


# ============================================================
# FILES TO UPLOAD PER RECORD
# ============================================================


def get_upload_files(item: FramWorkItem, extracted: dict) -> list[tuple[str, Path, str]]:
    """FRAM uploads exactly one FITS file per record -- no zip / multi-file
    branch, unlike SiPM's tray-folder case."""
    return [(item.fits_path.name, item.fits_path, "Measurement data")]


# ============================================================
# CLI ENTRY POINT
#
# bulk_async.py itself stays fully generic -- it always requires
# --metadata-dir/--data-root explicitly and knows nothing about FRAM's
# default paths. Running it directly means typing those out every time.
# It also errors out if run directly at all (see its __main__ guard) --
# this file is the actual entry point.
#
# Assumed layout: async_upload.py / bulk_async.py (the shared engine) live
# one directory up from this adapter, not alongside it, e.g.:
#     Upload/async_upload.py
#     Upload/bulk_async.py
#     Upload/FRAM/fram_upload.py   <- this file
# If that's not where they end up, adjust ENGINE_DIR below.
#
#   python3 fram_upload.py --environment local
#   python3 fram_upload.py --environment test1 --dry-run
#   python3 fram_upload.py --environment local --data-root "/some/other/Data to upload"
#
# --metadata-dir is not meaningful for FRAM (see module docstring) but is
# still injected below (pointing at the same path as --data-root) purely
# to satisfy bulk_async.py's generic "does metadata_dir exist?" preflight
# check; discover_items() above ignores its value.
#
# async_upload.py/bulk_async.py need no edits to run this adapter --
# --adapter fram (injected below) and the PHYSICS_ADAPTER env var both
# resolve to this module via adapters.py.
# ============================================================

ENGINE_DIR = Path(__file__).resolve().parent.parent


def _run_via_bulk_async() -> None:
    import importlib
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


#   cd upload/invenio/fram
#   python3 fram_upload.py --environment local --data-root "/home/erutherford/Python WSL/FRAM/Upload/Data to upload/cta-n/2021/20210409/03185" --dry-run
#   python3 fram_upload.py --environment production --max-concurrency 4
