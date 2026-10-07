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

COMMUNITY HANDLING: unlike SiPM's manual "communities" dict, FRAM sets a
"community" key in build_invenio_metadata() (the community slug, e.g.
"fram1"); async_upload.py's upload_record_async() passes it through as the
community= keyword argument of client.records.create() automatically
whenever an adapter sets one (see its "if metadata.get('community')"
check and the "COMMUNITY / WORKFLOW" section of its module docstring), so
no further engine change is needed. This mirrors the usage shown in
nrp_cmd's own guide, e.g.:
    record = await client.records.create(
        {...}, community="my-community", workflow="review"
    )
FRAM_COMMUNITY below is the community slug currently in use; update it if
Cesnet's community/access workflow for this model changes.

KNOWN OPEN ITEM: camera_serial is now read from the PRODUCT_ID header
keyword (a HIERARCH keyword in sample headers so far), matching the same
field calibrate.py's own find_calibration_config() already keys off of
internally for airtemp-based bias fallbacks and linearization curves --
this resolves the previous field-name mismatch between the two. What
remains open: if PRODUCT_ID is not resolvable on the header object
crop_overscans() receives -- e.g. because it's only present in the
primary HDU while extract_metadata() reads the header via
fits.getheader(path, -1), the *last* HDU -- find_calibration_config()
will raise a KeyError for any file whose overscan can't be measured
directly from the pixel data, and camera_serial in the output metadata
will be None rather than raising. Worth checking against a real file
before a production run.

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
import fnmatch
import logging
import os
import re
import warnings
from dataclasses import dataclass
from pathlib import Path

import healpy as hp
import numpy as np
from astropy.io import fits
from astropy.wcs import WCS, FITSFixedWarning

from calibrate import crop_overscans

warnings.simplefilter("ignore", FITSFixedWarning)

logger = logging.getLogger(__name__)

# ============================================================
# DEFAULTS (used by async_upload.py's smoke test; override via CLI flags
# for real runs)
# ============================================================

# FRAM has no separate metadata directory -- point this at the same root
# as DEFAULT_DATA_ROOT so bulk_async.py's "does metadata_dir exist?"
# preflight check passes trivially. discover_items() below ignores its
# metadata_dir argument entirely.
DEFAULT_DATA_ROOT = "/home/xy/FRAM/Upload/Data to upload"
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

# Matches FRAM's per-day folder naming convention (e.g. "20220417") at any
# depth under data_root, e.g. mnt/data3/cta-n/2022/20220417/03185/...
# Used by discover_items()'s --date-pattern filtering to identify which
# path component is the day-folder worth pruning against.
DATE_DIR_RE = re.compile(r"^\d{8}$")

# Env var fram_upload.py's own CLI wrapper (_run_via_bulk_async) sets from
# --date-pattern, read here by discover_items() since bulk_async.py's
# discover_items() call doesn't pass adapter-specific extra arguments (see
# module docstring's discussion of the shared interface). Not set at all
# means "no filtering, walk everything" -- the previous behavior.
DATE_PATTERN_ENV_VAR = "FRAM_DATE_PATTERN"

# Same mechanism as DATE_PATTERN_ENV_VAR, but for directory names to
# always skip regardless of depth -- e.g. a "bad" folder some FRAM sites
# use to quarantine known-bad frames that shouldn't be uploaded. Unlike
# the date pattern, this is NOT limited to 8-digit day-folders; it prunes
# any directory whose name matches, at any depth. Defaults to ["bad"] if
# neither --exclude-dirs nor the env var is set; pass --exclude-dirs ""
# explicitly to disable exclusion entirely.
EXCLUDE_DIRS_ENV_VAR = "FRAM_EXCLUDE_DIRS"
DEFAULT_EXCLUDE_DIRS = ["bad"]

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

# Community slug FRAM records are created under, passed to
# client.records.create() as the community= keyword argument (see
# "COMMUNITY HANDLING" in the module docstring above). Update if Cesnet
# changes the community/access workflow for this model.
FRAM_COMMUNITY = "fram"


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
    """Return the file path relative to the upload root, or after 'mnt' if that folder name appears in the path (matches the historic
    FRAM upload-tree convention, so site subfolders don't collide)."""
    if "mnt" in fits_path.parts:
        idx = fits_path.parts.index("mnt")
        return Path(*fits_path.parts[idx + 1:]).as_posix()
    return fits_path.relative_to(root).as_posix()


def _guess_site(path_str: str) -> str | None:
    for candidate in SITE_CANDIDATES:
        if candidate in path_str:
            return candidate
    return None


def _parse_date_patterns() -> list[str] | None:
    """Read FRAM_DATE_PATTERN (set by --date-pattern via the CLI wrapper
    below) as a list of comma-separated glob patterns, e.g.
    "202204*,202205*" -> ["202204*", "202205*"]. Returns None if the env
    var is unset/empty, meaning "no date filtering -- walk everything"."""
    raw = os.environ.get(DATE_PATTERN_ENV_VAR, "").strip()
    if not raw:
        return None
    patterns = [p.strip() for p in raw.split(",") if p.strip()]
    return patterns or None


def _parse_exclude_dirs() -> list[str]:
    """Read FRAM_EXCLUDE_DIRS (set by --exclude-dirs via the CLI wrapper
    below) as a list of comma-separated glob patterns matched against a
    directory's NAME (not its full path) at any depth. If the env var was
    never set at all, falls back to DEFAULT_EXCLUDE_DIRS (["bad"]). If it
    WAS set but to an empty string (--exclude-dirs "" explicitly passed),
    that's a deliberate opt-out -- returns an empty list, excluding
    nothing."""
    raw = os.environ.get(EXCLUDE_DIRS_ENV_VAR)
    if raw is None:
        return list(DEFAULT_EXCLUDE_DIRS)
    return [p.strip() for p in raw.split(",") if p.strip()]


def _name_matches_any_pattern(name: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(name, pattern) for pattern in patterns)


def discover_items(
    metadata_dir: Path, data_root: Path, readme_file: Path | None = None
) -> list[FramWorkItem]:
    """Recursively walk data_root for FITS files; each becomes its own
    work item. metadata_dir is accepted for interface compatibility with
    the other adapters but is not used -- FRAM has no separate metadata
    files, since each FITS file's own header is its metadata.

    readme_file is likewise accepted but unused for now -- FRAM's
    per-experiment README referencing scheme is still an open item).

    Two independent, os.walk-pruning filters (both read from env vars set
    by fram_upload.py's own CLI wrapper -- see --date-pattern and
    --exclude-dirs):

    - Date filtering: any directory whose name looks like an 8-digit date
      (matching DATE_DIR_RE, e.g. "20220417") and does NOT match at least
      one of the --date-pattern glob patterns is skipped entirely. Applies
      uniformly across every site under data_root (cta-n, auger2, ...),
      since the pattern is matched against the day-folder name alone, not
      the full path.
    - Name exclusion: any directory whose name matches one of the
      --exclude-dirs glob patterns (default: "bad") is skipped entirely,
      at ANY depth -- not limited to day-folders.

    Either filter pruning a directory means os.walk never descends into
    it, so the (often enormous) set of files under a skipped directory is
    never even touched -- this matters at FRAM's scale, where walking the
    full tree just to discard most of it afterward would be very slow.
    """
    date_patterns = _parse_date_patterns()
    exclude_patterns = _parse_exclude_dirs()
    if date_patterns:
        logger.info("Date filter active: %s (only matching YYYYMMDD day-folders will be walked)", date_patterns)
    if exclude_patterns:
        logger.info("Directory exclusion active: %s (matching directories are skipped at any depth)", exclude_patterns)

    items: list[FramWorkItem] = []
    log_every = 50_000
    found = 0
    pruned_dirs = 0
    for dirpath, dirnames, filenames in os.walk(data_root):
        if date_patterns or exclude_patterns:
            kept = []
            for name in dirnames:
                if exclude_patterns and _name_matches_any_pattern(name, exclude_patterns):
                    pruned_dirs += 1
                    continue
                if date_patterns and DATE_DIR_RE.match(name) and not _name_matches_any_pattern(name, date_patterns):
                    pruned_dirs += 1
                    continue
                kept.append(name)
            dirnames[:] = kept

        for name in filenames:
            if os.path.splitext(name)[1].lower() not in FITS_EXTENSIONS:
                continue
            fits_path = Path(dirpath) / name
            key = _relative_key(fits_path, data_root)
            items.append(FramWorkItem(key=key, fits_path=fits_path, site=_guess_site(str(fits_path))))
            found += 1
            if found % log_every == 0:
                print(f"Discovery in progress: {found} FITS files found so far...")
    if date_patterns or exclude_patterns:
        logger.info("Filters pruned %s non-matching/excluded directory(ies) before descending into them.", pruned_dirs)
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


def _compute_footprint(
    wcs, usable_width: int, usable_height: int, dec0: float, radius: float
) -> dict | None:
    """
    Return a GeoJSON shape describing the image footprint for OpenSearch
    geo_shape indexing, with RA remapped to -180..+180 deg.

    Populates the `footprint` metadata field. The original PostgreSQL
    schema had this as a POLYGON type for geo search; here we use a GeoJSON
    object so the Invenio/OpenSearch mapping can ingest it directly.

    Images near RA=0/360 deg straddle the antimeridian in the remapped
    system. Setting GeoJSON orientation='right' tells OpenSearch to take
    the short arc across the antimeridian rather than wrapping around the
    globe.

    BUG FIX (celestial-pole case): images whose field of view contains a
    celestial pole are a SEPARATE case from ordinary antimeridian-crossing.
    Near a pole, the 4 corner longitudes spread across nearly the full
    -180..+180 deg range (RA sweeps through all values over a tiny angular
    distance on the sky), so the naive 4-point Polygon ring built below is
    topologically invalid there regardless of `orientation` -- OpenSearch
    rejects it with a `mapper_parsing_exception` /
    "Unable to Tessellate shape. Possible malformed shape detected." error
    (confirmed against real production records -- e.g.
    20210901202536-251-RA.fits -- whose FOV contains the north celestial
    pole; this previously made every such record fail to upload entirely).
    In that case we instead emit a conservative bounding-box `envelope`
    shape (full longitude range, latitude from the FOV's near edge to the
    pole) -- verified to be accepted by OpenSearch's geo_shape mapping and
    to match `intersects` queries the same way a Polygon would, which is
    all the portal's cone-search round-2 containment check needs.

    `dec0`/`radius` are the FOV center Dec and half-diagonal angular radius
    (both already computed by the caller from the same WCS) -- a pole is
    considered inside the FOV whenever the center is within `radius`
    (great-circle distance) of it.

    Returns None on any WCS computation error.
    """
    try:
        px = [0, usable_width, usable_width, 0, 0]
        py = [0, 0, usable_height, usable_height, 0]
        ras, decs = wcs.all_pix2world(px, py, 0)

        coords = [[_ra_to_lon(float(ra)), float(dec)] for ra, dec in zip(ras, decs)]

        contains_north_pole = (90.0 - dec0) <= radius
        contains_south_pole = (dec0 - (-90.0)) <= radius
        if contains_north_pole or contains_south_pole:
            corner_decs = [c[1] for c in coords]
            if contains_north_pole:
                lat_lo, lat_hi = min(corner_decs), 90.0
            else:
                lat_lo, lat_hi = -90.0, max(corner_decs)
            return {"type": "envelope", "coordinates": [[-180.0, lat_hi], [180.0, lat_lo]]}

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
        footprint = (
            _compute_footprint(wcs, usable_width, usable_height, dec0, radius)
            if wcs is not None
            else None
        )
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
        # Sourced from PRODUCT_ID, matching calibrate.py's own internal
        # find_calibration_config() lookup key -- see the KNOWN OPEN ITEM
        # note at the top of this file for the remaining caveat this
        # doesn't resolve (PRODUCT_ID's resolvability on the header object
        # itself, not which field name to use).
        "camera_serial": (
            str(header["PRODUCT_ID"]) if header.get("PRODUCT_ID") is not None else None
        ),
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
    title = "FRAM_" + Path(extracted["site"]).stem + "_" + Path(extracted["ccd"]).stem + "_" + Path(extracted["filename"]).stem
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
            "identifiers": [{"identifier": "", "scheme": "url"}],
            "related_resources": [{"title": "FRAM_2024_auger", "identifiers": [{"identifier": "https://doi.org/10.83100/j3vr-c838", "scheme": "url"}], "relation_type": {"id": "IsPartOf"}},
                                  {"title": "FRAM_FZU_root", "identifiers": [{"identifier": "https://doi.org/10.83100/ddxy-p647", "scheme": "url"}], "relation_type": {"id": "IsPartOf"}}],
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
        "files": {"enabled": True},
        "access": {
            "record": "public",
            "files": "public",
            # Real Python types, not the string "false"/"null" this used to
            # send. Confirmed against an actual published test1 record
            # (hg8m6-w2179): Invenio coerced the string "false" into a real
            # boolean false for "active" on its own, but "reason" stayed the
            # literal string "null" rather than real JSON null -- sending
            # native types here removes the dependency on that coercion
            # happening at all, and fixes "reason" outright.
            "embargo": {"active": False, "reason": None}
        },
        # Passed through by async_upload.py as client.records.create()'s
        # community= keyword argument -- see "COMMUNITY HANDLING" in the
        # module docstring. This is the ONLY mechanism that actually works
        # for FRAM: a top-level "communities" key on the record body (the
        # previous approach here, "communities": [{"slug": FRAM_COMMUNITY}])
        # is not a field Invenio's record schema recognizes at that level --
        # community membership lives under parent.communities, which only
        # community= (via nrp_cmd's own create()) actually populates.
        # Confirmed by inspecting an actual published test1 record: sending
        # the old "communities" key produced parent.communities == {} (no
        # community at all), despite sending it.
        "community": FRAM_COMMUNITY,
        # Metadata-model routing kwarg, per Cesnet's own nrp_cmd usage
        # example (client.records.create(metadata, model="particles")) --
        # async_upload.py passes this through automatically as
        # client.records.create()'s model= keyword argument whenever it's
        # present. Confirmed working against a real test1 record (its
        # "$schema" resolved server-side from this, matching
        # DEFAULT_SCHEMA_URL, even with --disable-schema sending no
        # "$schema" at all).
        "model": "fram",
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


def _extract_flag_value(argv: list[str], flag: str) -> str | None:
    """Return the value passed for `flag` (either "--flag value" or
    "--flag=value" form) in `argv`, or None if it's absent. If the flag is
    repeated, the last occurrence wins, matching argparse's own behaviour.
    """
    value = None
    for i, arg in enumerate(argv):
        if arg == flag and i + 1 < len(argv):
            value = argv[i + 1]
        elif arg.startswith(flag + "="):
            value = arg.split("=", 1)[1]
    return value


def _strip_flag(argv: list[str], flag: str) -> list[str]:
    """Return argv with every occurrence of `flag` (and its value, in
    either "--flag value" or "--flag=value" form) removed. Needed for
    FRAM-only flags (--date-pattern, --exclude-dirs) that bulk_async.py's
    argparser doesn't know about -- forwarding them unstripped would make
    it error out with "unrecognized arguments" before ever reaching
    discover_items(), which is what actually consumes them (via env var,
    set just below)."""
    result: list[str] = []
    skip_next = False
    for arg in argv:
        if skip_next:
            skip_next = False
            continue
        if arg == flag:
            skip_next = True
            continue
        if arg.startswith(flag + "="):
            continue
        result.append(arg)
    return result


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

    # By convention (see module docstring), --metadata-dir must always
    # mirror --data-root for FRAM -- there's no real metadata directory,
    # so bulk_async.py's "does metadata_dir exist?" preflight check is
    # trivially satisfied by pointing it at data_root. DEFAULT_METADATA_DIR
    # only mirrors DEFAULT_DATA_ROOT at *module import time*, though, so if
    # the caller overrides --data-root on the CLI (e.g. to point at the
    # real server path instead of the intentionally-bogus default), that
    # override must also be picked up here for --metadata-dir; otherwise
    # argparse's last-flag-wins behaviour leaves --metadata-dir pinned to
    # the stale/bogus DEFAULT_METADATA_DIR while --data-root itself is
    # correctly overridden -- which is exactly the "default data root does
    # not exist" error this fixes.
    user_data_root = _extract_flag_value(sys.argv[1:], "--data-root")
    effective_data_root = user_data_root or DEFAULT_DATA_ROOT

    # FRAM-only flags -- not part of bulk_async.py's generic argparser, so
    # they're pulled out of sys.argv here (via env var, read by
    # discover_items() in this module) and stripped before forwarding the
    # rest to bulk_async.main(). See discover_items()'s docstring for what
    # each actually does.
    #
    # --date-pattern "202204*,202205*": comma-separated glob patterns
    # matched against YYYYMMDD day-folder names; only matching days are
    # walked. Omit entirely to upload everything (previous behavior).
    #
    # --exclude-dirs "bad,other_name": comma-separated glob patterns
    # matched against any directory NAME, at any depth, that should never
    # be descended into. Defaults to "bad" if the flag is never passed at
    # all; pass --exclude-dirs "" explicitly to disable exclusion.
    argv_rest = sys.argv[1:]
    date_pattern_value = _extract_flag_value(argv_rest, "--date-pattern")
    if date_pattern_value is not None:
        os.environ["FRAM_DATE_PATTERN"] = date_pattern_value
        argv_rest = _strip_flag(argv_rest, "--date-pattern")

    exclude_dirs_value = _extract_flag_value(argv_rest, "--exclude-dirs")
    if exclude_dirs_value is not None:
        os.environ["FRAM_EXCLUDE_DIRS"] = exclude_dirs_value
        argv_rest = _strip_flag(argv_rest, "--exclude-dirs")

    default_flags = {
        "--adapter": ADAPTER_NAME,
        "--metadata-dir": effective_data_root,
        "--data-root": DEFAULT_DATA_ROOT,
    }
    if DEFAULT_README_FILE:
        default_flags["--readme-file"] = DEFAULT_README_FILE

    injected = []
    for flag, value in default_flags.items():
        injected += [flag, value]

    sys.argv = [sys.argv[0]] + injected + argv_rest
    bulk_async.main()


if __name__ == "__main__":
    _run_via_bulk_async()

#   cd upload/invenio/fram
#   python3 fram_upload.py --environment local --data-root "/home/xyx/Python WSL/Archive/v1/FRAM/Upload/Data to upload/mnt/data3/cta-n/2021/20210409/03185" --dry-run
#   python3 fram_upload.py --environment production --max-concurrency 4
#
# --date-pattern and --exclude-dirs are FRAM-only flags, hand-parsed out
# of sys.argv by _run_via_bulk_async() above (via env var) rather than
# registered with bulk_async.py's argparser -- they will NOT show up in
# `python3 fram_upload.py --help`, only here and in discover_items()'s
# docstring.
#
#   # Upload only April + May 2022 data, across every site under data_root:
#   python3 fram_upload.py --environment test1 --data-root "..." --date-pattern "202204*,202205*"
#
#   # Default already skips any directory literally named "bad" at any depth;
#   # add more names/patterns (comma-separated, replaces the default entirely):
#   python3 fram_upload.py --environment test1 --data-root "..." --exclude-dirs "bad,quarantine,test_*"
#
#   # Explicitly disable exclusion (upload everything, including "bad" folders):
#   python3 fram_upload.py --environment test1 --data-root "..." --exclude-dirs ""
