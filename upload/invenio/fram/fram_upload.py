"""
FRAM (robotic telescope network) adapter for the generic async/bulk upload
pipeline (async_upload.py / bulk_async.py).

Every FITS file (light frame, masterdark, masterflat, bias, dcurrent, ...)
is uploaded as its own record, and its header is the source of the metadata.
discover_items() walks the data root recursively for FITS files. The shared
engine requires a `metadata_dir`; FRAM has no separate metadata files, so it is
accepted but ignored, and --metadata-dir is set to the same path as --data-root
automatically.

Calibration frames are not linked to light frames at upload time. The
association is made at read time by a metadata lookup (site, ccd,
camera_serial, binning, usable_width/usable_height, type-specific fields and
the nearest timestamp), so every record carries these fields consistently,
regardless of observation type -- see REQUIRED_METADATA_FIELDS and
validate_metadata().

Overscan cropping (calibrate.crop_overscans, without bias subtraction and
without linearization) is applied when computing metadata (usable dimensions,
mean, median, ...). mean/median therefore match the values of the FRAM
archive; the overscan bias is stored separately as bias_level. The uploaded
FITS file itself is always the untouched original.

COMMUNITY: build_invenio_metadata() sets a "community" key (the community
slug). async_upload.py passes it to client.records.create() as the community=
keyword argument, for example:
    record = await client.records.create(
        {...}, community="my-community", workflow="review"
    )

CAMERA SERIAL: camera_serial is read from the PRODUCT_ID header keyword
(HIERARCH, in the image HDU), the same key calibrate.py's
find_calibration_config() uses. fits.getheader(path, -1) returns the image
header for both tile-compressed files (empty primary HDU) and plain files.

QUALITY / REJECTION: extract_metadata() does not raise for a bad file; it
returns "problems" entries starting with a stable E_* code (see
validate_metadata()), which end up in the "error" column of upload_stats.csv
with status=skipped_invalid. Star-based image quality (FWHM) needs the optional
'sep' package and can be disabled with --no-quality.

The active adapter is resolved at runtime by adapters.py: this file passes
--adapter fram to bulk_async.py (and sets the PHYSICS_ADAPTER environment
variable as a fallback), so `python3 fram_upload.py ...` works directly.

The API token is not stored in this file. bulk_async.py / async_upload.py
resolve it from the --token flag, the INVENIO_TOKEN environment variable or an
interactive prompt, in that order.
"""

from __future__ import annotations

import datetime
import fnmatch
import logging
import math
import os
import re
import threading
import warnings
from dataclasses import dataclass
from pathlib import Path

import healpy as hp
import numpy as np
from astropy.io import fits
from astropy.wcs import WCS, FITSFixedWarning

from calibrate import crop_overscans

# OPTIONAL dependency (FWHM / star counts only). If it is not installed the
# image_quality fields are simply left empty and everything else works.
try:
    import sep
    sep.set_extract_pixstack(2_000_000)  # crowded wide-field frames overflow the default 300k
except ImportError:  # pragma: no cover
    sep = None

warnings.simplefilter("ignore", FITSFixedWarning)

logger = logging.getLogger(__name__)

# ============================================================
# DEFAULTS (used by async_upload.py's smoke test; override via CLI flags
# for real runs)
# ============================================================

# FRAM has no separate metadata directory. DEFAULT_METADATA_DIR mirrors the data
# root so that bulk_async.py's "does metadata_dir exist?" check is satisfied;
# discover_items() ignores it.
DEFAULT_DATA_ROOT = "/path/to/fram/data"
DEFAULT_METADATA_DIR = DEFAULT_DATA_ROOT
DEFAULT_README_FILE = None  # no shared README file for FRAM

# Must match this adapter's key in adapters.ADAPTERS.
ADAPTER_NAME = "fram"

# Sent only with --enable-schema (the "$schema" key is omitted by default).
DEFAULT_SCHEMA_URL = "local://fram-v1.0.0.json"

FITS_EXTENSIONS = (".fits", ".fit", ".fts")

HEALPIX_NSIDE = 64  # ~0.9 degree cell resolution; 49,152 total pixels

# Master-calibration / non-science IMAGETYP values; these are uploaded as
# their own records too. Kept here only for reference -- no filtering is
# applied based on this set.
CALIBRATION_IMAGETYPES = {
    "masterdark", "masterflat", "bias", "dcurrent",  # master / derived frames
    "dark", "flat", "zero", "flat1",                  # raw calibration exposures
}

# Already-processed calibration products. They are stored cropped, with bias
# already removed (masterdark) or normalised (masterflat ~ 1.0), and their
# AVERAGE/STDEV header keywords are stale copies from the raw inputs. They must
# NOT go through overscan crop / bias subtraction -- doing so produced negative
# means (-474, -420) on real examples. The portal likewise displays them as stored.
MASTER_IMAGETYPES = {"masterdark", "masterflat", "bias", "dcurrent"}

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
# Quality measurement (sep). --no-quality (fram_upload.py CLI) sets the env var.
QUALITY_DISABLED_ENV_VAR = "FRAM_NO_QUALITY"
MIN_STARS_FOR_FWHM = 20     # fewer clean stars -> fwhm left empty (cloudy / empty field)
SEP_DETECT_SIGMA = 5.0
# FWHM is measured on the central window of at most this many pixels per side (0 = whole
# frame). Star detection dominates the per-frame cost (55-80% of it on science frames), and
# on real frames a 2048 window is 3-6x faster while the median FWHM stays within ~5% of the
# whole-frame value. The centre is usually the best-focused part of the field, so it reads
# slightly optimistic -- fine as a RELATIVE quality indicator. Frames up to 2048 px are
# unaffected. Set via --quality-size (fram_upload.py CLI) / FRAM_QUALITY_SIZE.
QUALITY_SIZE_ENV_VAR = "FRAM_QUALITY_SIZE"
DEFAULT_QUALITY_SIZE = 2048
SEP_MAX_PEAK = 40000        # ADU (bias included); excludes (near-)saturated stars of 16-bit cameras

# Plausibility limits for validate_metadata().
MIN_PLAUSIBLE_YEAR = 2005   # FRAM has been observing since 2007

# Approximate observatory longitudes (deg E), used ONLY for a warning when the
# site guessed from the file path disagrees with the header's LONGITUD.
# auger2 is assumed to have the same longitude as auger.
SITE_LONGITUDES = {"cta-n": -17.89, "auger": -69.45, "auger2": -69.45, "cta-s0": -70.32, "cta-s1": -70.32}

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
    "FRAM",
    "Telescope",
    "Astrophysics",
    "Auger",
    "CTA",
    "Photometric robotic atmospheric monitor",
    "Physics",
    "Paranal",
    "Roque de los Muchachos",
]

# Community slug of FRAM records, passed to client.records.create() as the
# community= keyword argument (see COMMUNITY in the module docstring).
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
    work item. metadata_dir is accepted for interface compatibility but is
    not used: each FITS file's own header is its metadata. readme_file is
    accepted for the same reason and is not used.

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
    if sep is None:
        logger.warning("'sep' is not installed -- fwhm_* fields will be left empty. pip install sep to enable.")
    elif os.environ.get(QUALITY_DISABLED_ENV_VAR):
        logger.info("Quality measurement disabled (--no-quality).")
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

    Celestial poles: when the field of view contains a pole, the four corner
    longitudes spread across nearly the whole -180..+180 deg range, so a
    four-point Polygon ring is topologically invalid regardless of
    `orientation` and OpenSearch rejects it ("Unable to Tessellate shape"). In
    that case a conservative bounding-box `envelope` is returned instead (full
    longitude range, latitude from the near edge of the field to the pole). It
    is accepted by the geo_shape mapping and matches `intersects` queries like
    a Polygon would.

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
    """DATE-OBS, with or without fractional seconds."""
    string = str(string).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.datetime.strptime(string, fmt)
        except ValueError:
            continue
    raise ValueError(f"unrecognised DATE-OBS format: {string!r}")


def _get_night(time_: datetime.datetime, lon: float | None = None, site: str | None = None) -> str:
    if lon is None:
        if site in ("auger", "auger2"):
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


def _num(header, key: str, lo: float | None = None, hi: float | None = None) -> float | None:
    """Header value as a finite float, or None when the keyword is missing,
    non-numeric, NaN/inf, or outside the plausible range [lo, hi].

    Out-of-range -> None (not clamped) is deliberate: several FRAM headers
    carry known-bad values (G2 TEMPPWR=39426 labelled "[%]", MOONDIST=-9e-36,
    ...), and a wrong number in the archive is worse than an empty field."""
    value = header.get(key)
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    if lo is not None and number < lo:
        return None
    if hi is not None and number > hi:
        return None
    return number


def _is_transient(exc: BaseException) -> bool:
    """True for errors that say nothing about the FILE itself: real I/O errors
    from the OS (OSError with an errno: input/output error, timeout, stale NFS
    handle, missing file, ...) and memory pressure. These are re-raised so the
    engine records status=failed and retries them (also on the next run).
    Turning them into skipped_invalid would be wrong, because that status is
    TERMINAL in bulk_async.py: the file would never be retried. Format
    problems (corrupt/truncated/not FITS) carry no errno and stay E_* skips."""
    return isinstance(exc, MemoryError) or (isinstance(exc, OSError) and exc.errno is not None)


def _str(header, key: str) -> str | None:
    value = header.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _finite(value) -> float | None:
    """numpy/py number -> float, or None if NaN/inf (NaN is not valid JSON)."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def header_fields(header, is_master: bool = False) -> dict:
    """Header-derived fields, sanitised. SHARED between the FITS path
    (_extract_metadata) and convert_dump.py (which feeds it the archive dump's
    keywords dict), so both apply exactly the same ranges and rules. `header` may
    be anything with .get() (astropy Header or plain dict).

    bias_level (BIASAVG) is only meaningful for frames whose overscan was cropped,
    i.e. never for master calibration frames (stored as-is)."""
    return {
        "instrument": _str(header, "INSTRUME"),
        "ccd_temperature": _num(header, "CCD_TEMP", -150, 80),
        "air_temperature": _num(header, "CCD_AIR", -60, 100),
        "bias_level": None if is_master else _num(header, "BIASAVG"),
        "altitude": _num(header, "TEL_ALT"),
        "azimuth": _num(header, "TEL_AZ"),
    }


def _pixel_scale_arcsec(wcs, header) -> float | None:
    """Arcsec per (stored, i.e. possibly binned) pixel: from the solved WCS
    when available, else from the telescope's nominal CDELT1T keyword
    (present even in darks). CDELT1T is taken to refer to the stored (binned)
    pixel."""
    if wcs is not None:
        try:
            scale = math.sqrt(abs(float(np.linalg.det(wcs.pixel_scale_matrix)))) * 3600.0
            if math.isfinite(scale) and scale > 0:
                return scale
        except Exception:
            pass
    cdelt = _num(header, "CDELT1T")
    return abs(cdelt) * 3600.0 if cdelt else None


def _quality_size() -> int:
    """Side (px) of the central window used for FWHM; 0 = whole frame."""
    try:
        return max(0, int(os.environ.get(QUALITY_SIZE_ENV_VAR, DEFAULT_QUALITY_SIZE)))
    except ValueError:
        return DEFAULT_QUALITY_SIZE


def _measure_quality(image: np.ndarray, pixel_scale: float | None) -> dict:
    """Cheap star-based image quality (sep = SExtractor core): number of
    clean, unsaturated, round stars in the central window (see QUALITY_SIZE) and
    their median FWHM (from second moments, FWHM = 2.3548 sigma). Approximate -- stars are only ~1.5-2 px
    wide (undersampled), so treat FWHM as a RELATIVE quality indicator
    between frames, not an absolute measurement. FWHM is None when fewer than
    MIN_STARS_FOR_FWHM clean stars are found (cloudy / empty field); n_stars
    is still reported then, which is itself useful for screening.

    Returns all-None if sep isn't installed, --no-quality was given, or the
    measurement fails -- never raises."""
    empty = {"n_stars": None, "fwhm_pixels": None, "fwhm_arcsec": None}
    if sep is None or os.environ.get(QUALITY_DISABLED_ENV_VAR):
        return empty
    try:
        window = np.asarray(image)
        size = _quality_size()
        if size > 0 and (window.shape[0] > size or window.shape[1] > size):
            h, w = window.shape
            ch, cw = min(h, size), min(w, size)
            y0, x0 = (h - ch) // 2, (w - cw) // 2
            window = window[y0:y0 + ch, x0:x0 + cw]
        data = np.ascontiguousarray(window, dtype=np.float32)
        bkg = sep.Background(data)
        subtracted = data - bkg.back()
        objs = sep.extract(subtracted, SEP_DETECT_SIGMA, err=bkg.globalrms, minarea=5)
        a, b = objs["a"], objs["b"]
        clean = (
            (objs["flag"] == 0)
            & (a > 0.5)
            & (b / np.maximum(a, 1e-6) > 0.7)
            & (objs["peak"] < SEP_MAX_PEAK)
        )
        n_stars = int(clean.sum())
        result = dict(empty, n_stars=n_stars)
        if n_stars >= MIN_STARS_FOR_FWHM:
            fwhm_px = _finite(np.median(2.3548 * np.sqrt((a[clean] ** 2 + b[clean] ** 2) / 2.0)))
            result["fwhm_pixels"] = fwhm_px
            if fwhm_px is not None and pixel_scale:
                result["fwhm_arcsec"] = fwhm_px * pixel_scale
        return result
    except Exception as exc:
        logger.warning("Quality measurement failed: %s: %s", type(exc).__name__, exc)
        return empty


def _extract_metadata(item: FramWorkItem) -> dict:
    """
    Extract metadata from a single FITS file's header (+ cheap pixel
    statistics), returning a flat dict. Sync, since astropy I/O is not
    async-native -- async_upload.py runs this via asyncio.to_thread.

    Never raises for a bad FILE (transient I/O errors and memory errors do raise,
    see _is_transient): an unreadable/corrupt/odd file yields a dict
    with a non-empty "problems" list (each entry prefixed with a stable
    E_* reason code), which validate_metadata() returns as-is, so the record
    is skipped with status=skipped_invalid and the reason lands in the stats
    CSV "error" column instead of crashing and being retried 3 times.

    No filtering by IMAGETYP is applied: every type (object, dark, flat,
    masterdark, masterflat, bias, dcurrent, ...) becomes its own record.
    Position fields (center/radius/center_geo/footprint/healpix_idx) are
    only populated when has_position is True, i.e. a science frame with a
    valid WCS -- never as 0.0 sentinels.

    Applies overscan cropping only -- no bias subtraction and no
    linearization -- so mean/median/robust_std match the legacy archive. Master
    calibration frames (MASTER_IMAGETYPES) are used exactly as stored. The
    uploaded FITS file itself is always the untouched original.
    """
    problems: list[str] = []
    result: dict = {"key": item.key, "filename": item.key, "site": item.site, "problems": problems}
    path_str = str(item.fits_path)
    site = item.site

    try:
        header = fits.getheader(path_str, -1)
        image = fits.getdata(path_str, -1)
    except Exception as exc:
        if _is_transient(exc):
            raise
        problems.append(f"E_UNREADABLE: {type(exc).__name__}: {exc}")
        return result

    try:
        time_ = _parse_iso_time(header["DATE-OBS"])
    except Exception as exc:
        problems.append(f"E_BAD_DATE: DATE-OBS missing/unparsable ({type(exc).__name__}: {exc})")
        return result

    if header.get("LONGITUD") is not None:
        night = _get_night(time_, lon=header["LONGITUD"])
    else:
        night = _get_night(time_, site=site)

    obs_type = str(header.get("IMAGETYP", "unknown")).strip()
    try:
        width, height = int(header["NAXIS1"]), int(header["NAXIS2"])
        if obs_type not in MASTER_IMAGETYPES:
            image, header = crop_overscans(image, header, subtract=False)
        usable_width, usable_height = int(image.shape[1]), int(image.shape[0])
    except Exception as exc:
        if _is_transient(exc):
            raise
        problems.append(f"E_CROP_FAILED: {type(exc).__name__}: {exc}")
        return result

    if image.size == 0 or float(np.max(image)) == float(np.min(image)):
        problems.append("E_EMPTY_OR_CONSTANT_IMAGE: no pixel variation")
        return result

    is_science = obs_type == "object"
    is_calibration = obs_type in CALIBRATION_IMAGETYPES

    # --- sky position (science frames with a valid WCS only) -------------
    wcs = None
    has_position = False
    ra0 = dec0 = radius = None
    if is_science and header.get("CTYPE1"):
        try:
            wcs = WCS(header)
            ra, dec = wcs.all_pix2world(
                [0, usable_width, 0.5 * usable_width],
                [0, usable_height, 0.5 * usable_height],
                0,
            )
            radius = _finite(0.5 * _spherical_distance(ra[0], dec[0], ra[1], dec[1]))
            ra0, dec0 = _finite(ra[2]), _finite(dec[2])
            has_position = (
                radius is not None and ra0 is not None and dec0 is not None and abs(dec0) <= 90.0
            )
        except Exception as exc:
            logger.warning("[%s] WCS unusable, storing record without sky position: %s", item.key, exc)
            wcs = None
            has_position = False

    center_geo = footprint = healpix_idx = None
    if has_position:
        center_geo = {"lat": dec0, "lon": _ra_to_lon(ra0)}
        footprint = _compute_footprint(wcs, usable_width, usable_height, dec0, radius)
        theta = np.radians(90.0 - dec0)  # HEALPix co-latitude
        phi = np.radians(ra0)
        healpix_idx = int(hp.ang2pix(HEALPIX_NSIDE, theta, phi))
    else:
        ra0 = dec0 = radius = None

    # --- target: meaningless for calibration frames ----------------------
    target = header.get("TARGET")
    obj_name = header.get("OBJECT")
    if is_calibration:
        target_display = None
    else:
        target_display = f"{target} / {obj_name}" if target and obj_name else (target or obj_name)
        target_display = str(target_display) if target_display is not None else None

    # --- hardware / optics (deliberately minimal) -------------------------
    pixel_scale = _pixel_scale_arcsec(wcs, header)

    # --- image statistics: usable area, overscan cropped, bias NOT subtracted
    # (identical to the legacy archive); robust_std on a subsample.
    sample = np.asarray(image).ravel()[::7].astype(np.float64)
    sample_median = float(np.median(sample))
    robust_std = _finite(1.4826 * np.median(np.abs(sample - sample_median)))

    quality = _measure_quality(image, pixel_scale) if is_science else {
        "n_stars": None, "fwhm_pixels": None, "fwhm_arcsec": None,
    }

    result.update({
        "night": night,
        "observation_time": time_.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "creation_date": time_.date().isoformat(),
        "target": target_display,
        "type": obs_type,
        "filter": header.get("FILTER", "unknown"),
        "ccd": header.get("CCD_NAME"),
        # Sourced from PRODUCT_ID (HIERARCH keyword in the image HDU). Verified
        # on tile-compressed files (primary HDU empty) AND uncompressed ones
        # (image in primary HDU) via fits.getheader(path, -1). CCD_SER is NOT
        # usable: the same value appears on different cameras.
        "camera_serial": (
            str(header["PRODUCT_ID"]) if header.get("PRODUCT_ID") is not None else None
        ),
        "ra0": ra0,
        "dec0": dec0,
        "radius": radius,
        "has_position": has_position,
        "exposure": _num(header, "EXPOSURE"),
        "width": width,
        "height": height,
        "usable_width": usable_width,
        "usable_height": usable_height,
        "binning": header.get("BINNING"),
        "footprint": footprint,
        "center_geo": center_geo,
        "healpix_idx": healpix_idx,
        "longitude": _num(header, "LONGITUD"),  # not uploaded; used by validate_metadata()'s site cross-check
        "n_stars": quality["n_stars"],          # not uploaded; only gates FWHM (see _measure_quality)
        # --- statistics, hardware and camera-state fields (see metadata.yaml) ---
        "mean": _finite(np.mean(image)),
        "median": _finite(np.median(image)),
        "robust_std": robust_std,
        "pixel_scale": pixel_scale,
        # instrument, ccd_temperature, air_temperature, bias_level, altitude, azimuth
        # (BIASAVG exists only when crop_overscans() measured it from a real overscan
        # region; empty for cameras without overscan and for master frames -- never
        # estimated from the temperature model):
        **header_fields(header, is_master=obs_type in MASTER_IMAGETYPES),
        "fwhm_arcsec": quality["fwhm_arcsec"],
        "fwhm_pixels": quality["fwhm_pixels"],
    })
    return result


_EXTRACT_LIMIT: threading.BoundedSemaphore | None = None
_EXTRACT_LIMIT_INIT = threading.Lock()


def extract_metadata(item: FramWorkItem) -> dict:
    """Concurrency-bounded wrapper around _extract_metadata().

    async_upload.py runs extraction via asyncio.to_thread, i.e. in the default
    thread pool (up to ~32 threads), independent of --max-concurrency. One G4
    frame needs a few hundred MB while being processed, so unbounded
    extraction can exhaust RAM. At most FRAM_EXTRACT_CONCURRENCY (default 4;
    --extract-concurrency on the CLI) frames are processed at once; the rest
    wait here. Upload throughput is rate-limited anyway, so this costs nothing
    unless the server-side limiter is lifted."""
    global _EXTRACT_LIMIT
    if _EXTRACT_LIMIT is None:
        with _EXTRACT_LIMIT_INIT:
            if _EXTRACT_LIMIT is None:
                _EXTRACT_LIMIT = threading.BoundedSemaphore(
                    max(1, int(os.environ.get("FRAM_EXTRACT_CONCURRENCY", "4")))
                )
    with _EXTRACT_LIMIT:
        return _extract_metadata(item)


def validate_metadata(extracted: dict) -> list[str]:
    """Return a list of problems with extracted FITS metadata. An empty
    list means the record is fine to upload. Each entry starts with a
    stable reason code (E_*), so the stats CSV "error" column can be
    grouped by reason in the notebook.

    Codes: E_UNREADABLE, E_BAD_DATE, E_CROP_FAILED, E_EMPTY_OR_CONSTANT_IMAGE
    (all set by extract_metadata()), plus E_MISSING_FIELD, E_NO_USABLE_SIZE,
    E_BAD_DATE_RANGE, E_BAD_EXPOSURE (checked here).

    This is a minimal defensive check -- it stops malformed files from
    crashing the batch or silently uploading records with null fields the
    read-time calibration association depends on. It does NOT judge
    scientific quality (clouds, defocus): n_stars/fwhm are stored as
    metadata for that instead of discarding frames.
    """
    problems = list(extracted.get("problems") or [])
    if problems:
        return problems  # extraction already failed; nothing more to check

    for field in REQUIRED_METADATA_FIELDS:
        if not extracted.get(field):
            problems.append(f"E_MISSING_FIELD: {field}")
    if not extracted.get("usable_width") or not extracted.get("usable_height"):
        problems.append("E_NO_USABLE_SIZE: usable_width/usable_height is zero or missing")

    year = int(extracted["observation_time"][:4])
    if not (MIN_PLAUSIBLE_YEAR <= year <= datetime.date.today().year + 1):
        problems.append(f"E_BAD_DATE_RANGE: observation year {year} is implausible")
    exposure = extracted.get("exposure")
    if exposure is not None and exposure < 0:
        problems.append(f"E_BAD_EXPOSURE: negative exposure {exposure}")

    # Soft cross-check only (warning, never a rejection): site guessed from
    # the file path vs the observatory longitude in the header itself.
    site, lon = extracted.get("site"), extracted.get("longitude")
    expected = SITE_LONGITUDES.get(site)
    if expected is not None and lon is not None and abs(lon - expected) > 2.0:
        logger.warning(
            "[%s] site %r (from path) expects longitude ~%.1f but header LONGITUD=%.1f",
            extracted.get("key"), site, expected, lon,
        )
    return problems


# ============================================================
# METADATA -> INVENIORDM JSON
# ============================================================


def _drop_none(obj):
    """Recursively remove None values (and dicts left empty by that) so the
    JSON only carries fields that really have a value. Omitting is safer
    than sending explicit nulls: marshmallow can reject null on fields that
    don't declare allow_none, and OpenSearch treats null and missing alike."""
    if isinstance(obj, dict):
        cleaned = {k: _drop_none(v) for k, v in obj.items()}
        return {k: v for k, v in cleaned.items() if v is not None and v != {}}
    if isinstance(obj, list):
        return [_drop_none(v) for v in obj]
    return obj


def build_invenio_metadata(extracted: dict) -> dict:
    title = "FRAM_" + Path(extracted["site"]).stem + "_" + Path(extracted["ccd"]).stem + "_" + Path(extracted["filename"]).stem
    publication_date = datetime.date.today().isoformat()

    metadata = {
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
                    "This dataset contains observation data in the FITS format. "
                    "The observation comes from a robotic telescope "
                    "(Photometric Robotic Atmospheric Monitor - FRAM)."
                ),
            }
        ],
        "identifiers": [{"identifier": "", "scheme": "url"}],
        "related_resources": [{"title": "FRAM_2024_auger", "identifiers": [{"identifier": "https://doi.org/10.83100/j3vr-c838", "scheme": "url"}], "relation_type": {"id": "IsPartOf"}},
                              {"title": "FRAM Robotic Telescopes Data Collection", "identifiers": [{"identifier": "https://doi.org/10.83100/ddxy-p647", "scheme": "url"}], "relation_type": {"id": "IsPartOf"}}],
        "subjects": [{"subject": s} for s in SUBJECTS],
        "rights": [{"id": "4-BY"}],
        "dates": [{"date": extracted["creation_date"], "type": {"id": "Created"}}],
        "experiment": {"id": "FRAM"},
        "target": extracted["target"],
        "type": extracted["type"],
        "observation_time": extracted["observation_time"],
        "observation_night": extracted["night"],
        "exposure": extracted["exposure"],
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
        "alt_az": {
            "altitude": extracted["altitude"],
            "azimuth": extracted["azimuth"],
        },
        "filename": extracted["filename"],
        # Image statistics of the usable area: overscan cropped, bias NOT
        # subtracted (same as the legacy archive; not the raw AVERAGE/STDEV keywords).
        "mean": extracted["mean"],
        "median": extracted["median"],
        "robust_std": extracted["robust_std"],
        "instrument": extracted["instrument"],
        "pixel_scale": extracted["pixel_scale"],
        "ccd_temperature": extracted["ccd_temperature"],
        "air_temperature": extracted["air_temperature"],
        "bias_level": extracted["bias_level"],
        "fwhm_arcsec": extracted["fwhm_arcsec"],
        "fwhm_pixels": extracted["fwhm_pixels"],
    }

    # Sky-position fields exist ONLY for science frames with a valid WCS.
    # Calibration frames / frames without WCS simply don't carry them (the
    # old 0.0 sentinels were indistinguishable from a real position at
    # RA=0, Dec=0 and would match cone searches there).
    if extracted.get("has_position"):
        metadata["center"] = {"ra": extracted["ra0"], "dec": extracted["dec0"]}
        metadata["radius"] = extracted["radius"]
        metadata["footprint"] = extracted["footprint"]
        metadata["center_geo"] = extracted["center_geo"]
        metadata["healpix_idx"] = extracted["healpix_idx"]

    return {
        "metadata": _drop_none(metadata),
        "files": {"enabled": True},
        "access": {
            "record": "public",
            "files": "public",
            # Native Python types (False / None), not the strings "false" / "null".
            "embargo": {"active": False, "reason": None}
        },
        # Passed through by async_upload.py as client.records.create()'s
        # community= keyword argument (see COMMUNITY in the module docstring).
        # Community membership lives under parent.communities, which only that
        # argument populates; a top-level "communities" key on the record body
        # is not recognised.
        "community": FRAM_COMMUNITY,
        # Metadata-model routing: async_upload.py passes this as
        # client.records.create()'s model= keyword argument.
        "model": "fram",
    }


# ============================================================
# FILES TO UPLOAD PER RECORD
# ============================================================


def get_upload_files(item: FramWorkItem, extracted: dict) -> list[tuple[str, Path, str]]:
    """FRAM uploads exactly one FITS file per record."""
    return [(item.fits_path.name, item.fits_path, "Measurement data")]


# ============================================================
# CLI ENTRY POINT
#
# bulk_async.py is the generic engine and cannot be run directly; this file is
# the entry point for FRAM. It fills in --adapter fram and --metadata-dir (the
# same path as --data-root) and forwards everything else to bulk_async.py.
#
# Expected layout: the shared engine (async_upload.py, bulk_async.py,
# adapters.py) lives one directory above this file. Adjust ENGINE_DIR below if
# it lives elsewhere.
#     Upload/async_upload.py
#     Upload/bulk_async.py
#     Upload/adapters.py
#     Upload/FRAM/fram_upload.py   <- this file
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

    # FRAM has no metadata directory, so --metadata-dir always mirrors
    # --data-root. DEFAULT_METADATA_DIR is computed at import time, so a
    # --data-root given on the command line must also be used for --metadata-dir.
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

    # --quality-size N: side of the central window used for FWHM (default 2048, 0 = whole frame).
    quality_size = _extract_flag_value(argv_rest, "--quality-size")
    if quality_size is not None:
        os.environ["FRAM_QUALITY_SIZE"] = quality_size
        argv_rest = _strip_flag(argv_rest, "--quality-size")

    # --no-quality: boolean FRAM-only flag, skips star detection / FWHM.
    if "--no-quality" in argv_rest:
        os.environ["FRAM_NO_QUALITY"] = "1"
        argv_rest = [a for a in argv_rest if a != "--no-quality"]

    extract_conc = _extract_flag_value(argv_rest, "--extract-concurrency")
    if extract_conc is not None:
        os.environ["FRAM_EXTRACT_CONCURRENCY"] = extract_conc
        argv_rest = _strip_flag(argv_rest, "--extract-concurrency")

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

# Usage examples. The FRAM-only flags below are parsed in this file and passed
# on through environment variables, so they do not appear in the --help output
# of bulk_async.py.
#
#   python3 fram_upload.py --environment local --data-root "/path/to/data" --dry-run
#   python3 fram_upload.py --environment production --max-concurrency 4
#
#   # Only April and May 2022 (comma-separated YYYYMMDD day-folder glob patterns):
#   python3 fram_upload.py --environment local --data-root "..." --date-pattern "202204*,202205*"
#
#   # Skip directories by name at any depth (default: "bad"; replaces the default):
#   python3 fram_upload.py --environment local --data-root "..." --exclude-dirs "bad,quarantine,test_*"
#
#   # Limit how many FITS frames are processed at once (RAM; default 4):
#   python3 fram_upload.py --environment local --data-root "..." --extract-concurrency 2
#
#   # FWHM window: central N x N pixels (default 2048; 0 = whole frame, slowest):
#   python3 fram_upload.py --environment local --data-root "..." --quality-size 0
#
#   # Skip star detection / FWHM (faster; fwhm_* fields stay empty):
#   python3 fram_upload.py --environment local --data-root "..." --no-quality
#
#   # Disable directory exclusion (upload everything, including "bad" folders):
#   python3 fram_upload.py --environment local --data-root "..." --exclude-dirs ""
