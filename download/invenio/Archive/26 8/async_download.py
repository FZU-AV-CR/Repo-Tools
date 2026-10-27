"""
Generic async download engine for a single Physics-repository record's
files.

Mirrors the shape of async_upload.py (this pipeline's upload counterpart)
as closely as the download direction allows:
  - same ENVIRONMENTS dict (local/test1/production), same alias/URL/
    verify_tls values, same TOKEN_ENV_VAR ("INVENIO_TOKEN") and token
    resolution order (explicit -> env var -> interactive prompt -- never
    hardcoded), same typed-"PRODUCTION"-to-confirm client factory for the
    production environment.
  - same WeightedSemaphore transfer-budget concept: a large/multi-part
    download counts for more of the shared in-flight-bytes budget than a
    small single-shot one, independent of --max-concurrency (which only
    bounds how many *items* are being processed at once). See bulk_download
    .py's --transfer-weight-budget.
  - same per-item stats-row shape/idea as async_upload.py's
    upload_record_async()/_write_stats(), just for downloads. bulk_download
    .py's resume/retry/circuit-breaker/progress logic all key off this.

Differences from async_upload.py, because downloading is a different shape
of problem than uploading:
  - the unit of work here is one FILE, not one record. A record can carry
    several files (e.g. Delphi/particles), and main.py (the CERN Open Data
    downloader this pipeline replaces for the Physics repository) tracked
    progress/retries per file too, not per record -- keeping that grain
    means an interrupted run resumes without re-fetching files that had
    already finished, even if a sibling file in the same record failed.
  - no adapter/metadata-model-building step: there is nothing to "extract"
    or "validate" on the way down, so there is no adapters.py-style
    interface to implement per model. What DOES vary per model is which
    records to select in the first place (its repository "model" name and,
    optionally, a default query) -- see download_adapters.py, which is a
    much thinner registry than adapters.py because of this.
  - resume-safe by default even without a stats CSV: a file that already
    exists locally with the expected size is skipped before any network
    call is made (see already_downloaded()). The stats CSV is still what
    bulk_download.py's resume logic (interrupted-run detection, circuit
    breaker inputs, --dry-run planning) is keyed off, same as upload.
  - big files use nrp-cmd's own multi-part parallel GET (parts=/part_size=)
    instead of the S3 multipart-upload transfer_type="M" mechanism -- the
    MULTIPART_THRESHOLD_BYTES/MULTIPART_WEIGHT split below is the download
    analogue of async_upload.py's, tuned higher since FRAM FITS frames and
    Delphi payloads run much larger than typical upload attachments.

ENVIRONMENTS is defined *here*, not in any adapter, since the Physics-
repository endpoints are shared across models -- same rationale as
async_upload.py.
"""

from __future__ import annotations

import asyncio
import csv
import getpass
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from yarl import URL

try:
    from nrp_cmd.async_client import get_async_client
    from nrp_cmd.async_client.streams import FileSink, FileSource
    from nrp_cmd.config import Config, RepositoryConfig
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"Missing NRP async library: {exc}")

logger = logging.getLogger(__name__)


# ============================================================
# ENVIRONMENT / CLIENT CONFIGURATION
# (identical values to async_upload.py -- same repository, same
# deployments, just a different token in practice since download tokens
# can be scoped read-only)
# ============================================================

TOKEN_ENV_VAR = "INVENIO_TOKEN"

ENVIRONMENTS = {
    "local": {
        "alias": "physica-local",
        "url": "https://127.0.0.1:5000/",
        "verify_tls": False,
    },
    "test1": {
        "alias": "physica-test1",
        "url": "https://test1.physics.du.cesnet.cz/",
        "verify_tls": True,
    },
    "production": {
        "alias": "physica-production",
        "url": "https://invenio.fzu.cz/",
        "verify_tls": True,
    },
}

# Canonical stats row schema. Same idea as async_upload.py's STATS_FIELDS:
# every call to _write_stats supplies exactly this set of keys, and a
# retried item produces multiple rows sharing the same `key` (see
# bulk_download.py's resume/"final outcome" helpers for how to read that).
#
# key: f"{record_id}:{file_key}" -- file-level, not record-level (see
# module docstring).
STATS_FIELDS = (
    "key",
    "record_id",
    "file_key",
    "status",
    "error",
    "attempt",
    "max_attempts",
    "start_ts",
    "duration_s",
    "bytes",
    "expected_size",
    "multipart",
    "parts",
)


# ============================================================
# CLIENT HELPERS  (unchanged in shape from async_upload.py)
# ============================================================


def _resolve_token(token: str | None = None) -> str:
    """Resolve an API token: explicit arg -> INVENIO_TOKEN env var ->
    interactive prompt. Never hardcode tokens in source (nrplib_download.py
    did; that's the thing this fixes)."""
    if token:
        return token
    env_token = os.environ.get(TOKEN_ENV_VAR)
    if env_token:
        return env_token
    prompted = getpass.getpass(f"Enter API token ({TOKEN_ENV_VAR} is not set): ").strip()
    if not prompted:
        raise RuntimeError(f"No API token supplied, {TOKEN_ENV_VAR} is not set, and none was entered.")
    return prompted


async def _create_client(env_name: str, token: str | None = None):
    env = ENVIRONMENTS[env_name]
    resolved_token = _resolve_token(token)
    config = Config()
    config.add_repository(
        RepositoryConfig(
            alias=env["alias"],
            url=URL(env["url"]),
            token=resolved_token,
            verify_tls=env["verify_tls"],
        )
    )
    return await get_async_client(env["alias"], config=config)


async def create_local_client(token: str | None = None):
    """Connect to the local dev repository (physica-local @ 127.0.0.1:5000)."""
    return await _create_client("local", token=token)


async def create_test1_client(token: str | None = None):
    """Connect to the test1 repository (physica-test1 @ test1.physics.du.cesnet.cz)."""
    return await _create_client("test1", token=token)


async def create_production_client(token: str | None = None, confirm: bool = True):
    """Connect to the PRODUCTION repository (physica-production @ invenio.fzu.cz).

    Requires interactive confirmation by default, same as async_upload.py --
    a bulk download can run unattended against a large dataset for a long
    time, and it's worth a deliberate step before pointing that at
    production. Pass confirm=False only from a caller that already got
    confirmation another way (e.g. a --yes CLI flag).
    """
    if confirm:
        env = ENVIRONMENTS["production"]
        response = input(
            f"You are about to connect to the PRODUCTION repository ({env['url']}). "
            "Type 'PRODUCTION' (all caps) to continue, anything else to abort: "
        ).strip()
        if response != "PRODUCTION":
            raise RuntimeError("Production run not confirmed by operator. Aborting.")
    return await _create_client("production", token=token)


async def create_client_for_environment(
    env_name: str, token: str | None = None, confirm_production: bool = True
):
    """Dispatch to the right create_*_client() based on an environment name
    string ('local' / 'test1' / 'production'), as used by the CLI's
    --environment flag."""
    if env_name == "local":
        return await create_local_client(token=token)
    if env_name == "test1":
        return await create_test1_client(token=token)
    if env_name == "production":
        return await create_production_client(token=token, confirm=confirm_production)
    raise ValueError(f"Unknown environment: {env_name!r} (expected one of {sorted(ENVIRONMENTS)})")


# ============================================================
# TRANSFER LIMITER
# (same WeightedSemaphore class as async_upload.py, duplicated rather than
# imported so Download/ stays deployable on its own without also needing
# Upload/ on sys.path -- see download_adapters.py for the same choice.)
# ============================================================


class WeightedSemaphore:
    """Like asyncio.Semaphore, but acquire()/release() take a weight
    instead of always being 1. Used so a single multi-part download of a
    large file can count for more of the shared transfer budget than a
    small single-shot download, rather than treating every in-flight file
    transfer as equivalent regardless of size."""

    def __init__(self, value: int):
        self._value = value
        self._cond = asyncio.Condition()

    async def acquire(self, weight: int = 1) -> None:
        async with self._cond:
            while self._value < weight:
                await self._cond.wait()
            self._value -= weight

    async def release(self, weight: int = 1) -> None:
        async with self._cond:
            self._value += weight
            self._cond.notify_all()

    @asynccontextmanager
    async def slot(self, weight: int = 1):
        await self.acquire(weight)
        try:
            yield
        finally:
            await self.release(weight)


@asynccontextmanager
async def _maybe_weighted_slot(limiter: "WeightedSemaphore | None", weight: int = 1):
    if limiter is None:
        yield
        return
    async with limiter.slot(weight):
        yield


# Files at or above this size use nrp-cmd's multi-part parallel GET instead
# of a single-stream download, and count for MULTIPART_WEIGHT of the
# shared transfer budget instead of 1. Higher than async_upload.py's 10 MB
# upload threshold on purpose -- FRAM FITS frames and Delphi payloads are
# typically much larger than what gets uploaded per-call, so a low
# threshold here would put almost everything on the multi-part path.
MULTIPART_THRESHOLD_BYTES = 200 * 1024 * 1024  # 200 MiB
MULTIPART_WEIGHT = 5
DEFAULT_PARTS = 4


def _transfer_weight_for(use_multipart: bool) -> int:
    return MULTIPART_WEIGHT if use_multipart else 1


# ============================================================
# STATS
# ============================================================


def _stats_payload(key: str, **overrides: Any) -> dict[str, Any]:
    payload = {field: None for field in STATS_FIELDS}
    payload["key"] = key
    payload.update(overrides)
    return payload


async def _write_stats(stats_path: Path, stats_format: str, payload: dict[str, Any]) -> None:
    def _write() -> None:
        stats_path.parent.mkdir(parents=True, exist_ok=True)
        new_file = not stats_path.exists()
        with stats_path.open("a", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=STATS_FIELDS)
            if new_file:
                writer.writeheader()
            writer.writerow(payload)

    await asyncio.to_thread(_write)


# ============================================================
# RESUME HELPERS
# ============================================================


def already_downloaded(target: Path, expected_size: int | None) -> bool:
    """Size-based resume check -- cheap enough to run before every file,
    even at FRAM/Delphi scale. Pass verify_checksum=True on
    download_file_async() for an (opt-in, slower) hash-based re-check of
    files that already pass this test."""
    if not target.exists():
        return False
    if expected_size is None:
        return True
    try:
        return target.stat().st_size == expected_size
    except OSError:
        return False


# ============================================================
# PER-FILE DOWNLOAD
# ============================================================


@dataclass
class DownloadItem:
    """One file to fetch. The unit of work bulk_download.py's resume/
    retry/circuit-breaker/progress machinery operates on -- see module
    docstring for why this is file-level, not record-level."""

    key: str  # f"{record_id}:{file_key}", unique + stable across runs
    record_id: str
    file_key: str
    file_obj: Any  # nrp_cmd.types.files.File
    target_path: Path


async def download_file_async(
    client: Any,
    item: DownloadItem,
    *,
    stats_path: Path,
    stats_format: str = "csv",
    dry_run: bool = False,
    verify_checksum: bool = False,
    multipart_threshold: int = MULTIPART_THRESHOLD_BYTES,
    parts: int = DEFAULT_PARTS,
    part_size: int | None = None,
    transfer_limiter: "WeightedSemaphore | None" = None,
    attempt: int = 1,
    max_attempts: int = 1,
) -> bool:
    """Download one file. Returns True if the file is now present and
    correct locally (whether by downloading it just now or because it was
    already there), False/raises on failure.

    Mirrors async_upload.py's upload_record_async() in spirit: single
    attempt (bulk_download.py's retry wrapper handles re-attempts and
    writes the "started" row), always writes a terminal stats row in a
    finally block, converts repository error payloads to readable text.
    """
    start = time.perf_counter()
    start_ts = time.time()
    status = "failed"
    error = ""
    bytes_downloaded = None
    try:
        expected_size = getattr(item.file_obj, "size", None)
        try:
            expected_size = int(expected_size) if expected_size is not None else None
        except (TypeError, ValueError):
            expected_size = None

        if already_downloaded(item.target_path, expected_size):
            if verify_checksum:
                checksum = getattr(item.file_obj, "checksum", None)
                if checksum and ":" in checksum:
                    algo, _, expected_hex = checksum.partition(":")
                else:
                    algo, expected_hex = "md5", checksum
                if algo and algo.lower() == "md5" and expected_hex:
                    local_hex = await FileSource(item.target_path).checksum("md5")
                    if local_hex != expected_hex:
                        logger.warning(
                            "[%s] Checksum mismatch on existing file, re-downloading", item.key
                        )
                        raise IOError("checksum mismatch on existing file")
            status = "skipped_exists"
            bytes_downloaded = item.target_path.stat().st_size
            logger.info("[%s] Already downloaded, skipping", item.key)
            return True

        if dry_run:
            status = "dryrun"
            logger.info(
                "[%s] Dry run OK (would download %s bytes to %s)",
                item.key, expected_size, item.target_path,
            )
            return True

        use_multipart = bool(expected_size and expected_size > multipart_threshold)
        weight = _transfer_weight_for(use_multipart)

        await _write_stats(
            stats_path, stats_format,
            _stats_payload(
                item.key, record_id=item.record_id, file_key=item.file_key,
                status="started", start_ts=start_ts, attempt=attempt, max_attempts=max_attempts,
                expected_size=expected_size, multipart=use_multipart,
            ),
        )

        item.target_path.parent.mkdir(parents=True, exist_ok=True)
        async with _maybe_weighted_slot(transfer_limiter, weight):
            sink = FileSink(item.target_path)
            await client.files.download(
                item.file_obj, sink,
                parts=parts if use_multipart else None,
                part_size=part_size if use_multipart else None,
            )

        bytes_downloaded = item.target_path.stat().st_size
        if expected_size is not None and bytes_downloaded != expected_size:
            raise IOError(f"size mismatch: expected {expected_size}, got {bytes_downloaded}")

        status = "ok"
        logger.info(
            "[%s] Downloaded (%s bytes%s)", item.key, bytes_downloaded,
            f", {parts} parts" if use_multipart else "",
        )
        return True

    except Exception as exc:
        status = "failed"
        error = str(exc)
        logger.error("[%s] Download failed: %s", item.key, error)
        raise
    finally:
        await _write_stats(
            stats_path, stats_format,
            _stats_payload(
                item.key, record_id=item.record_id, file_key=item.file_key,
                status=status, error=error, attempt=attempt, max_attempts=max_attempts,
                start_ts=start_ts, duration_s=round(time.perf_counter() - start, 3),
                bytes=bytes_downloaded, expected_size=expected_size if "expected_size" in dir() else None,
                multipart=locals().get("use_multipart"), parts=parts if locals().get("use_multipart") else None,
            ),
        )


# ============================================================
# RECORD / FILE SELECTION
# (generic across models -- what varies per model is just the `model`
# search-param string and an optional default query, see
# download_adapters.py)
# ============================================================


def build_query(*, query: str | None, filters: list[str], ranges: list[str] | None = None,
                 regexes: list[str] | None = None, year: int | None = None,
                 created_after: str | None = None, created_before: str | None = None) -> str | None:
    """Combine free-text --query and three kinds of repeatable metadata
    filter, plus --year and --created-from/--created-to, into one
    OpenSearch query string. All clauses are ANDed together.

    Three filter modes cover "any metadata field, keyword or range,
    with regex":
      --filter FIELD=VALUE        exact keyword match: field:"value"
      --filter-range FIELD=MIN:MAX  numeric or date range (either bound
                                   may be left empty for an open range):
                                   field:[min TO max]
      --filter-regex FIELD=REGEX  regex match on a keyword field:
                                   field:/regex/ -- OpenSearch/Lucene
                                   regex syntax, which is similar to but
                                   not identical to Python's `re` (e.g.
                                   no lookaround); test against a real
                                   search before relying on an elaborate
                                   pattern. Unescaped '/' in the pattern
                                   must be backslash-escaped by the
                                   caller (Lucene's own regex escaping,
                                   not Python's).

    All field names are passed through as-is -- they must match the
    actual indexed field path for the model in question, which can
    differ from the raw metadata key (see async_download.py's MODEL_NAME
    caveats); confirm against a real search response.
    """
    clauses: list[str] = []
    if query:
        clauses.append(f"({query})")
    for flt in filters:
        if "=" not in flt:
            raise ValueError(f"--filter must be FIELD=VALUE, got: {flt!r}")
        field_name, value = flt.split("=", 1)
        clauses.append(f'{field_name}:"{value}"')
    for rng in ranges or []:
        if "=" not in rng:
            raise ValueError(f"--filter-range must be FIELD=MIN:MAX, got: {rng!r}")
        field_name, bounds = rng.split("=", 1)
        if ":" not in bounds:
            raise ValueError(f"--filter-range bounds must be MIN:MAX (either side may be empty), got: {rng!r}")
        low, high = bounds.split(":", 1)
        low = low.strip() or "*"
        high = high.strip() or "*"
        clauses.append(f"{field_name}:[{low} TO {high}]")
    for rgx in regexes or []:
        if "=" not in rgx:
            raise ValueError(f"--filter-regex must be FIELD=REGEX, got: {rgx!r}")
        field_name, pattern = rgx.split("=", 1)
        if not pattern:
            raise ValueError(f"--filter-regex pattern must not be empty, got: {rgx!r}")
        clauses.append(f"{field_name}:/{pattern}/")
    if year:
        clauses.append(f"created:[{year}-01-01 TO {year}-12-31]")
    if created_after or created_before:
        after = created_after or "*"
        before = created_before or "*"
        clauses.append(f"created:[{after} TO {before}]")
    return " AND ".join(clauses) if clauses else None


async def resolve_records(
    client: Any, *, model: str, ids: list[str] | None, query: str | None,
) -> list[Any]:
    """Return the list of Record objects to process: either the explicit
    `ids` list (read one by one), or every record matching `query` for
    `model` via the library's own scan() (paginates + date-bisects past
    ~5000 hits automatically -- safe to point at a whole community).

    Materializes the full list rather than streaming it, mirroring
    async_upload.py/bulk_async.py's discover_items() -> list[item] shape
    (needed for resume filtering + an accurate progress total). At true
    FRAM/Delphi full-community scale (potentially millions of records) this
    trades some memory for that simplicity/consistency; if that becomes a
    real constraint, narrow the run with --filter/--year/--created-after
    /--created-before, or split it into several --ids-file batches.
    """
    if ids:
        records = []
        for record_id in ids:
            try:
                records.append(await client.records.published_records.read(record_id, model=model))
            except Exception:
                logger.exception("Could not read record %s", record_id)
        return records

    logger.info("Scanning model=%s query=%r for matching records...", model, query)
    records = []
    async with client.records.published_records.scan(q=query, model=model) as hits:
        async for record in hits:
            records.append(record)
    return records


async def list_download_items(
    client: Any, records: list[Any], *, output_dir: Path, metadata_dir: Path | None,
) -> list[DownloadItem]:
    """Expand each selected Record into one DownloadItem per attached file,
    optionally caching each record's metadata JSON to `metadata_dir` along
    the way (skipped if metadata_dir is None)."""
    items: list[DownloadItem] = []
    for record in records:
        record_id = str(record.id)
        record_dir = output_dir / record_id

        if metadata_dir is not None:
            meta_path = metadata_dir / f"{record_id}.json"
            if not meta_path.exists():
                try:
                    from nrp_cmd.converter import converter
                    meta_path.parent.mkdir(parents=True, exist_ok=True)
                    meta_path.write_text(json.dumps(converter.unstructure(record), indent=2, default=str))
                except Exception:
                    logger.exception("Could not save metadata for %s", record_id)

        try:
            files = await client.files.list(record)
        except Exception:
            logger.exception("Could not list files for record %s", record_id)
            continue

        if not files:
            logger.warning("Record %s has no files attached", record_id)
            continue

        for file_obj in files:
            items.append(DownloadItem(
                key=f"{record_id}:{file_obj.key}",
                record_id=record_id,
                file_key=file_obj.key,
                file_obj=file_obj,
                target_path=record_dir / file_obj.key,
            ))
    return items


def setup_logging(level=logging.INFO, log_file: Path | None = None):
    from logging.handlers import RotatingFileHandler

    handlers = [logging.StreamHandler()]
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(
            RotatingFileHandler(log_file, maxBytes=20 * 1024 * 1024, backupCount=5, encoding="utf-8")
        )
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers,
        force=True,
    )
    for noisy in ("nrp_cmd", "urllib3", "aiohttp", "botocore", "boto3", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
