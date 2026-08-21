"""
Generic async upload pipeline for a single Physics-repository record.

Mirrors the shape of fram_async_upload.py, but with all model-specific
metadata extraction/validation/building and file-selection logic factored
out into a separate "adapter" module (see itk_upload.py for the current
one). This file plus bulk_async.py should stay usable for any metadata
model in the Physics repository -- only the adapter import below (and the
discovery-related CLI args in bulk_async.py) should need to change for a
new model.

The adapter (ATLAS ITk, DUNE SiPM, FRAM, Delphi/Particles, ...) is no
longer hardcoded here -- it's resolved at runtime by adapters.py, either
from an explicit name passed to configure_adapter()/main_async(), or from
the PHYSICS_ADAPTER environment variable that each model's own
entry-point script (itk_upload.py, sipm_upload.py, fram_upload.py,
delphi_upload.py) sets before calling into bulk_async.py. Call
configure_adapter(name) once at startup before running any uploads;
upload_record_async() below raises clearly if that hasn't been done.

Every adapter module must provide:
    discover_items(...)                -> list[item]   (each item needs a
                                           unique, stable `.key` attribute)
    extract_metadata(item)             -> dict                (sync)
    validate_metadata(extracted)       -> list[str]            (sync)
    build_invenio_metadata(extracted)  -> dict  (with "metadata", "files",
                                           "access" keys, and optionally
                                           "communities", "community",
                                           "workflow", "model" -- see below)
    get_upload_files(item, extracted)  -> list[(file_key, Path, description)]
and the constant:
    DEFAULT_SCHEMA_URL                 -> str

COMMUNITY / WORKFLOW / MODEL: an adapter has two independent, non-exclusive
ways to put a record into a community when calling records.create() (see
nrp_cmd.async_client.invenio.records.AsyncInvenioRecordsClient.create()):
  - the older manual style -- set a "communities" key (e.g.
    {"ids": [...]}) inside the dict returned by build_invenio_metadata();
    it's merged straight into the record's JSON body as a top-level
    "communities" key (see below). Used by sipm_upload.py.
  - the client's own community=/workflow=/model= keyword arguments -- set
    "community" (a community slug/id string), "workflow" (a workflow name
    string), and/or "model" (a metadata-model name string, e.g. the
    original Delphi script's model="particles") keys in the dict returned
    by build_invenio_metadata(); upload_record_async() below passes
    whichever of these are present through as keyword arguments to
    client.records.create(), e.g. client.records.create(record_payload,
    community=..., workflow=..., model=...). community/workflow get
    turned into record_payload["parent"]["communities"]["default"]/
    ["workflow"] by the client itself -- this is the mechanism documented
    in nrp_cmd's own usage guide and is what fram_upload.py /
    fram_upload_test.py use. If "workflow" is omitted, the community's
    default workflow is used. "model" has no such transformation -- it's
    passed through as-is.

See adapters.py for the full interface and instructions for registering a
new metadata model.

ENVIRONMENTS (local/test1/production) is defined *here*, not in any
adapter, since the Physics-repository endpoints are shared across models.
"""

from __future__ import annotations

import asyncio
import csv
import getpass
import hashlib
import json
import logging
import os
import time
import zipfile
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from yarl import URL

try:
    from nrp_cmd.async_client import get_async_client
    from nrp_cmd.config import Config, RepositoryConfig
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"Missing NRP async library: {exc}")

import adapters

logger = logging.getLogger(__name__)

# Set by configure_adapter() -- deliberately not imported directly at
# module load time (see module docstring). Every function below that
# needs the active metadata-model adapter reads this module-level name at
# call time, so reassigning it via configure_adapter() takes effect
# immediately for any subsequent call, including already-scheduled asyncio
# tasks that haven't reached their adapter.* call yet.
adapter = None


def configure_adapter(name: str | None = None):
    """Resolve and set the module-level adapter used by
    upload_record_async() / main_async(). Call this once at startup,
    before any uploads run -- bulk_async.py does this automatically from
    its --adapter flag / PHYSICS_ADAPTER env var; call it explicitly if
    using async_upload.py's functions directly (e.g. from a notebook or a
    custom script).
    """
    global adapter
    adapter = adapters.load(name)
    logger.info("Using adapter: %s", adapter.__name__)
    return adapter


def _format_exception_details(exc: Exception) -> str:
    """Return a readable string that includes repository JSON error details when available."""
    parts = [str(exc)]

    if hasattr(exc, "json") and isinstance(getattr(exc, "json"), dict):
        payload = exc.json
        try:
            parts.append(json.dumps(payload, indent=2, ensure_ascii=False))
        except TypeError:
            parts.append(str(payload))

    request_info = getattr(exc, "request_info", None)
    if request_info is not None:
        url = getattr(request_info, "url", None)
        if url:
            parts.insert(0, f"Request URL: {url}")

    return "\n".join(parts)


# ============================================================
# ENVIRONMENT / CLIENT CONFIGURATION
# (shared Physics-repository infrastructure -- same across metadata models)
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

# Canonical stats row schema -- every call to _write_stats supplies exactly
# this set of keys so the CSV header stays consistent across "started" and
# terminal rows.
#
# attempt / max_attempts: an item that fails and is retried by
# bulk_async.py's _upload_with_retries() produces MULTIPLE rows sharing the
# same key -- e.g. attempt=1 status=failed, attempt=2 status=failed,
# attempt=3 status=ok. Without these two fields, scanning the CSV for
# status=="failed" flags items that ultimately succeeded on a later retry,
# which is misleading. To find items that TRULY failed (exhausted all
# retries), filter for status=="failed" AND attempt==max_attempts. To find
# every item's final outcome regardless of status, take the last row per
# key (by start_ts) or equivalently the row where attempt==max_attempts OR
# status=="ok". See the "reading upload_stats.csv" note near
# _upload_with_retries() in bulk_async.py.
STATS_FIELDS = (
    "key",
    "recid",
    "status",
    "error",
    "attempt",
    "max_attempts",
    "start_ts",
    "duration_s",
    "file_count",
    "zip_used",
    "bytes_uploaded",
    "checksum_md5",
)


# ============================================================
# CLIENT HELPERS
# ============================================================


def _resolve_token(token: str | None = None) -> str:
    """Resolve an API token: explicit arg -> INVENIO_TOKEN env var ->
    interactive prompt. Never hardcode tokens in source."""
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

    By default this requires an interactive confirmation before connecting,
    since a bulk run can run unattended for a long time against a large,
    hard-to-undo dataset. Pass confirm=False only from a caller that has
    already obtained confirmation some other way (e.g. a --yes CLI flag).
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
# (restored from the original, model-specific Delphi async_upload.py this
# engine was generalized from -- dropped during generalization, which is
# suspected to be a factor in HTTP transfer errors / bulk_async.py's
# circuit breaker tripping under high --max-concurrency, since nothing was
# left bounding how many large file transfers can be in flight at once.
# See bulk_async.py's --transfer-weight-budget flag for how this is sized.)
#
# Concurrency of *records* (--max-concurrency) and concurrency of *transfer
# weight* (this limiter) are independent: a large --max-concurrency lets
# many records' extract/create/publish steps overlap, while this limiter
# is the thing that actually caps how many bytes-in-flight hit the
# repository/network at once, regardless of how many records are "active".
# ============================================================


class WeightedSemaphore:
    """Like asyncio.Semaphore, but acquire()/release() take a weight
    instead of always being 1. Used so a single multipart ("M") transfer
    of a large file can count for more of the shared transfer budget than
    a small single-shot ("L") transfer, rather than treating every
    in-flight file upload as equivalent regardless of size."""

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


# Files at or above this size use a multipart ("M") transfer instead of a
# single-shot local ("L") one, and count for MULTIPART_WEIGHT of the
# shared transfer budget instead of 1. Matches the 10 MB threshold /
# 5x weighting used by the original Delphi script.
MULTIPART_THRESHOLD_BYTES = 10 * 1024 * 1024
MULTIPART_WEIGHT = 5


def _transfer_type_for(file_path: Path) -> str:
    try:
        size = file_path.stat().st_size
    except OSError:
        # Missing/unreadable file: let the subsequent upload call raise a
        # clearer error rather than guessing here.
        return "L"
    return "M" if size >= MULTIPART_THRESHOLD_BYTES else "L"


def _transfer_weight_for(transfer_type: str) -> int:
    return MULTIPART_WEIGHT if transfer_type == "M" else 1


async def _upload_file_with_limiter(
    client,
    record,
    *,
    key: str,
    metadata: dict,
    source: str,
    file_path: Path,
    limiter: "WeightedSemaphore | None" = None,
):
    """Upload a single file with an explicit, size-based transfer_type
    (restored from the original Delphi script -- omitting transfer_type
    left it up to the client library's own default, which is suspected to
    be behind HTTP transfer errors on larger files) and, if a limiter is
    supplied, hold a weighted slot in the shared transfer budget for the
    duration of the transfer."""
    transfer_type = _transfer_type_for(file_path)
    weight = _transfer_weight_for(transfer_type)
    async with _maybe_weighted_slot(limiter, weight):
        return await client.files.upload(
            record, key=key, metadata=metadata, source=source, transfer_type=transfer_type,
        )


# ============================================================
# ZIP BUNDLING
# (ported from the original, model-specific Delphi async_upload.py as a
# generic utility -- not wired into the core pipeline directly, since the
# decision of *whether* to bundle a record's files into a zip belongs to
# an adapter's own get_upload_files() (e.g. "more than 20 loose files ->
# zip them"), not to this shared engine. An adapter that wants Delphi's
# original bundling behavior can call ensure_zip_async() from its own
# get_upload_files() / a pre-processing step and return the resulting zip
# path as its single upload file, the same way Delphi's original script
# did inline.
#
# NOTE: ensure_zip() below deletes each original file (fp.unlink()) once
# it's been written into the archive -- this matches the original script's
# behavior exactly, but means it is destructive to the source files. Any
# adapter using this should be deliberate about that, and pass an
# already-known-safe-to-delete file list.
# ============================================================


def ensure_zip(dataset_files: list[Path], zip_path: Path) -> None:
    """Bundle dataset_files into a single zip at zip_path, removing each
    original file once archived. If zip_path already exists, updates it
    in place (adds any files not yet present or newer than their zip
    entry) rather than recreating it from scratch -- safe to call again
    after a partial/interrupted previous run. Sync/blocking; call via
    ensure_zip_async() from async code."""
    existing_files = [p for p in dataset_files if p.exists()]
    if zip_path in existing_files:
        existing_files.remove(zip_path)

    if zip_path.exists():
        if not existing_files:
            logger.info("Zip archive exists and originals are already removed: %s", zip_path)
            return

        updated = 0
        with zipfile.ZipFile(zip_path, "a", compression=zipfile.ZIP_STORED, allowZip64=True) as zf:
            zip_info = {zi.filename: zi for zi in zf.infolist()}
            for fp in existing_files:
                zi = zip_info.get(fp.name)
                if zi is None:
                    zf.write(fp, arcname=fp.name)
                    fp.unlink()
                    updated += 1
                    continue

                # Compare zip entry timestamp to file mtime
                zip_ts = time.mktime(zi.date_time + (0, 0, -1))
                if fp.stat().st_mtime > zip_ts:
                    logger.warning("Zip archive has outdated file %s", fp.name)
                    updated += 1
                else:
                    fp.unlink()

        logger.info(
            "Zip archive updated (fixed in place): %s, files added: %s, originals removed: %s",
            zip_path, updated, len(existing_files) - updated,
        )
        return

    if not existing_files:
        logger.warning("No original files to zip and no zip found: %s", zip_path)
        return

    logger.info("Too many files to upload individually, creating a zip archive...")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as zf:
        for fp in existing_files:
            zf.write(fp, arcname=fp.name)
            fp.unlink()

    logger.info("Created zip archive: %s", zip_path)


async def ensure_zip_async(
    dataset_files: list[Path], zip_path: Path, zip_sem: "asyncio.Semaphore | None" = None
) -> None:
    """Async wrapper around ensure_zip(), running the blocking zip I/O in
    a thread. Pass a shared asyncio.Semaphore as zip_sem to bound how many
    zip-creation operations run concurrently across a batch (zip creation
    is CPU/IO-bound and independent of the network transfer limiter
    above)."""
    async with _maybe_sem(zip_sem):
        await asyncio.to_thread(ensure_zip, dataset_files, zip_path)


@asynccontextmanager
async def _maybe_sem(sem: "asyncio.Semaphore | None"):
    if sem is None:
        yield
        return
    async with sem:
        yield


# ============================================================
# CHECKSUM
# ============================================================


def compute_md5(path: Path, chunk_size: int = 1024 * 1024) -> str:
    md5 = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            md5.update(chunk)
    return md5.hexdigest()


# ============================================================
# STATS WRITING
# ============================================================


def _stats_payload(key: str, **overrides) -> dict:
    payload = {field: None for field in STATS_FIELDS}
    payload.update(key=key, file_count=0, zip_used=False, bytes_uploaded=0, attempt=1, max_attempts=1)
    payload.update(overrides)
    return payload


async def _write_stats(stats_path: Path | None, fmt: str, payload: dict) -> None:
    if not stats_path:
        return

    def _sync_write():
        stats_path.parent.mkdir(parents=True, exist_ok=True)
        if fmt == "csv":
            write_header = not stats_path.exists()
            with stats_path.open("a", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=STATS_FIELDS, extrasaction="ignore")
                if write_header:
                    writer.writeheader()
                writer.writerow(payload)
        else:  # jsonl
            with stats_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

    await asyncio.to_thread(_sync_write)


# ============================================================
# PER-RECORD PIPELINE
# (generic -- delegates anything model-specific to `adapter`)
# ============================================================


async def upload_record_async(
    client,
    item,
    stats_path: Path | None = Path("upload_stats.csv"),
    stats_format: str = "csv",
    dry_run: bool = False,
    validate: bool = True,
    schema_url: str | None = None,
    upload_limiter: "WeightedSemaphore | None" = None,
    file_concurrency: int = 1,
    attempt: int = 1,
    max_attempts: int = 1,
) -> object | None:
    """
    Full pipeline for a single record: extract -> validate -> build
    metadata -> [create -> upload file(s) -> publish] -> stats.

    `item` is whatever adapter.discover_items() produced; it must at least
    expose a stable string `.key` used for resume/dedup and stats rows.

    A "started" stats row is written immediately before records.create() is
    called (real, non-dry-run uploads only) -- if the process dies between
    that point and the terminal row written in `finally` below, the key
    will show up as "interrupted" on the next run (see bulk_async.py's
    _scan_stats()), which may indicate an orphaned draft in the repository
    worth checking manually. This is not a full transactional guarantee --
    just enough to make crashes visible instead of silent.

    attempt / max_attempts are supplied by the caller (bulk_async.py's
    retry wrapper passes the current attempt number and its configured
    retry count) purely so they end up in the stats CSV -- see the
    STATS_FIELDS comment for why that matters when reading the CSV back.
    A direct caller not using bulk_async.py's retry wrapper can safely
    leave both at their default of 1.

    file_concurrency bounds how many of this record's own files (per
    get_upload_files()) upload at once -- irrelevant for adapters like
    FRAM that return exactly one file per record, but lets an adapter like
    Delphi's (multiple files per record) upload them concurrently instead
    of strictly one-at-a-time. Each individual file transfer still also
    goes through the shared, global upload_limiter regardless of this
    value, so file_concurrency only affects how many of *this record's*
    files can be waiting on that shared limiter simultaneously.
    """
    if adapter is None:
        raise RuntimeError(
            "No adapter configured -- call async_upload.configure_adapter(name) "
            "(or run via bulk_async.py's --adapter flag / PHYSICS_ADAPTER env var) "
            "before uploading."
        )

    start = time.perf_counter()
    start_ts = time.time()
    status = "ok"
    error = None
    bytes_uploaded = 0
    checksum_md5 = None
    published = None
    file_count = 0

    try:
        extracted = await asyncio.to_thread(adapter.extract_metadata, item)
        upload_files = adapter.get_upload_files(item, extracted)
        zip_used = any(p.suffix.lower() == ".zip" for _, p, _ in upload_files)

        if upload_files:
            checksum_md5 = await asyncio.to_thread(compute_md5, upload_files[0][1])

        if validate:
            problems = adapter.validate_metadata(extracted)
            if problems:
                status = "skipped_invalid"
                error = "; ".join(problems)
                logger.warning("[%s] Skipping, validation failed: %s", item.key, error)
                return None

        metadata = adapter.build_invenio_metadata(extracted)
        record_schema_url = schema_url or adapter.DEFAULT_SCHEMA_URL

        if dry_run:
            status = "dryrun"
            bytes_uploaded = sum(p.stat().st_size for _, p, _ in upload_files if p.exists())
            file_count = len(upload_files)
            logger.info("[%s] Dry run OK (would create/upload/publish, %s file(s))", item.key, file_count)
            return None

        await _write_stats(
            stats_path,
            stats_format,
            _stats_payload(
                item.key, status="started", start_ts=start_ts, zip_used=zip_used,
                checksum_md5=checksum_md5, attempt=attempt, max_attempts=max_attempts,
            ),
        )

        record_payload = {
            "metadata": metadata["metadata"],
            "access": metadata["access"],
            "files": metadata["files"],
            "$schema": record_schema_url,
        }
        # Only some adapters set "communities" (e.g. SiPM does, ITk
        # doesn't) -- omit the key entirely rather than sending a null
        # value when an adapter's build_invenio_metadata() doesn't
        # include it.
        if metadata.get("communities"):
            record_payload["communities"] = metadata["communities"]

        # Newer, client-native mechanism (see module docstring's
        # "COMMUNITY / WORKFLOW" section): an adapter may instead (or
        # additionally) set "community"/"workflow" keys, which are passed
        # as records.create() keyword arguments rather than folded into
        # the JSON body directly -- this is what fram_upload.py uses.
        create_kwargs: dict[str, Any] = {}
        if metadata.get("community"):
            create_kwargs["community"] = metadata["community"]
        if metadata.get("workflow"):
            create_kwargs["workflow"] = metadata["workflow"]
        # "model" is a third, independent kwarg some deployments need to
        # route record creation to the right metadata model (e.g. the
        # original Delphi script calls client.records.create(metadata,
        # model="particles")). Optional and omitted entirely unless an
        # adapter's build_invenio_metadata() sets "model".
        if metadata.get("model"):
            create_kwargs["model"] = metadata["model"]

        record = await client.records.create(record_payload, **create_kwargs)
        logger.info("[%s] Created draft: %s", item.key, record.id)

        # Self-referential URL identifier: Physics-repository records
        # generally use the record's own landing-page URL as their
        # identifier rather than minting a DOI (see the Delphi template's
        # equivalent record.metadata["identifiers"][0] logic). An adapter
        # opts in by including an {"identifier": "", "scheme": "url"} entry
        # in the "identifiers" list returned from build_invenio_metadata();
        # any such entry is filled in here and pushed back to the draft.
        # Adapters that omit "identifiers" entirely, or that populate their
        # own identifier scheme (e.g. a DOI), are unaffected -- no extra
        # API call is made for them.
        identifiers = getattr(record, "metadata", {}).get("identifiers") if hasattr(record, "metadata") else None
        if identifiers:
            filled = False
            for ident in identifiers:
                if ident.get("scheme") == "url" and not ident.get("identifier"):
                    ident["identifier"] = str(record.links.self_html)
                    filled = True
            if filled:
                record = await client.records.draft_records.update(record)
                logger.info(
                    "[%s] Filled self-URL identifier, now at revision: %s",
                    item.key, getattr(record, "revision_id", None),
                )

        file_sem = asyncio.Semaphore(max(1, file_concurrency))

        async def _upload_one(file_key: str, file_path: Path, description: str) -> int:
            if not file_path.exists():
                raise FileNotFoundError(f"Missing file for upload: {file_path}")
            async with file_sem:
                file_ = await _upload_file_with_limiter(
                    client,
                    record,
                    key=file_key,
                    metadata={"description": description},
                    source=str(file_path),
                    file_path=file_path,
                    limiter=upload_limiter,
                )
            logger.info("[%s] Uploaded: %s", item.key, file_.key)
            return file_path.stat().st_size

        upload_sizes = await asyncio.gather(
            *[_upload_one(fk, fp, desc) for fk, fp, desc in upload_files]
        )
        bytes_uploaded += sum(upload_sizes)
        file_count += len(upload_files)

        published = await client.records.publish(record)
        logger.info("[%s] Published: %s", item.key, published.id)

    except Exception as exc:
        status = "failed"
        error = _format_exception_details(exc)
        logger.error("[%s] Upload failed: %s", item.key, error)
        raise
    finally:
        await _write_stats(
            stats_path,
            stats_format,
            _stats_payload(
                item.key,
                recid=getattr(published, "id", None),
                status=status,
                error=error,
                attempt=attempt,
                max_attempts=max_attempts,
                start_ts=start_ts,
                duration_s=round(time.perf_counter() - start, 3),
                file_count=file_count,
                zip_used=zip_used,
                bytes_uploaded=bytes_uploaded,
                checksum_md5=checksum_md5,
            ),
        )

    return published


def setup_logging(level=logging.INFO, log_file: Path | None = None):
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


async def main_async(adapter_name: str | None = None) -> None:
    """Single-record smoke test against the local dev repository.

    adapter_name picks which metadata model to smoke-test: an explicit
    name, else the PHYSICS_ADAPTER environment variable (see adapters.py).
    """
    setup_logging()
    active = configure_adapter(adapter_name)
    client = await create_local_client()

    items = active.discover_items(
        metadata_dir=Path(active.DEFAULT_METADATA_DIR),
        data_root=Path(active.DEFAULT_DATA_ROOT),
        readme_file=Path(active.DEFAULT_README_FILE) if active.DEFAULT_README_FILE else None,
    )
    if not items:
        print("No work items discovered.")
        return

    await upload_record_async(
        client=client,
        item=items[0],
        stats_path=Path("upload_stats.csv"),
        upload_limiter=WeightedSemaphore(50),
    )


def main() -> None:
    import sys

    # Optional: `python3 async_upload.py sipm` for a quick single-record
    # smoke test against a specific model; falls back to PHYSICS_ADAPTER
    # if omitted (see adapters.py).
    adapter_name = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(main_async(adapter_name))


if __name__ == "__main__":
    main()
