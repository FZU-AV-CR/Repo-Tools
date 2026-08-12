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
                                           "access", "communities" keys)
    get_upload_files(item, extracted)  -> list[(file_key, Path, description)]
and the constant:
    DEFAULT_SCHEMA_URL                 -> str

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
STATS_FIELDS = (
    "key",
    "recid",
    "status",
    "error",
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
    payload.update(key=key, file_count=0, zip_used=False, bytes_uploaded=0)
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
                item.key, status="started", start_ts=start_ts, zip_used=zip_used, checksum_md5=checksum_md5,
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

        record = await client.records.create(record_payload)
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

        for file_key, file_path, description in upload_files:
            if not file_path.exists():
                raise FileNotFoundError(f"Missing file for upload: {file_path}")
            file_ = await client.files.upload(
                record,
                key=file_key,
                metadata={"description": description},
                source=str(file_path),
            )
            logger.info("[%s] Uploaded: %s", item.key, file_.key)
            bytes_uploaded += file_path.stat().st_size
            file_count += 1

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

    await upload_record_async(client=client, item=items[0], stats_path=Path("upload_stats.csv"))


def main() -> None:
    import sys

    # Optional: `python3 async_upload.py sipm` for a quick single-record
    # smoke test against a specific model; falls back to PHYSICS_ADAPTER
    # if omitted (see adapters.py).
    adapter_name = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(main_async(adapter_name))


if __name__ == "__main__":
    main()
