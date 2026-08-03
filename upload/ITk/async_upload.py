"""
Generic async upload pipeline for a single Physics-repository record.

Mirrors the shape of fram_async_upload.py, but with all model-specific
metadata extraction/validation/building and file-selection logic factored
out into a separate "adapter" module (see itk_upload.py for the current
one). This file plus bulk_async.py should stay usable for any metadata
model in the Physics repository -- only the adapter import below (and the
discovery-related CLI args in bulk_async.py) should need to change for a
new model.

Currently wired to: itk_upload (ATLAS ITk silicon-sensor test data).

To reuse for another model, swap the `import itk_upload as adapter` line
below for a new adapter module that provides:
    discover_items(...)                -> list[item]   (each item needs a
                                           unique, stable `.key` attribute)
    extract_metadata(item)             -> dict                (sync)
    validate_metadata(extracted)       -> list[str]            (sync)
    build_invenio_metadata(extracted)  -> dict  (with "metadata", "files",
                                           "access", "communities" keys)
    get_upload_files(item, extracted)  -> list[(file_key, Path, description)]
and the constant:
    DEFAULT_SCHEMA_URL                 -> str

ENVIRONMENTS (local/test1/production) is defined *here*, not in the
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

import itk_upload as adapter

logger = logging.getLogger(__name__)


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

        record = await client.records.create(
            {
                "metadata": metadata["metadata"],
                "access": metadata["access"],
                "files": metadata["files"],
                #"communities": metadata.get("communities"),
                "$schema": record_schema_url,
            }
        )
        logger.info("[%s] Created draft: %s", item.key, record.id)

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


async def main_async() -> None:
    """Single-record smoke test against the local dev repository."""
    setup_logging()
    client = await create_local_client()

    items = adapter.discover_items(
        metadata_dir=Path(adapter.DEFAULT_METADATA_DIR),
        data_root=Path(adapter.DEFAULT_DATA_ROOT),
        readme_file=Path(adapter.DEFAULT_README_FILE) if adapter.DEFAULT_README_FILE else None,
    )
    if not items:
        print("No work items discovered.")
        return

    await upload_record_async(client=client, item=items[0], stats_path=Path("upload_stats.csv"))


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
