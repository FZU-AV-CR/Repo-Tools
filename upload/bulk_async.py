import argparse
import asyncio
import csv
import json
import logging
import time
from pathlib import Path

from async_upload import create_default_client, upload_record_async, WeightedSemaphore, setup_logging

logger = logging.getLogger(__name__)


def _load_master_recids(master_cache: Path) -> list[int]:
    payload = json.loads(master_cache.read_text(encoding="utf-8"))
    recids = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            try:
                recid = int(key)
            except (TypeError, ValueError):
                recid = int(value.get("recid")) if isinstance(value, dict) and value.get("recid") else None
            if recid is not None:
                recids.append(recid)
    return sorted(set(recids))


def _is_collision(metadata_path: Path) -> str:
    if not metadata_path.exists():
        return "missing"
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    except Exception:
        return "none"
    meta = payload.get("metadata", {})
    type_info = meta.get("type") or {}
    secondary = type_info.get("secondary") if isinstance(type_info, dict) else []
    if isinstance(secondary, list):
        lowered = [str(s).lower() for s in secondary if s is not None]
        if any(s == "collision" for s in lowered):
            return "collision"
        if any(s == "simulated" for s in lowered):
            return "simulated"
        return "none" if not lowered else "none"
    secondary_str = str(secondary).strip().lower()
    if secondary_str == "collision":
        return "collision"
    if secondary_str == "simulated":
        return "simulated"
    return "none"


def _load_uploaded_recids(stats_path: Path) -> set[int]:
    """Return recids with successful uploads from upload_stats.csv."""
    # TODO: improve the logic to redo unfinished datasets
    if not stats_path.exists():
        return set()

    uploaded: set[int] = set()
    try:
        with stats_path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                status = str(row.get("status", "")).strip().lower()
                recid_raw = row.get("recid")
                if status != "ok" or recid_raw is None:
                    continue
                try:
                    uploaded.add(int(recid_raw))
                except (TypeError, ValueError):
                    continue
    except Exception as exc:
        logger.warning("Failed to parse stats file %s: %s", stats_path, exc)
        return set()

    return uploaded


async def _upload_with_retries(client, recid, data_dir, metadata_dir, template_path, retries=3, delay=2, file_concurrency=4, upload_limiter=None, zip_sem=None, stats_path=None, stats_format="jsonl"):
    for attempt in range(1, retries + 1):
        try:
            return await upload_record_async(
                client=client,
                recid=recid,
                data_dir=data_dir,
                metadata_dir=metadata_dir,
                template_path=template_path,
                file_concurrency=file_concurrency,
                upload_limiter=upload_limiter,
                zip_sem=zip_sem,
                stats_path=stats_path,
                stats_format=stats_format,
            )
        except Exception as exc:
            logger.warning("[%s] Upload failed (attempt %s/%s): %s", recid, attempt, retries, exc)
            if attempt == retries:
                raise
            await asyncio.sleep(delay * attempt)


async def main_async():
    # parser = argparse.ArgumentParser(description="Bulk async upload by recids from master cache.")
    # parser.add_argument("--data-dir", required=True, help="Path to dataset folders")
    # parser.add_argument("--metadata-dir", required=True, help="Path to metadata files")
    # parser.add_argument("--master-cache", required=True, help="Path to master cache JSON")
    # parser.add_argument("--template", default="delphi_nrp_example.json", help="Path to DELPHI template JSON")
    # args = parser.parse_args()
    #
    # data_dir = Path(args.data_dir)
    # metadata_dir = Path(args.metadata_dir)
    # master_cache = Path(args.master_cache)
    # template_path = Path(args.template)

    setup_logging()
    data_dir = Path("../data")
    metadata_dir = Path("../metadata")
    master_cache = Path("../delphi_records_master.json")
    template_path = Path("delphi_nrp_example.json")
    max_concurrency = 4
    file_concurrency = 4
    max_retries = 3
    upload_limiter = WeightedSemaphore(50)
    zip_sem = asyncio.Semaphore(2)
    stats_path = Path("upload_stats.csv")
    stats_format = "csv"  # or "csv"

    # number of files in metadata dir
    meta_files = list(metadata_dir.glob("*.json"))
    logger.info("Metadata files found: %s", len(meta_files))

    recids = _load_master_recids(master_cache)
    if not recids:
        logger.warning("No recids found in master cache.")
        return

    collision, simulated, none_type, missing = [], [], [], []
    for recid in recids:
        meta_path = metadata_dir / f"{recid}.json"
        status = _is_collision(meta_path)
        if status == "collision":
            collision.append(recid)
        elif status == "simulated":
            simulated.append(recid)
        elif status == "missing":
            missing.append(recid)
        else:
            none_type.append(recid)

    # Resume logic to skip data already uploaded
    already_uploaded = _load_uploaded_recids(stats_path)
    if already_uploaded:
        before = len(collision) + len(simulated) + len(none_type) + len(missing)
        collision = [r for r in collision if r not in already_uploaded]
        simulated = [r for r in simulated if r not in already_uploaded]
        none_type = [r for r in none_type if r not in already_uploaded]
        missing = [r for r in missing if r not in already_uploaded]
        after = len(collision) + len(simulated) + len(none_type) + len(missing)
        logger.info("Resume: skipped %s already uploaded recids from %s", before - after, stats_path)

    ordered = collision + simulated + none_type + missing
    logger.info(
        "Recids total: %s | Collision: %s | Simulated: %s | None: %s | Missing: %s",
        len(ordered), len(collision), len(simulated), len(none_type), len(missing)
    )
    logger.info("Limits: max_concurrency=%s, upload_slots=50 (multipart=5), zip_tasks=2", max_concurrency)

    client = await create_default_client()
    sem = asyncio.Semaphore(max_concurrency)
    start = time.perf_counter()

    async def _run_one(rid: int):
        async with sem:
            return await _upload_with_retries(
                client=client,
                recid=rid,
                data_dir=data_dir,
                metadata_dir=metadata_dir,
                template_path=template_path,
                retries=max_retries,
                file_concurrency=file_concurrency,
                upload_limiter=upload_limiter,
                zip_sem=zip_sem,
                stats_path=stats_path,
                stats_format=stats_format,
            )

    # print("Debug mode")
    # ordered = ordered[3:9]  # Limit to first x for testing
    tasks = [asyncio.create_task(_run_one(rid)) for rid in ordered]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    failed = [r for r in results if isinstance(r, Exception)]
    logger.info("Bulk finished in %.2fs. Failed: %s", time.perf_counter() - start, len(failed))


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
