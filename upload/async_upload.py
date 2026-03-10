import asyncio
import csv
import json
import logging
import time
from pathlib import Path
import zipfile
from contextlib import asynccontextmanager

from yarl import URL

try:
    from nrp_cmd.async_client import get_async_client
    from nrp_cmd.config import Config, RepositoryConfig
    from nrp_cmd.errors import (
        RepositoryCommunicationError,
        RepositoryClientError,
        StructureError
    )
except Exception as exc:
    raise SystemExit(f"Missing NRP async library: {exc}")

logger = logging.getLogger(__name__)
# logger.setLevel(logging.WARNING)

def fill_cern_metadata(metadata, path) -> dict:
    def _first(seq, default=None):
        return seq[0] if isinstance(seq, list) and seq else default

    def _to_year(value):
        if not value:
            return ""
        return str(value)[:4]

    def _parse_energy_range(value):
        # Expect strings like "181-210 GeV"
        if not value or not isinstance(value, str):
            return "", 0, 0
        numbers = []
        current = ""
        for ch in value:
            if ch.isdigit() or ch == ".":
                current += ch
            elif current:
                numbers.append(current)
                current = ""
        if current:
            numbers.append(current)
        if len(numbers) >= 2:
            try:
                return value, float(numbers[0]), float(numbers[1])
            except ValueError:
                return value, 0, 0
        return value, 0, 0

    def _extract_ecms(value):
        # finds patterns like "ecms=191.6" and returns float
        if not value or not isinstance(value, str):
            return None
        lowered = value.lower()
        token = "ecms="
        if token not in lowered:
            return None
        start = lowered.find(token) + len(token)
        num = []
        for ch in lowered[start:]:
            if ch.isdigit() or ch == ".":
                num.append(ch)
            elif num:
                break
        try:
            return float("".join(num)) if num else None
        except ValueError:
            return None

    with open(path, "r", encoding="utf-8") as fh:
        source = json.load(fh)

    src = source.get("metadata", {})
    tgt = metadata.get("metadata", {})

    # Title / publisher / dates
    tgt["title"] = src.get("title", tgt.get("title", ""))
    tgt["publisher"] = src.get("publisher", tgt.get("publisher", ""))
    tgt["publication_date"] = _to_year(src.get("date_published")) or tgt.get("publication_date", "")

    # Record ID
    recid = src.get("recid")
    tgt["recid"] = recid

    # Description: combine key narrative fields
    parts = []
    for key in ("methodology",  "abstract"):
        entry = src.get(key, {})
        desc = entry.get("description") if isinstance(entry, dict) else None
        if desc:
            parts.append(desc.strip())
    if parts:
        tgt["description"] = "\n\n".join(parts)

    # Creators from collaboration
    collab = (src.get("collaboration") or {}).get("name")
    if collab:
        tgt["creators"] = [{
            "person_or_org": {"name": collab, "type": "organizational"}
        }]

    # File types / formats
    formats = (src.get("distribution") or {}).get("formats")
    # always add json
    formats = set(formats or []) | {"json"}
    if formats:
        tgt["file_types"] = [str(f).lower() for f in formats]

    # Subjects from categories + collections
    subjects = []
    primary = (src.get("categories") or {}).get("primary")
    if primary:
        subjects.append({"subject": str(primary)})
    for item in src.get("collections", []):
        subjects.append({"subject": str(item)})
    if subjects:
        tgt["subjects"] = subjects

    # Rights / license
    license_info = src.get("license", {})
    if isinstance(license_info, dict):
        attr = str(license_info.get("attribution", "")).lower()
        if "cc0-1.0" in attr:
            tgt["rights"] = [{"id": "cc0-1.0"}]

    # Identifiers (append DOI/OAI if present)
    identifiers = tgt.get("identifiers", [])
    def _add_identifier(value, scheme):
        if not value:
            return
        if any(i.get("identifier") == value for i in identifiers):
            return
        identifiers.append({"identifier": value, "scheme": scheme})

    # _add_identifier(src.get("doi"), "doi")
    # _add_identifier(((src.get("pids") or {}).get("oai") or {}).get("id"), "oai")
    # if identifiers:
    #     tgt["identifiers"] = identifiers

    # Dates (created)
    created_year = _to_year(_first(src.get("date_created", [])))
    if created_year and tgt.get("dates"):
        tgt["dates"][0]["date"] = created_year

    # Experiment
    exp = _first(src.get("experiment", [])) or collab
    if exp:
        tgt["experiment"] = exp

    # Collision information
    colinfo = src.get("collision_information", {})
    if isinstance(colinfo, dict):
        energy_str, e_min, e_max = _parse_energy_range(colinfo.get("energy"))
        ecms = _extract_ecms((src.get("abstract") or {}).get("description"))
        if ecms is not None:
            energy_str = f"{ecms} GeV"
            e_min = e_max = ecms
        tgt["collision_information"] = {
            "type": colinfo.get("type", tgt.get("collision_information", {}).get("type", "")),
            "energy": energy_str,
            "energy_min": e_min,
            "energy_max": e_max
        }
        # print(tgt["collision_information"])

    # Category
    if primary:
        tgt["category"] = {"id": str(primary).lower()}

    # Dataset type
    secondary = (src.get("type") or {}).get("secondary", [])
    if isinstance(secondary, list) and any(str(s).lower() == "simulated" for s in secondary):
        tgt["dataset_type"] = "simulated"
    elif isinstance(secondary, list) and any(str(s).lower() == "collision" for s in secondary):
        tgt["dataset_type"] = "collision"
    else:
        if "dataset_type" in tgt and tgt["dataset_type"] not in ("Collision", "Simulated"):
            print(f"Warning: dataset_type must be 'Collision' or 'Simulated'. Got '{tgt['dataset_type']}'")

    # Number of events
    num_events = (src.get("distribution") or {}).get("number_events")
    if isinstance(num_events, int):
        tgt["number_of_events"] = num_events

    metadata["metadata"] = tgt
    return metadata


async def create_default_client():
    config = Config()
    config.add_repository(RepositoryConfig(
        alias="physica",
        url=URL("https://test1.physics.du.cesnet.cz/"),
        token="QhrCdMuk4dYOBmIM3frzCOcKTnF28LOvA28MYGEbzZBhPy5wk2nr9thRKZGD",
        verify_tls=True
    ))
    return await get_async_client("physica", config=config)


class WeightedSemaphore:
    def __init__(self, value: int):
        self._value = value
        self._cond = asyncio.Condition()

    async def acquire(self, weight: int = 1):
        async with self._cond:
            while self._value < weight:
                await self._cond.wait()
            self._value -= weight

    async def release(self, weight: int = 1):
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
async def _maybe_sem(sem: asyncio.Semaphore | None):
    if sem is None:
        yield
        return
    async with sem:
        yield


async def _upload_with_limiter(client, record, *, key, metadata, source, transfer_type=None, limiter: WeightedSemaphore | None = None):
    weight = 5 if transfer_type == "M" else 1
    if limiter:
        async with limiter.slot(weight):
            return await client.files.upload(record, key=key, metadata=metadata, source=source, transfer_type=transfer_type)
    return await client.files.upload(record, key=key, metadata=metadata, source=source, transfer_type=transfer_type)


async def _upload_one_file(client, record, file_path: Path, sem: asyncio.Semaphore, limiter: WeightedSemaphore | None = None):
    async with sem:
        transfer_type = "M" if file_path.stat().st_size > 10 * 1024 * 1024 else "L"
        return await _upload_with_limiter(
            client,
            record,
            key=file_path.name,
            metadata={"description": f"File {file_path.name} in DELPHI dataset"},
            source=file_path,
            transfer_type=transfer_type,
            limiter=limiter,
        )


async def _write_stats(stats_path: Path | None, fmt: str, payload: dict):
    if not stats_path:
        return

    def _sync_write():
        stats_path.parent.mkdir(parents=True, exist_ok=True)
        if fmt == "csv":
            write_header = not stats_path.exists()
            with stats_path.open("a", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=sorted(payload.keys()))
                if write_header:
                    writer.writeheader()
                writer.writerow(payload)
        else:  # jsonl
            with stats_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

    await asyncio.to_thread(_sync_write)


async def upload_record_async(
    client,
    recid: int,
    data_dir: Path,
    metadata_dir: Path,
    template_path: Path = Path("delphi_nrp_example.json"),
    file_concurrency: int = 4,
    upload_limiter: WeightedSemaphore | None = None,
    zip_sem: asyncio.Semaphore | None = None,
    stats_path: Path | None = None,
    stats_format: str = "jsonl",
):
    # Used by bulk_async.py as the single-record upload unit
    start = time.perf_counter()
    start_ts = time.time()
    status = "ok"
    error = None
    zip_used = False
    zip_path = None
    dataset_files: list[Path] = []
    try:
        metadata_file = metadata_dir / f"{recid}.json"
        folder = data_dir / f"{recid}"
        if not metadata_file.exists():
            logger.warning("[%s] Missing metadata file: %s", recid, metadata_file)
            return None
        if not folder.exists():
            logger.warning("[%s] Missing dataset folder: %s", recid, folder)
            return None

        dataset_files = [p for p in folder.glob("*") if p.is_file()]
        dataset_size = sum(p.stat().st_size for p in dataset_files if p.exists())
        logger.info("[%s] Found %s files in dataset folder, total size %.2f GB", recid, len(dataset_files), dataset_size / (1024 * 1024 * 1024))

        with open(template_path, "r", encoding="utf-8") as file:
            metadata = json.load(file)

        # Fill metadata template with actual values
        metadata = fill_cern_metadata(metadata, metadata_file)
        # print(json.dumps(metadata, indent=2))

        # Create a new record
        record = await client.records.create(
            metadata,
            model="particles" # , community="my-community-slug", workflow="review"
        )
        logger.info("[%s] Created record: %s", recid, record.id)

        record.metadata["identifiers"][0]["identifier"] = str(record.links.self_html)
        record.metadata["identifiers"][0]["scheme"] = "url"

        if len(dataset_files) > 20:
            record.metadata["file_types"] = record.metadata.get("file_types", []) + ["zip"]

        updated = await client.records.draft_records.update(record)
        logger.info("[%s] Updated to revision: %s", recid, updated.revision_id)

        # Upload a file
        file = await _upload_with_limiter(
            client,
            record,
            key=f"{recid}.json",
            metadata={"description": "The original CERN metadata file for DELPHI dataset"},
            source=metadata_file,
            transfer_type="L",
            limiter=upload_limiter,
        )
        logger.info("[%s] Uploaded metadata: %s", recid, file.key)

        # Upload a folder
        if len(dataset_files) > 20:
            # TODO: switch to streaming zip creation to avoid creating large zip files on disk, ask Cesnet for an example
            # check if zip exists and is up to date, if not create it.
            zip_path = folder / f"{recid}_bundle.zip"
            await ensure_zip_async(dataset_files, zip_path, zip_sem=zip_sem)

            file = await _upload_with_limiter(
                client,
                record,
                key=zip_path.name,
                metadata={"description": f"Zipped dataset files ({len(dataset_files)} files)"},
                source=zip_path,
                transfer_type="M",
                limiter=upload_limiter,
            )

            logger.info("[%s] Uploaded zipped folder: %s", recid, file.key)
        else:
            sem = asyncio.Semaphore(max(1, file_concurrency))
            tasks = [asyncio.create_task(_upload_one_file(client, record, fp, sem, upload_limiter)) for fp in dataset_files]
            await asyncio.gather(*tasks)

        published = await client.records.publish(record)
        logger.info("[%s] Published record: %s (%.2fs)", recid, published.id, time.perf_counter() - start)
        return published
    except Exception as exc:
        status = "failed"
        error = str(exc)
        raise
    finally:
        bytes_uploaded = 0
        try:
            if zip_path and zip_path.exists():
                bytes_uploaded = zip_path.stat().st_size
                zip_used = True
            elif dataset_files:
                bytes_uploaded = sum(p.stat().st_size for p in dataset_files if p.exists())
        except Exception:
            pass

        await _write_stats(
            stats_path,
            stats_format,
            {
                "recid": recid,
                "status": status,
                "error": error,
                "start_ts": start_ts,
                "duration_s": round(time.perf_counter() - start, 3),
                "file_count": len(dataset_files),
                "zip_used": zip_used,
                "bytes_uploaded": bytes_uploaded,
            },
        )


def ensure_zip(dataset_files: list[Path], zip_path: Path):
    existing_files = [p for p in dataset_files if p.exists()]
    if zip_path in existing_files:
        existing_files.remove(zip_path)

    if zip_path.exists():
        if not existing_files:
            logger.info(f"Zip archive exists and originals are already removed: {zip_path}")
            return

        print(zip_path)
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
                    logger.warning(f"Zip archive is has outdated file {fp.name}")
                    updated += 1
                else:
                    fp.unlink()

        logger.info(f"Zip archive updated (fixed in place): {zip_path}, files added: {updated}, originals removed: {len(existing_files) - updated}")
        return

    if not existing_files:
        logger.warning(f"No original files to zip and no zip found: {zip_path}")
        return

    logger.info("Too many files to upload individually, creating a zip archive...")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as zf:
        for fp in existing_files:
            zf.write(fp, arcname=fp.name)
            fp.unlink()

    logger.info(f"Created zip archive: {zip_path}")


async def ensure_zip_async(dataset_files: list[Path], zip_path: Path, zip_sem: asyncio.Semaphore | None = None):
    async with _maybe_sem(zip_sem):
        await asyncio.to_thread(ensure_zip, dataset_files, zip_path)


async def main_async() -> None:
    client = await create_default_client()

    recid = 85104
    metadata_dir = Path("../metadata")
    data_dir = Path("../downloads")

    await upload_record_async(
        client=client,
        recid=recid,
        data_dir=data_dir,
        metadata_dir=metadata_dir,
        template_path=Path("delphi_nrp_example.json"),
        stats_path=Path("upload_stats.jsonl"),
        stats_format="jsonl",
    )


def setup_logging(level=logging.INFO):
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
    for noisy in (
        "nrp_cmd",
        "urllib3",
        "aiohttp",
        "botocore",
        "boto3",
        "asyncio",
    ):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def main() -> None:
    setup_logging()
    asyncio.run(main_async())


if __name__ == "__main__":
    main()

