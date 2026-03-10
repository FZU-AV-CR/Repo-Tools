import argparse
import copy
import csv
import hashlib
import logging
import os
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Dict, Iterable, List, Tuple

import json
import requests

STATS_HEADER = [
    "timestamp",
    "dataset_folder",
    "record_id",
    "file_name",
    "size_bytes",
    "checksum_md5",
    "upload_seconds",
    "upload_speed_Bps",
    "workers",
    "success",
    "remote_size_bytes",
    "remote_checksum",
    "remote_content_link",
    "remote_status",
    "checksum_size_match",
]
_STATS_LOCK = Lock()

__all__ = [
    "STATS_HEADER",
    "append_stat_row",
    "build_headers",
    "build_upload_plan",
    "compute_md5",
    "extract_remote_metadata",
    "list_dataset_folders",
    "load_cern_metadata",
    "load_record_template",
    "prepare_payload",
    "transform_cern_metadata",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload every dataset in /downloads and log upload metrics.")
    parser.add_argument("--token", help="API token (defaults to WORKFLOW_TOKEN env var).")
    parser.add_argument("--base-url", default="https://workflow-repo.test.du.cesnet.cz/api", help="API base URL.")
    parser.add_argument("--downloads", default="downloads", help="Directory containing dataset folders.")
    parser.add_argument("--record-template", default="minimal_record.json", help="JSON template for new records.")
    parser.add_argument("--workers", type=int, default=os.cpu_count() or 4, help="Number of parallel dataset workers.")
    parser.add_argument("--stats-file", default="upload_stats.csv", help="CSV file for per-file statistics.")
    parser.add_argument("--timeout", type=int, default=600, help="HTTP timeout (seconds).")
    parser.add_argument("--metadata-dir", default="metadata", help="Directory with CERN metadata JSON files.")
    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def load_record_template(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as handle:
        return json_load(handle)


def json_load(handle):
    import json

    return json.load(handle)


def compute_md5(file_path: Path) -> str:
    digest = hashlib.md5()
    with file_path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def append_stat_row(stats_path: Path, row: Dict[str, object]) -> None:
    stats_path.parent.mkdir(parents=True, exist_ok=True)
    with _STATS_LOCK:
        file_exists = stats_path.exists()
        with stats_path.open("a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=STATS_HEADER)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)


def load_cern_metadata(metadata_dir: Path, dataset_name: str) -> Dict | None:
    metadata_path = metadata_dir / f"{dataset_name}.json"
    if not metadata_path.exists():
        return None
    with metadata_path.open("r", encoding="utf-8") as handle:
        return json_load(handle)


def _parse_energy_range(energy: str) -> Tuple[int | None, int | None]:
    import re

    numbers = [int(x) for x in re.findall(r"\d+", energy or "")]
    if len(numbers) >= 2:
        return numbers[0], numbers[1]
    if len(numbers) == 1:
        return numbers[0], numbers[0]
    return None, None


def _derive_file_types(record: Dict) -> List[str]:
    formats = set((record.get("../metadata", {}).get("distribution", {}).get("formats") or []))
    for item in record.get("../metadata", {}).get("files", []) or []:
        key = (item.get("key") or "").lower()
        if "." in key:
            formats.add(key.rsplit(".", 1)[-1])
    return sorted({f.lower() for f in formats if f})


def transform_cern_metadata(record: Dict) -> Dict:
    meta = record.get("../metadata", {})
    primary = (meta.get("categories", {}) or {}).get("primary") or ""
    collision = meta.get("collision_information", {}) or {}
    energy = collision.get("energy", "")
    energy_min, energy_max = _parse_energy_range(energy)
    subjects = []
    if primary:
        subject_map = {"higgs": "Higgs physics"}
        subjects.append({"subject": subject_map.get(primary.lower(), f"{primary} physics")})
    if collision.get("type"):
        collision_map = {"e+e-": "Electron–positron collisions"}
        subjects.append({"subject": collision_map.get(collision["type"], collision["type"])})
    creators = [
        {
            "person_or_org": {
                "name": (meta.get("collaboration", {}) or {}).get("name", "DELPHI Collaboration"),
                "type": "organizational",
            }
        }
    ]
    return {
        "metadata": {
            "resource_type": {"id": "dataset"},
            "creators": creators,
            "file_types": _derive_file_types(record),
            "title": meta.get("title", ""),
            "publication_date": meta.get("date_published") or "",
            "publisher": meta.get("publisher", ""),
            "description": (meta.get("abstract", {}) or {}).get("description", ""),
            "subjects": subjects,
            "rights": [{"id": (meta.get("license", {}) or {}).get("attribution", "").lower()}],
            "identifiers": [{"identifier": meta.get("doi", ""), "scheme": "doi"}] if meta.get("doi") else [],
            "dates": [{"date": d, "type": {"id": "created"}} for d in (meta.get("date_created") or [])],
            "experiment": (meta.get("experiment") or [""])[0],
            "collision_information": {
                "type": collision.get("type", ""),
                "energy": energy,
                "energy_min": energy_min,
                "energy_max": energy_max,
            },
            "category": {"id": primary.lower()} if primary else {},
            "dataset_type": (meta.get("type", {}) or {}).get("secondary", [""])[0].lower(),
            "number_of_events": (meta.get("distribution", {}) or {}).get("number_events"),
        },
        "files": {"enabled": True},
    }


def prepare_payload(template: Dict, dataset_name: str, cern_record: Dict | None = None) -> Dict:
    if cern_record:
        payload = transform_cern_metadata(cern_record)
    else:
        payload = copy.deepcopy(template)
        metadata = payload.setdefault("metadata", {})
        title = metadata.get("title", "Dataset")
        metadata["title"] = f"{title} - {dataset_name}"
        metadata["description"] = metadata.get("description", "")[:5000]
    return payload


def build_headers(token: str, content_type: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": content_type,
    }


def list_dataset_folders(download_root: Path) -> List[Path]:
    return sorted([p for p in download_root.iterdir() if p.is_dir()])


def normalize_remote_checksum(raw_value) -> str:
    if isinstance(raw_value, dict):
        raw_value = raw_value.get("checksum") or raw_value.get("value")
    if isinstance(raw_value, str) and ":" in raw_value:
        raw_value = raw_value.split(":", 1)[1]
    return (raw_value or "").strip().lower()


def extract_remote_metadata(commit_payload: Dict) -> Tuple[int, str, str, str]:
    size = commit_payload.get("size") or commit_payload.get("../metadata", {}).get("size") or 0
    checksum = normalize_remote_checksum(
        commit_payload.get("checksum") or commit_payload.get("../metadata", {}).get("checksum")
    )
    content_link = commit_payload.get("links", {}).get("content", "")
    status = commit_payload.get("status") or commit_payload.get("../metadata", {}).get("status", "unknown")
    return int(size), checksum, content_link, status


def _zip_dataset_files(zip_path: Path, files: List[Path]) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file_path in files:
            archive.write(file_path, arcname=file_path.name)


def build_upload_plan(folder: Path, metadata_dir: Path) -> Tuple[List[Path], Path | None, int]:
    dataset_files = [p for p in folder.iterdir() if p.is_file()]
    metadata_path = metadata_dir / f"{folder.name}.json"
    upload_files: List[Path] = []
    zip_path: Path | None = None

    if len(dataset_files) > 20:
        zip_path = folder / f"{folder.name}__bundle.zip"
        _zip_dataset_files(zip_path, dataset_files)
        upload_files.append(zip_path)
    else:
        upload_files.extend(dataset_files)

    if metadata_path.exists():
        upload_files.append(metadata_path)

    return upload_files, zip_path, len(dataset_files)


def upload_dataset(
    folder: Path,
    template: Dict,
    base_url: str,
    token: str,
    stats_path: Path,
    workers_count: int,
    timeout: int,
    metadata_dir: Path,
) -> Tuple[str, bool, int, int, str]:
    logger = logging.getLogger(__name__)
    dataset_name = folder.name
    session = requests.Session()
    json_headers = build_headers(token, "application/json")
    octet_headers = build_headers(token, "application/octet-stream")
    payload = prepare_payload(template, dataset_name)
    base_url = base_url.rstrip("/")
    record_url = f"{base_url}/records"
    files_url = None
    record_id = ""
    bytes_uploaded = 0
    files_uploaded = 0
    zip_path: Path | None = None
    try:
        cern_record = load_cern_metadata(metadata_dir, dataset_name)
        payload = prepare_payload(template, dataset_name, cern_record)
        response = session.post(record_url, headers=json_headers, json=payload, timeout=timeout)
        response.raise_for_status()
        record_id = response.json()["id"]
        logger.info("[%s] Created record %s", dataset_name, record_id)

        files_url = f"{base_url}/records/{record_id}/draft/files"
        upload_files, zip_path, dataset_count = build_upload_plan(folder, metadata_dir)
        if not upload_files:
            raise RuntimeError("No files or metadata to upload in dataset folder.")
        if zip_path:
            logger.info("[%s] Zipped %d files into %s", dataset_name, dataset_count, zip_path.name)

        init_payload = [{"key": f.name} for f in upload_files]
        init_resp = session.post(files_url, headers=json_headers, json=init_payload, timeout=timeout)
        init_resp.raise_for_status()

        for file_path in upload_files:
            checksum = compute_md5(file_path)
            size_bytes = file_path.stat().st_size
            upload_url = f"{files_url}/{file_path.name}/content"
            started = time.perf_counter()
            with file_path.open("rb") as stream:
                upload_resp = session.put(upload_url, headers=octet_headers, data=stream, timeout=timeout)
            upload_resp.raise_for_status()
            duration = max(time.perf_counter() - started, 1e-9)
            speed_bps = size_bytes / duration
            commit_url = f"{files_url}/{file_path.name}/commit"
            commit_resp = session.post(commit_url, headers={"Authorization": f"Bearer {token}"}, timeout=timeout)
            commit_resp.raise_for_status()
            remote_size, remote_checksum, remote_link, remote_status = extract_remote_metadata(commit_resp.json())
            checksum_match = checksum == remote_checksum if remote_checksum else False
            size_match = remote_size == size_bytes if remote_size else False

            append_stat_row(
                stats_path,
                {
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "dataset_folder": dataset_name,
                    "record_id": record_id,
                    "file_name": file_path.name,
                    "size_bytes": size_bytes,
                    "checksum_md5": checksum,
                    "upload_seconds": round(duration, 3),
                    "upload_speed_Bps": round(speed_bps, 2),
                    "workers": workers_count,
                    "success": True,
                    "remote_size_bytes": remote_size,
                    "remote_checksum": remote_checksum,
                    "remote_content_link": remote_link,
                    "remote_status": remote_status,
                    "checksum_size_match": checksum_match and size_match,
                },
            )
            logger.info(
                "[%s] Uploaded %s (%.2f MB @ %.2f MB/s)",
                dataset_name,
                file_path.name,
                size_bytes / (1024**2),
                speed_bps / (1024**2),
            )
            files_uploaded += 1
            bytes_uploaded += size_bytes

        publish_url = f"{base_url}/records/{record_id}/draft/actions/publish"
        publish_resp = session.post(publish_url, headers={"Authorization": f"Bearer {token}"}, timeout=timeout)
        publish_resp.raise_for_status()
        logger.info("[%s] Published record %s", dataset_name, record_id)
        return dataset_name, True, files_uploaded, bytes_uploaded, record_id
    except Exception as exc:
        logger.error("[%s] Failed: %s", dataset_name, exc, exc_info=True)
        append_stat_row(
            stats_path,
            {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "dataset_folder": dataset_name,
                "record_id": record_id or "n/a",
                "file_name": "n/a",
                "size_bytes": 0,
                "checksum_md5": "",
                "upload_seconds": 0,
                "upload_speed_Bps": 0,
                "workers": workers_count,
                "success": False,
                "remote_size_bytes": 0,
                "remote_checksum": "",
                "remote_content_link": "",
                "remote_status": "failed",
                "checksum_size_match": False,
            },
        )
        return dataset_name, False, files_uploaded, bytes_uploaded, record_id or "n/a"
    finally:
        if zip_path and zip_path.exists():
            try:
                zip_path.unlink()
            except OSError as exc:
                logger.warning("[%s] Failed to remove zip %s: %s", dataset_name, zip_path, exc)


def main() -> None:
    args = parse_args()
    configure_logging()
    token = args.token or os.getenv("WORKFLOW_TOKEN")
    if not token:
        raise SystemExit("Missing API token (provide --token or set WORKFLOW_TOKEN).")

    download_root = Path(args.downloads).resolve()
    template_path = Path(args.record_template).resolve()
    stats_path = Path(args.stats_file).resolve()
    metadata_dir = Path(args.metadata_dir).resolve()

    if not download_root.exists():
        raise SystemExit(f"Downloads folder '{download_root}' does not exist.")
    template = load_record_template(template_path)

    dataset_folders = list_dataset_folders(download_root)
    if not dataset_folders:
        logging.info("No dataset folders found in %s", download_root)
        return

    logging.info("Found %d dataset(s); starting %d worker(s).", len(dataset_folders), args.workers)
    results: List[Tuple[str, bool, int, int, str]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                upload_dataset,
                folder,
                template,
                args.base_url,
                token,
                stats_path,
                args.workers,
                args.timeout,
                metadata_dir,
            )
            for folder in dataset_folders
        ]
        for future in as_completed(futures):
            results.append(future.result())

    total_files = sum(r[2] for r in results)
    total_bytes = sum(r[3] for r in results)
    successes = sum(1 for r in results if r[1])
    logging.info(
        "Upload finished: %d/%d datasets succeeded, %d files, %.2f GB total.",
        successes,
        len(results),
        total_files,
        total_bytes / (1024**3),
    )


if __name__ == "__main__":
    main()

