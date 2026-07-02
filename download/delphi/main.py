#!/usr/bin/env python3
"""
Parallel DELPHI dataset downloader using cernopendata-client python modules.

Features:
- Finds DELPHI records via CERN Open Data REST API (includes simulated MC content).
- Saves metadata JSON per record under metadata/<recid>.json
- Downloads all files for each record into data/<recid>/
- Uses cernopendata_client internals (get_record_as_json, get_files_list, download_single_file, ...)
- Parallel file data with ThreadPoolExecutor (I/O bound).
- Robust numeric checksum comparison (fixes leading-zero Adler32 bug).
- Stats written to stats/download_stats.csv and errors to stats/errors.log
- Resume-safe: skips files that already exist with matching size and checksum.

Requirements:
- Python 3.8+
- cernopendata-client installed and importable in your environment
- Requests (used for record listing)
"""

import os
import sys
import json
import time
import csv
import logging
import argparse
import concurrent.futures
from pathlib import Path
from urllib.parse import urlencode

import zlib
import requests

# import internals from cernopendata_client
from cernopendata_client.searcher import get_record_as_json, get_files_list, get_file_info_remote
from cernopendata_client.downloader import download_single_file, check_error
from cernopendata_client.verifier import get_file_info_local, verify_file_info

# --- Configuration ---
# SERVER_HTTP_URI = "https://opendata.cern.ch"
SERVER_HTTP_URI = "https://opendata-qa.cern.ch" # use QA instance based on email from Pablo Saiz
DEFAULT_OUTPUT_DIR = Path("../..")
SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_BASE = DEFAULT_OUTPUT_DIR
OUTPUT_ROOT = OUTPUT_BASE / "data"
METADATA_DIR = OUTPUT_BASE / "metadata"
STATS_DIR = OUTPUT_BASE / "stats"
STATS_CSV = STATS_DIR / "download_stats.csv"
# add date to error log name
ERROR_LOG = STATS_DIR / f"errors{time.strftime('%Y%m%d_%H%M%S')}.log"
MASTER_CACHE_JSON = SCRIPT_DIR / "delphi_records_master.json"
MASTER_CACHE_FIELDS = ["recid", "record_payload", "files_status"]

# REST search URL for more control when requesting big result sets
SEARCH_API = SERVER_HTTP_URI + "/api/records/"

# Defaults
DEFAULT_WORKERS = 12
REQUESTS_PAGE_SIZE = 1000  # fetch large batches; tune if needed
DOWNLOAD_PROTOCOL = "http"  # use "http" (https/http) or "xrootd"
DOWNLOAD_ENGINE_DEFAULT = 'requests'  # (requests/<->xrootd)
RETRY_LIMIT = 3
RETRY_SLEEP = 3

HELP_MESSAGE = f"""
Arguments:
  --workers <int>          Number of parallel download workers (default: {DEFAULT_WORKERS}).
  --protocol <str>         Transfer protocol to use ('http' or 'xrootd').
  --download-engine <str>  Download engine backend (requests, pycurl, or xrootd).
  --retry-limit <int>      Retries per file before failing (default: {RETRY_LIMIT}).
  --retry-sleep <int>      Sleep seconds between retries (default: {RETRY_SLEEP}).
  --max-recid <int>        Limit number of recids processed (testing purposes).
  --output-dir <path>      Base directory for data, metadata, and stats.
  --verify-recid <int>     Manually verify a single recid using remote metadata, then exit.
  -h, --help               Show this help message and exit.
  
Example usage:
    python main.py --workers 12 --protocol xrootd --max-recid 50
"""

# Which records to query: experiment:DELPHI; include everything (simulated flagged in metadata)
SEARCH_QUERY = 'experiment:DELPHI'  # includes simulated datasets

# Create necessary directories
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
METADATA_DIR.mkdir(parents=True, exist_ok=True)
STATS_DIR.mkdir(parents=True, exist_ok=True)

# Setup logging for errors
logging.basicConfig(filename=str(ERROR_LOG), level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger().addHandler(console)


def load_recids_from_cache(cache_path=MASTER_CACHE_JSON):
    if not cache_path.exists():
        return []
    try:
        with open(cache_path, "r", encoding="utf-8") as cache_file:
            cache_data = json.load(cache_file)
        if isinstance(cache_data, dict):
            recids = sorted(
                int(rid)
                for rid in cache_data.keys()
                if str(rid).strip().isdigit()
            )
            if recids:
                logging.info(
                    "Loaded %d recids from cache %s; skipping API.",
                    len(recids),
                    cache_path,
                )
                return recids
        logging.warning("Cache %s contained no valid recids; refetching.", cache_path)
    except Exception:
        logging.exception("Failed to read recid cache %s; refetching via API.", cache_path)
    return []


def load_master_cache_data(cache_path=MASTER_CACHE_JSON):
    if not cache_path.exists():
        return {}
    try:
        with open(cache_path, "r", encoding="utf-8") as cache_file:
            data = json.load(cache_file)
        if isinstance(data, dict):
            return data
        logging.warning("Cache %s is not a dict; ignoring.", cache_path)
    except Exception:
        logging.exception("Failed to read master cache %s; rebuilding.", cache_path)
    return {}


def build_master_cache(server=SERVER_HTTP_URI, query=SEARCH_QUERY, page_size=REQUESTS_PAGE_SIZE,
                       protocol=DOWNLOAD_PROTOCOL, limit=0):
    recids = []
    params = {"q": query, "size": page_size, "from": 1}
    logging.info("Fetching DELPHI recids from %s with query: %s", server, query)
    if limit != 0 and page_size > limit:
        params["size"] = limit
        logging.info("Applying recid limit %d to API page size", limit)
    limit_reached = False
    while True:
        resp = requests.get(SEARCH_API, params=params)
        if resp.status_code == 400:
            try:
                jerr = resp.json()
            except Exception:
                jerr = {}
            msg = (jerr.get("message") or "").lower()
            if "maximum number of 10000 results" in msg:
                logging.warning("API limit of 10000 results reached; stopping early. "
                                "Result set may be truncated. See function docstring for future strategy.")
                limit_reached = True
                break
            resp.raise_for_status()
        resp.raise_for_status()
        j = resp.json()
        hits = j.get("hits", {}).get("hits", [])
        if not hits:
            break
        for h in hits:
            rec_id = h.get("id")
            if isinstance(rec_id, str) and rec_id.isdigit():
                recids.append(int(rec_id))
        logging.info("Listing progress: %d recids collected so far (page offset %s)", len(recids), params["from"])
        total = j.get("hits", {}).get("total", {}) or j.get("hits", {}).get("total")
        params["from"] += page_size
        if params["from"] >= (total or params["from"]):
            break
        if len(recids) >= limit != 0:
            logging.info("Reached recid limit %d; stopping listing.", limit)
            break
    recids = sorted(set(recids))
    cache_payload = {}
    total_files = 0
    for recid in recids:
        record_json = {}
        file_locations_info = []
        for attempt in range(RETRY_LIMIT):
            try:
                record_json = get_record_as_json(server, recid, None, None)
                save_metadata(recid, record_json)
                file_locations_info = get_files_list(server, record_json, protocol, True)
                break
            except Exception:
                logging.exception("Failed to collect metadata/files for recid %s", recid)
            except SystemExit as exc:
                logging.error("Invalid recid %s: %s", recid, exc)
            if attempt == RETRY_LIMIT - 1:
                logging.error("Giving up on recid %s after %d attempts", recid, RETRY_LIMIT)
            else:
                time.sleep(RETRY_SLEEP)
        file_entries = []
        for location, size, checksum in file_locations_info:
            try:
                size_int = int(size)
            except (TypeError, ValueError):
                size_int = None
            local_path = OUTPUT_ROOT / str(recid) / Path(location).name
            file_entries.append({
                "remote": location,
                "path": str(local_path),
                "size": size_int,
                "checksum": checksum,
                "downloaded": False,
            })
        total_files += len(file_entries)
        cache_payload[str(recid)] = {
            "record": record_json,
            "done": False,
            "checked": False,
            "files": file_entries,
        }
    if cache_payload:
        try:
            with open(MASTER_CACHE_JSON, "w", encoding="utf-8") as cache_file:
                json.dump(cache_payload, cache_file, indent=2)
            logging.info(
                "Cached %d records (%d files described) into %s",
                len(cache_payload),
                total_files,
                MASTER_CACHE_JSON,
            )
        except Exception:
            logging.exception("Unable to persist recid cache to %s", MASTER_CACHE_JSON)
    else:
        logging.warning("No cache entries were created; verify API connectivity.")
    if limit_reached:
        logging.info("Found %d recids (truncated by 10k API cap).", len(recids))
    else:
        logging.info("Found %d recids", len(recids))
    return cache_payload


def fetch_all_delphi_recids(server=SERVER_HTTP_URI, query=SEARCH_QUERY, page_size=REQUESTS_PAGE_SIZE,
                            protocol=DOWNLOAD_PROTOCOL, limit=0):
    """
    Query the REST API and return a sorted list of recid integers for query.
    Uses the public API /api/records/ with a large size value to attempt to get all results.

    NOTE: API hard limit: when more than 10000 results exist the server responds:
      {"status": 400, "message": "Maximum number of 10000 results have been reached."}
    Current handling: log warning, stop early (results may be truncated).
    Master cache: delphi_records_master.json (next to this script) stores each recid’s payload plus done/checked flags
    and per-file metadata (local path, remote link, size, checksum, downloaded flag). This function refreshes the
    downloaded flags, saves the cache back to disk, and returns only the recids that still need downloading.
    """
    cache_data = load_master_cache_data()
    if not cache_data or len(cache_data) == 0:
        cache_data = build_master_cache(server=server, query=query, page_size=page_size, protocol=protocol, limit=limit)
    if not cache_data:
        logging.error("Master cache is empty; nothing to process.")
        return []

    updated = False
    pending_recids = []
    sorted_entries = sorted(
        cache_data.items(),
        key=lambda kv: int(kv[0]) if str(kv[0]).isdigit() else kv[0],
    )
    recid_keys = []
    done_skipped = 0
    for recid_str, entry in sorted_entries:
        if entry.get("done"):
            done_skipped += 1
            continue
        recid_keys.append(recid_str)
    if done_skipped:
        logging.info("Skipping %d recids already marked done in cache.", done_skipped)
    if not recid_keys:
        logging.info("All %d recids already done; nothing left to download.", done_skipped)
        return []

    total_recids = len(recid_keys)
    progress_step = max(1, total_recids // 20)
    done_during_refresh = 0
    for idx, recid_str in enumerate(recid_keys, start=1):
        if idx % progress_step == 0 or idx == total_recids:
            pct = (idx / total_recids) * 100 if total_recids else 100
            logging.info("Cache refresh progress: %d/%d (%.1f%%)", idx, total_recids, pct)
        # If recid folder doesn't exist, mark all files as not downloaded
        if not (OUTPUT_ROOT / recid_str).exists():
            entry = cache_data[recid_str]
            # files = entry.get("files") or []
            # for file_entry in files:
            #     if file_entry.get("downloaded") != False:
            #         file_entry["downloaded"] = False
            #         updated = True
            if entry.get("done") != False:
                entry["done"] = False
                updated = True
            pending_recids.append(int(recid_str))
            continue

        entry = cache_data[recid_str]
        files = entry.get("files") or []
        all_downloaded = bool(files)
        for file_entry in files:
            local_path = Path(file_entry.get("path") or (OUTPUT_ROOT / recid_str / Path(file_entry.get("remote", "")).name))
            size = file_entry.get("size")
            checksum = file_entry.get("checksum")
            downloaded = already_downloaded(local_path, expected_size=size, expected_checksum=checksum)
            if file_entry.get("downloaded") != downloaded:
                file_entry["downloaded"] = downloaded
                updated = True
            if not downloaded:
                all_downloaded = False
        if entry.get("done") != all_downloaded:
            entry["done"] = all_downloaded
            updated = True
        if not entry.get("checked", False):
            entry["checked"] = True
            updated = True
        if not all_downloaded:
            try:
                pending_recids.append(int(recid_str))
            except ValueError:
                logging.warning("Invalid recid key '%s' in cache; skipping.", recid_str)
        else:
            done_during_refresh += 1


    pending_recids.sort()
    if updated:
        try:
            with open(MASTER_CACHE_JSON, "w", encoding="utf-8") as cache_file:
                json.dump(cache_data, cache_file, indent=2)
            logging.info("Updated master cache progress at %s", MASTER_CACHE_JSON)
        except Exception:
            logging.exception("Failed to update master cache %s", MASTER_CACHE_JSON)

    logging.info(
        "Cache filter summary: %d recids skipped (already done), %d newly completed, %d pending (of %d total).",
        done_skipped,
        done_during_refresh,
        len(pending_recids),
        len(cache_data),
    )
    return pending_recids


def save_metadata(recid, record_json):
    path = METADATA_DIR / f"{recid}.json"
    with open(path, "w") as f:
        json.dump(record_json, f, indent=2)
    logging.debug("Saved metadata to %s", path)


def parse_remote_file_info(file_locations_info, file_location):
    """
    file_locations_info as returned by get_files_list(server, record_json, protocol, expand)
    The CLI expects each entry as (location, size, checksum).
    This helper finds and returns (name, size, checksum_string) for a given file_location.
    """
    for i in file_locations_info:
        if i[0] == file_location:
            return i  # (location, size, checksum)
    # fallback: maybe file_locations_info contains absolute or other variations
    for i in file_locations_info:
        if Path(i[0]).name == Path(file_location).name:
            return i
    return None


def parse_checksum_numeric(checksum_str):
    """
    Accepts strings like "adler32:03d9681c" or "adler32:3d9681c" or "md5:...."
    Returns tuple (alg, int_value) where int_value is numeric representation (for hex-based).
    If algorithm is not hex-based (e.g., md5), returns (alg, None, hexstr).
    """
    if not checksum_str:
        return (None, None, None)
    try:
        alg, val = checksum_str.split(":", 1)
    except ValueError:
        return (None, None, checksum_str)
    alg = alg.lower()
    val = val.strip()
    if alg in ("adler32",):
        # adler32 fits into 32-bit
        try:
            ival = int(val, 16)
            return (alg, ival, val.lower().zfill(8))
        except ValueError:
            return (alg, None, val)
    else:
        # non-numeric or larger hex (md5) - keep raw string
        return (alg, None, val)


def compute_adler32_of_file(path):
    """
    Read file in streaming mode and compute adler32 numeric value (32-bit unsigned).
    TODO: since the checksum is fixed we can switch back to the client
    """
    bufsize = 1 << 20
    a = 1  # initial adler32 value for zlib.adler32 when passing data incrementally
    with open(path, "rb") as f:
        while True:
            data = f.read(bufsize)
            if not data:
                break
            a = zlib.adler32(data, a)
    return a & 0xFFFFFFFF


def already_downloaded(target_path, expected_size=None, expected_checksum=None):
    """
    Quick resume check: if file exists and matches expected size and checksum (numeric), skip download.
    expected_checksum: string like "adler32:03d..." or None
    """
    if not target_path.exists():
        return False
    if expected_size is not None:
        try:
            got_size = target_path.stat().st_size
            if got_size != expected_size:
                return False
        except Exception:
            return False
    if expected_checksum:
        alg, num, raw = parse_checksum_numeric(expected_checksum)
        if alg == "adler32" and num is not None:
            try:
                got_num = compute_adler32_of_file(target_path)
                return got_num == num
            except Exception:
                return False
        else:
            # for non-adler checks, fallback to string compare of stored checksum if available
            return True  # can't robustly check here; assume OK if size matches
    return True  # exists and either no expectations or size matched


def download_worker(task):
    """
    task: dict with keys recid, file_location, protocol, download_engine, retry_limit, retry_sleep
    Returns a stats dict for CSV writing
    """
    recid = task["recid"]
    file_location = task["file_location"]
    protocol = task["protocol"]
    download_engine = task.get("download_engine", DOWNLOAD_ENGINE_DEFAULT)
    retry_limit = task.get("retry_limit", RETRY_LIMIT)
    retry_sleep = task.get("retry_sleep", RETRY_SLEEP)

    recid_dir = OUTPUT_ROOT / str(recid)
    recid_dir.mkdir(parents=True, exist_ok=True)

    filename = Path(file_location).name
    target_path = recid_dir / filename
    attempt = 0

    # fetch remote file info (size/checksum) using client helper:
    try:
        # TODO: just pass the info from master cache
        remote_info_list = get_file_info_remote(SERVER_HTTP_URI, recid, protocol=protocol, filtered_files=[file_location])
        if remote_info_list:
            # remote_info_list likely returns dict keyed by names or list. Try both.
            # standard expected format in CLI is a list of dicts or tuples; handle common cases:
            if isinstance(remote_info_list, dict):
                # sometimes returns dict keyed by filename
                if filename in remote_info_list:
                    remote_entry = remote_info_list[filename]
                    remote_size = int(remote_entry.get("size", 0))
                    remote_checksum = remote_entry.get("checksum")
                else:
                    # maybe the function returned mapping by location
                    key = list(remote_info_list.keys())[0]
                    remote_entry = remote_info_list[key]
                    remote_size = int(remote_entry.get("size", 0))
                    remote_checksum = remote_entry.get("checksum")
            elif isinstance(remote_info_list, (list, tuple)) and remote_info_list:
                # accept either a list of 3-tuples or list of dicts
                elem = remote_info_list[0]
                if isinstance(elem, (list, tuple)) and len(elem) >= 3:
                    # (location, size, checksum)
                    _, remote_size, remote_checksum = elem[0], int(elem[1]), elem[2]
                elif isinstance(elem, dict):
                    remote_size = int(elem.get("size", 0))
                    remote_checksum = elem.get("checksum")
                else:
                    remote_size = None
                    remote_checksum = None
            else:
                remote_size = None
                remote_checksum = None
        else:
            remote_size = None
            remote_checksum = None
    except Exception:
        logging.exception("Failed to get remote file info for recid %s file %s", recid, file_location)
        remote_size = None
        remote_checksum = None
    except SystemExit as exc:
        logging.error("Invalid recid %s for %s: %s", recid, file_location, exc)
        return {
            "recid": recid,
            "file": str(target_path),
            "bytes": None,
            "start": None,
            "end": None,
            "duration_s": None,
            "rate_Bps": None,
            "success": False,
            "error": "invalid recid",
            "expected_size": None,
            "expected_checksum": None,
            "computed_checksum_numeric": None,
            "checksum_ok": False,
            "attempts": attempt,
        }

    # resume check
    if already_downloaded(target_path, expected_size=remote_size, expected_checksum=remote_checksum):
        logging.info("SKIP (already downloaded) %s/%s", recid, filename)
        return {
            "recid": recid,
            "file": str(target_path),
            "bytes": target_path.stat().st_size,
            "start": None,
            "end": None,
            "duration_s": 0.0,
            "rate_Bps": None,
            "success": True,
            "error": "SKIP (already downloaded)",
            "expected_size": remote_size,
            "expected_checksum": remote_checksum,
            "computed_checksum_numeric": None,
            "checksum_ok": True,
            "attempts": attempt,
        }

    # attempt download with retries
    attempt = 0
    last_exception = None
    start_time = None
    end_time = None
    success = False
    error_msg = ""
    bytes_written = None
    computed_checksum_numeric = None
    checksum_ok = False

    while attempt <= retry_limit and not success:
        attempt += 1
        try:
            logging.info("Downloading recid %s file %s (attempt %d)", recid, filename, attempt)
            start_time = time.time()
            # if the file exists from previous attempt, remove it before retrying
            if target_path.exists():
                target_path.unlink()
            # call the library downloader
            download_single_file(
                path=str(recid_dir),
                file_location=file_location,
                protocol=protocol,
                download_engine=download_engine,
            )
            end_time = time.time()
            success = True
        except Exception as e:
            last_exception = e
            error_msg = f"download failed attempt {attempt}: {e}"
            logging.warning("%s (recid %s file %s)", error_msg, recid, filename)
            time.sleep(retry_sleep)

        # post-download: check file size and checksum
        try:
            bytes_written = target_path.stat().st_size
        except Exception:
            bytes_written = None

        if remote_size:
            if bytes_written != remote_size:
                logging.warning("Size mismatch for %s/%s: expected %s, got %s", recid, filename, remote_size,
                                bytes_written)
                success = False
                continue
            else:
                logging.info("Size match for %s/%s: %s bytes", recid, filename, bytes_written)

        # compute adler32 numeric if remote checksum is adler32
        if remote_checksum:
            alg, num, rawz = parse_checksum_numeric(remote_checksum)
            if alg == "adler32" and num is not None:
                try:
                    computed_checksum_numeric = compute_adler32_of_file(target_path)
                    checksum_ok = (computed_checksum_numeric == num)
                    if not checksum_ok:
                        logging.warning("Checksum mismatch for %s/%s: expected %08x, got %08x",
                                        recid, filename, num, computed_checksum_numeric)
                        success = False
                        continue
                    else:
                        logging.info("Checksum match for %s/%s: %08x", recid, filename, computed_checksum_numeric)
                except Exception:
                    checksum_ok = False
                    success = False
                    continue
            else:
                # for non-adler algorithm, we don't compute here; attempt to use library verifier to compare if possible
                computed_checksum_numeric = None
                checksum_ok = None

                # also call verify_file_info if remote info available and file present (this uses verifier module)
                if remote_checksum and alg != "adler32":
                    try:
                        # get local file info representation and remote info and run their verify routine (it may use formatting internally)
                        file_info_local = get_file_info_local(recid_dir)
                        file_info_remote = get_file_info_remote(SERVER_HTTP_URI, recid, protocol=protocol,
                                                                filtered_files=[file_location])
                        # verify_file_info will raise or log; wrap in try/except
                        try:
                            verify_file_info(file_info_local, file_info_remote)
                        except Exception:
                            # if verify_file_info throws due to formatting bug, we've already done numeric check above
                            logging.debug("verify_file_info raised (non-fatal) for %s/%s", recid, filename)
                            success = False
                            continue
                    except Exception:
                        logging.debug("verify step failed (non-fatal) for %s/%s", recid, filename)
                        success = False
                        continue

    if not success:
        logging.exception("Giving up on %s/%s after %d attempts", recid, filename, attempt)
        return {
            "recid": recid,
            "file": str(target_path),
            "bytes": None,
            "start": start_time,
            "end": end_time,
            "duration_s": None,
            "rate_Bps": None,
            "success": False,
            "error": str(last_exception),
            "expected_size": remote_size,
            "expected_checksum": remote_checksum,
            "computed_checksum_numeric": None,
            "checksum_ok": False,
            "attempts": attempt,
        }

    duration_s = (end_time - start_time) if (start_time and end_time) else None
    rate_Bps = (bytes_written / duration_s) if (bytes_written and duration_s and duration_s > 0) else None

    return {
        "recid": recid,
        "file": str(target_path),
        "bytes": bytes_written,
        "start": start_time,
        "end": end_time,
        "duration_s": duration_s,
        "rate_Bps": rate_Bps,
        "success": True,
        "error": "",
        "expected_size": remote_size,
        "expected_checksum": remote_checksum,
        "computed_checksum_numeric": computed_checksum_numeric,
        "checksum_ok": checksum_ok,
        "attempts": attempt,
    }


def write_stats_header_if_missing(csv_path):
    if not csv_path.exists():
        with open(csv_path, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=[
                "recid", "file", "bytes", "start", "end", "duration_s", "rate_Bps",
                "success", "error", "expected_size", "expected_checksum",
                "computed_checksum_numeric", "checksum_ok", "attempts"
            ])
            writer.writeheader()


def append_stats(csv_path, stats):
    write_stats_header_if_missing(csv_path)
    with open(csv_path, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=[
            "recid", "file", "bytes", "start", "end", "duration_s", "rate_Bps",
            "success", "error", "expected_size", "expected_checksum",
            "computed_checksum_numeric", "checksum_ok", "attempts"
        ])
        writer.writerow(stats)


def build_tasks_for_recid(recid, protocol=DOWNLOAD_PROTOCOL, download_engine=DOWNLOAD_ENGINE_DEFAULT,
                          retry_limit=RETRY_LIMIT, retry_sleep=RETRY_SLEEP, expand=True, cached_metadata=None):
    """
    Uses library functions to get the record JSON and file locations, returns a list of task dicts.
    """
    try:
        metadata_path = METADATA_DIR / f"{recid}.json"
        record_json = None
        if metadata_path.exists():
            try:
                with open(metadata_path, "r") as f:
                    record_json = json.load(f)
                logging.info("Reusing cached metadata for recid %s", recid)
            except Exception:
                logging.warning("Failed to load cached metadata for recid %s, refetching.", recid)
                record_json = None
        if cached_metadata:
            file_locations = cached_metadata["remote"]
        else:
            if record_json is None:
                logging.info("Fetching metadata for recid %s", recid)
                record_json = get_record_as_json(SERVER_HTTP_URI, recid, None, None)
                save_metadata(recid, record_json)
            file_locations_info = get_files_list(SERVER_HTTP_URI, record_json, protocol, expand)
            # get_files_list returns a list of tuples (location, size, checksum)
            file_locations = [f[0] for f in file_locations_info]
        tasks = []
        for fl in file_locations:
            tasks.append({
                "recid": recid,
                "file_location": fl,
                "protocol": protocol,
                "download_engine": download_engine,
                "retry_limit": retry_limit,
                "retry_sleep": retry_sleep,
            })
        return tasks
    except Exception:
        logging.exception("Failed to build tasks for recid %s", recid)
        return []


def verify_single_recid(recid, protocol=DOWNLOAD_PROTOCOL):
    logging.info("Manual verification started for recid %s", recid)
    try:
        record_json = get_record_as_json(SERVER_HTTP_URI, recid, None, None)
        file_locations_info = get_files_list(SERVER_HTTP_URI, record_json, protocol, True)
    except Exception:
        logging.exception("Unable to fetch fresh metadata for recid %s", recid)
        return False
    total_files = len(file_locations_info)
    if not total_files:
        logging.warning("Recid %s contains no files; nothing to verify.", recid)
        return False
    recid_dir = OUTPUT_ROOT / str(recid)
    summary = {"verified": 0, "missing": 0, "size_mismatch": 0, "checksum_mismatch": 0, "errors": 0}
    start_ts = time.time()
    for idx, (location, size, checksum) in enumerate(file_locations_info, start=1):
        pct = (idx / total_files) * 100
        filename = Path(location).name
        logging.info("Verification progress %d/%d (%.1f%%) - %s", idx, total_files, pct, filename)
        local_path = recid_dir / filename
        if not local_path.exists():
            summary["missing"] += 1
            logging.warning("Missing file for recid %s: %s", recid, filename)
            continue
        try:
            expected_size = int(size) if size is not None else None
        except (TypeError, ValueError):
            expected_size = None
        if expected_size is not None and local_path.stat().st_size != expected_size:
            summary["size_mismatch"] += 1
            logging.warning("Size mismatch for %s/%s (expected %s, got %s)", recid, filename, expected_size,
                            local_path.stat().st_size)
            continue
        if checksum:
            alg, num, _ = parse_checksum_numeric(checksum)
            if alg == "adler32" and num is not None:
                try:
                    computed = compute_adler32_of_file(local_path)
                except Exception:
                    summary["errors"] += 1
                    logging.exception("Checksum computation failed for %s/%s", recid, filename)
                    continue
                if computed != num:
                    summary["checksum_mismatch"] += 1
                    logging.warning("Checksum mismatch for %s/%s (expected %08x, got %08x)", recid, filename, num,
                                    computed)
                    continue
        summary["verified"] += 1
    duration = time.time() - start_ts
    logging.info(
        "Verification summary for recid %s: verified=%d, missing=%d, size_mismatch=%d, checksum_mismatch=%d, errors=%d (%.2fs)",
        recid,
        summary["verified"],
        summary["missing"],
        summary["size_mismatch"],
        summary["checksum_mismatch"],
        summary["errors"],
        duration,
    )
    return all(summary[key] == 0 for key in ("missing", "size_mismatch", "checksum_mismatch", "errors"))


def main(args):
    recids = fetch_all_delphi_recids(page_size=REQUESTS_PAGE_SIZE, protocol=args.protocol, limit=args.max_recids)
    if args.max_recids:
        recids = recids[: args.max_recids]

    if args.protocol == "xrootd" and args.download_engine != "xrootd":
        logging.info("Using xrootd protocol")
        args.download_engine = "xrootd"

    # prepare CSV header
    write_stats_header_if_missing(STATS_CSV)

    # load master cache again for file paths
    cache_data = load_master_cache_data()

    # create a master task list
    master_tasks = []
    for recid in recids:
        tasks = build_tasks_for_recid(
            recid,
            protocol=args.protocol,
            download_engine=args.download_engine,
            retry_limit=args.retry_limit,
            retry_sleep=args.retry_sleep,
            cached_metadata=cache_data[recid]
        )
        master_tasks.extend(tasks)

    total_tasks = len(master_tasks)
    logging.info("Total files to download (tasks): %d", total_tasks)
    if not total_tasks:
        logging.warning("No files to download; exiting.")
        return

    completed_tasks = 0

    # parallel download pool
    workers = args.workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_task = {executor.submit(download_worker, t): t for t in master_tasks}
        for future in concurrent.futures.as_completed(future_to_task):
            task = future_to_task[future]
            completed_tasks += 1
            pct = (completed_tasks / total_tasks) * 100
            logging.info("Progress: %d/%d (%.1f%%)", completed_tasks, total_tasks, pct)
            try:
                stats = future.result()
                append_stats(STATS_CSV, stats)
                if not stats.get("success", False):
                    logging.error("Failed download: %s", stats)
                # Update master cache downloaded flag?
            except Exception as e:
                logging.exception("Task raised an exception: %s (task %s)", e, task)
                append_stats(STATS_CSV, {
                    "recid": task["recid"],
                    "file": task["file_location"],
                    "bytes": None,
                    "start": None,
                    "end": None,
                    "duration_s": None,
                    "rate_Bps": None,
                    "success": False,
                    "error": str(e),
                    "expected_size": None,
                    "expected_checksum": None,
                    "computed_checksum_numeric": None,
                    "checksum_ok": False,
                    "attempts": None,
                })

    logging.info("Progress: %d/%d (100.0%%) - all data processed", total_tasks, total_tasks)
    logging.info("All tasks finished. Stats written to %s", STATS_CSV)


def configure_output_paths(base_dir):
    global OUTPUT_BASE, OUTPUT_ROOT, METADATA_DIR, STATS_DIR, STATS_CSV, ERROR_LOG
    OUTPUT_BASE = Path(base_dir).expanduser()
    OUTPUT_ROOT = OUTPUT_BASE / "data"
    METADATA_DIR = OUTPUT_BASE / "metadata"
    STATS_DIR = OUTPUT_BASE / "stats"
    STATS_CSV = STATS_DIR / "download_stats.csv"
    ERROR_LOG = STATS_DIR / f"errors{time.strftime('%Y%m%d_%H%M%S')}.log"
    for directory in (OUTPUT_ROOT, METADATA_DIR, STATS_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def setup_logging(error_log_path):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
    file_handler = logging.FileHandler(error_log_path)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console)


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Parallel DELPHI dataset downloader using cernopendata_client internals.",
        add_help=False,
    )
    p.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Number of parallel download workers.")
    p.add_argument("--protocol", choices=["http", "xrootd"], default="http", help="Protocol to download files (http/xrootd).")
    p.add_argument("--download-engine", choices=["requests", "pycurl", "xrootd"], default=requests, help="download engine preference.")
    p.add_argument("--retry-limit", type=int, default=RETRY_LIMIT, help="Retries per file.")
    p.add_argument("--retry-sleep", type=int, default=RETRY_SLEEP, help="Seconds to sleep between retries.")
    p.add_argument("--max-recid", dest="max_recids", type=int, default=None, help="Limit number of recids to process (for tests).")
    p.add_argument(
        "--output-dir",
        type=str,
        default=str(DEFAULT_OUTPUT_DIR),
        help="Base directory for all outputs (data/metadata/stats).",
    )
    p.add_argument(
        "--verify-recid",
        type=int,
        default=None,
        help="Verify an already-downloaded recid against live metadata and exit.",
    )
    p.add_argument(
        "-h",
        "--help",
        action="store_true",
        help="Show detailed descriptions for all arguments and exit.",
    )
    args = p.parse_args()

    if args.help:
        print(HELP_MESSAGE.strip())
        sys.exit(0)

    configure_output_paths(args.output_dir)
    setup_logging(ERROR_LOG)

    if args.verify_recid is not None:
        success = verify_single_recid(args.verify_recid, protocol=args.protocol)
        sys.exit(0 if success else 2)

    try:
        main(args)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user; exiting.")
        sys.exit(1)
