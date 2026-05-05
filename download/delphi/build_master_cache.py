# expand master cache using lists/ metadata
import json
from pathlib import Path

MASTER_CACHE_PATH = Path('delphi_records_master.json')
if MASTER_CACHE_PATH.exists():
    with MASTER_CACHE_PATH.open('r', encoding='utf-8') as cache_file:
        master_cache = json.load(cache_file)
else:
    master_cache = {}

def _iter_metadata_records(payload):
    if isinstance(payload, dict):
        yield payload
        for key in ('hits', 'records', 'datasets', 'results', 'entries', 'items'):
            seq = payload.get(key)
            if isinstance(seq, list):
                for item in seq:
                    yield from _iter_metadata_records(item)
    elif isinstance(payload, list):
        for item in payload:
            yield from _iter_metadata_records(item)

def _extract_recid_from_record(record):
    candidates = [
        record.get('recid'),
        record.get('id'),
        (record.get('metadata') or {}).get('recid'),
        (record.get('metadata') or {}).get('id'),
    ]
    for cand in candidates:
        if cand is None:
            continue
        try:
            return int(cand)
        except (TypeError, ValueError):
            continue
    return None

def _normalize_file_entry(raw):
    checksum = raw.get('checksum') or raw.get('checksum_value')
    checksum_type = raw.get('checksum_type')
    if isinstance(checksum, str) and ':' in checksum and not checksum_type:
        checksum_type, checksum = checksum.split(':', 1)
    remote = raw.get('uri') or raw.get('remote') or raw.get('url') or raw.get('link')
    size = raw.get('size') or raw.get('bytes') or raw.get('filesize')
    return {
        'label': raw.get('name') or raw.get('filename') or raw.get('path'),
        'remote': remote,
        'local_path': raw.get('local_path') or raw.get('local'),
        'size': size,
        'checksum_type': checksum_type,
        'checksum': checksum,
        'downloaded': bool(raw.get('downloaded', False)),
    }

def _extract_files(record):
    meta = record.get('metadata') or {}
    files = meta.get('files') or record.get('files') or []
    normalized = []
    for item in files:
        if isinstance(item, dict):
            normalized.append(_normalize_file_entry(item))
    return normalized

records_from_lists = {}
lists_dir = Path('lists')
if lists_dir.exists():
    for path in sorted(lists_dir.glob('*')):
        if not path.is_file():
            continue
        raw_text = path.read_text(encoding='utf-8').strip()
        if not raw_text:
            continue
        decoded = None
        try:
            decoded = json.loads(raw_text)
        except json.JSONDecodeError:
            pass
        if decoded is None:
            for line in raw_text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    decoded = json.loads(line)
                except json.JSONDecodeError:
                    decoded = None
                if decoded is not None:
                    for rec in _iter_metadata_records(decoded):
                        recid = _extract_recid_from_record(rec)
                        if recid is not None and recid not in records_from_lists:
                            records_from_lists[recid] = rec
                    decoded = None
            continue
        for rec in _iter_metadata_records(decoded):
            recid = _extract_recid_from_record(rec)
            if recid is not None and recid not in records_from_lists:
                # if recid == 83931:
                    records_from_lists[recid] = rec

existing_recids = {int(r) for r in master_cache.keys()}
new_recids = sorted(set(records_from_lists.keys()) - existing_recids)

print(f"Parsed records from lists/: {len(records_from_lists)}")
print(f"Existing master-cache recids: {len(existing_recids)}")
print(f"New recids available: {len(new_recids)}")

if new_recids:
    for recid in new_recids:
        files = _extract_files(records_from_lists[recid])
        master_cache[str(recid)] = {
            'recid': recid,
            'done': False,
            'checked': False,
            'files': files,
        }
    with MASTER_CACHE_PATH.open('w', encoding='utf-8') as cache_file:
        json.dump(master_cache, cache_file, indent=2, sort_keys=True)
    print(f"Master cache expanded to {len(master_cache)} recids.")
else:
    print("Master cache already includes every recid found in lists/.")
