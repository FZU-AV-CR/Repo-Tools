#!/usr/bin/env python3
"""Export DELPHI file entries from JSON record lists into a CSV file."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from urllib.parse import urlparse


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    default_lists_dir = script_dir / "lists"
    default_output = script_dir / "delphi_files.csv"

    parser = argparse.ArgumentParser(
        description="List all DELPHI files from JSON lists and export them as CSV."
    )
    parser.add_argument(
        "--lists-dir",
        type=Path,
        default=default_lists_dir,
        help=f"Directory containing source JSON files (default: {default_lists_dir})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=default_output,
        help=f"Output CSV file path (default: {default_output})",
    )
    return parser.parse_args()


def iter_rows(lists_dir: Path):
    for json_path in sorted(lists_dir.glob("*.json")):
        with json_path.open("r", encoding="utf-8") as handle:
            print(f"Processing {json_path}...")
            records = json.load(handle)

        if not isinstance(records, list):
            continue

        for record in records:
            recid = record.get("recid", "") if isinstance(record, dict) else ""
            files = record.get("files", []) if isinstance(record, dict) else []
            if not isinstance(files, list):
                continue

            for file_entry in files:
                if not isinstance(file_entry, dict):
                    continue

                uri = str(file_entry.get("uri", ""))
                file_name = Path(urlparse(uri).path).name if uri else ""
                yield {
                    "recid": recid,
                    "file_name": file_name,
                    "odp_path": uri,
                    "file_size": file_entry.get("size", ""),
                    "checksum": file_entry.get("checksum", ""),
                }


def main() -> int:
    args = parse_args()
    lists_dir = args.lists_dir.resolve()
    output_path = args.output.resolve()

    if not lists_dir.exists() or not lists_dir.is_dir():
        raise SystemExit(f"Lists directory does not exist or is not a directory: {lists_dir}")

    rows = list(iter_rows(lists_dir))
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["recid", "file_name", "odp_path", "file_size", "checksum"],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} file rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

