#!/usr/bin/env python3
"""Stratified sampler for metadata records in /lists.

Sample 50 records while preserving (approximately) the relative
representation of:
  - categories.primary
  - date_created
  - distribution.number_events   (binned by quantiles)
  - distribution.number_files    (binned by quantiles)
  - type.secondary

Additionally, the sampler guarantees that **at least one record** is
selected for every unique value of every stratification feature
(coverage‑first allocation).  When the sample budget is too small to
cover every unique value, the rarest values are prioritised.
"""

import argparse
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Stratified metadata sampler")
    p.add_argument(
        "--folder",
        default="lists",
        help="Directory containing source JSON file(s)",
    )
    p.add_argument(
        "-n",
        "--sample-size",
        type=int,
        default=50,
        help="Number of records to sample",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_records(folder: Path):
    """Load JSON objects from all .json files in *folder*."""
    records = []
    if not folder.exists():
        raise SystemExit(f"Folder not found: {folder}")
    for path in folder.iterdir():
        if path.is_file() and path.suffix.lower() == ".json":
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    records.extend(data)
                elif isinstance(data, dict):
                    records.append(data)
            except Exception as exc:
                print(f"Warning: skipping {path} ({exc})", file=sys.stderr)
    return records


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------
def extract_features(rec: dict):
    """Pull the stratification keys and recid out of a raw record."""
    cats = rec.get("categories", {})
    cat = cats.get("primary", "Unknown") if isinstance(cats, dict) else "Unknown"

    dc = rec.get("date_created", [])
    if isinstance(dc, list) and len(dc):
        date = str(dc[0])
    elif isinstance(dc, str):
        date = dc
    else:
        date = "Unknown"

    dist = rec.get("distribution", {})
    if isinstance(dist, dict):
        num_events = dist.get("number_events", None)
        num_files = dist.get("number_files", None)
    else:
        num_events = None
        num_files = None

    typ = rec.get("type", {})
    if isinstance(typ, dict):
        sec = typ.get("secondary", [])
        if isinstance(sec, list):
            type_sec = ", ".join(str(s) for s in sec)
        else:
            type_sec = str(sec) if sec is not None else "Unknown"
    else:
        type_sec = "Unknown"

    return {
        "recid": rec.get("recid"),
        "cat": cat,
        "date": date,
        "num_events": num_events,
        "num_files": num_files,
        "type_sec": type_sec,
    }


# ---------------------------------------------------------------------------
# Binning
# ---------------------------------------------------------------------------
def quantile_bin(series: pd.Series, q: int = 5):
    """Convert a numeric series into quantile-based string labels."""
    s = pd.to_numeric(series, errors="coerce")
    if s.isna().all():
        return pd.Series(["Unknown"] * len(s), index=s.index)

    if s.nunique(dropna=True) <= q:
        labels = s.apply(lambda x: "Unknown" if pd.isna(x) else str(int(x)))
        return labels

    try:
        binned = pd.qcut(s, q=q, duplicates="drop")
        labels = binned.astype(str)
    except ValueError:
        labels = pd.cut(s, bins=q).astype(str)

    labels[s.isna()] = "Unknown"
    return labels


# ---------------------------------------------------------------------------
# Proportional allocation helper
# ---------------------------------------------------------------------------
def allocate_proportional(counts: pd.Series, n: int):
    """Allocate *n* samples across strata proportionally to *counts*."""
    total = counts.sum()
    if total == 0:
        return counts.copy()

    exact = counts / total * n
    alloc = exact.apply(math.floor)
    remaining = int(n - alloc.sum())

    if remaining > 0:
        frac = exact - alloc
        room = counts - alloc
        order = frac.sort_values(ascending=False).index
        for st in order:
            if remaining <= 0:
                break
            if room[st] <= 0:
                continue
            add = min(remaining, int(room[st]))
            alloc[st] += add
            remaining -= add

        if remaining > 0:
            for st in counts.index:
                if remaining <= 0:
                    break
                add = min(remaining, counts[st] - alloc[st])
                alloc[st] += add
                remaining -= add

    return alloc.astype(int)


# ===========================================================================
#  Coverage‑first sampling (NEW)
# ===========================================================================
def _greedy_coverage(
    df: pd.DataFrame,
    feature_cols: list[str],
    budget: int,
    rng: np.random.RandomState,
):
    """Select up to *budget* records covering every unique feature value.

    Returns
    -------
    selected : list of index labels
        Indices of the records chosen in the coverage pass.
    remaining_budget : int
        How many sample slots are still available after covering.
    """
    if budget <= 0:
        return [], 0

    # (feature_name, value) -> set of row indices that have it
    val_to_indices: dict[tuple[str, str], set] = {}
    for col in feature_cols:
        for val, grp in df.groupby(col):
            val_to_indices[(col, val)] = set(grp.index)

    uncovered = set(val_to_indices.keys())
    selected: set = set()

    while uncovered and len(selected) < budget:
        # ---- find the rarest still-uncovered feature value ----
        best_key = None
        best_available = float("inf")
        for key in uncovered:
            available = val_to_indices[key] - selected
            if available and len(available) < best_available:
                best_available = len(available)
                best_key = key

        if best_key is None:
            break  # no uncovered value has any remaining record

        # ---- pick one record from the candidates ----
        candidates = list(val_to_indices[best_key] - selected)

        # score each candidate by how many *other* uncovered values it covers
        scored = []
        for idx in candidates:
            cov = sum(
                1 for k in uncovered if idx in val_to_indices.get(k, set())
            )
            scored.append((cov, idx))
        # sort by coverage desc, then randomly shuffle ties for reproducibility
        scored.sort(key=lambda x: (-x[0], rng.random()))
        picked = scored[0][1]

        selected.add(picked)

        # ---- remove all newly covered values from *uncovered* ----
        for key in list(uncovered):
            if picked in val_to_indices.get(key, set()):
                uncovered.discard(key)

    return list(selected), budget - len(selected)


# ===========================================================================
#  Main stratified sampler (modified)
# ===========================================================================
def proportional_stratified_sample(
    df: pd.DataFrame,
    strata_cols: list[str],
    n: int,
    random_state: int,
):
    """Return a DataFrame with a coverage‑first stratified sample of size *n*."""
    N = len(df)
    if N <= n:
        return df.copy()

    rng = np.random.RandomState(random_state)

    # ---- Phase 1 : guarantee every unique feature value appears ----
    coverage_idx, remaining_budget = _greedy_coverage(
        df, strata_cols, n, rng
    )

    # ---- Phase 2 : proportional fill with what is left ----
    if remaining_budget > 0:
        # remove records already taken
        remaining_df = df.drop(index=coverage_idx, errors="ignore")
    else:
        remaining_df = pd.DataFrame(columns=df.columns)

    # Build stratum labels on the *remaining* pool
    remaining_df = remaining_df.copy()
    remaining_df["_stratum"] = remaining_df[strata_cols].astype(str).agg(
        "|".join, axis=1
    )

    counts = remaining_df["_stratum"].value_counts().sort_index()
    alloc = allocate_proportional(counts, remaining_budget)

    prop_idx = []
    for st, n_h in alloc.items():
        if n_h <= 0:
            continue
        pool = remaining_df[remaining_df["_stratum"] == st]
        if len(pool) == 0:
            continue
        pick = min(n_h, len(pool))
        prop_idx.extend(
            pool.sample(n=pick, random_state=rng).index.tolist()
        )

    all_idx = coverage_idx + prop_idx
    result = df.loc[all_idx]
    return result


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------
def main():
    args = parse_args()
    records = load_records(Path(args.folder))

    if not records:
        raise SystemExit("No records found in the specified folder.")

    rows = [extract_features(r) for r in records]
    df = pd.DataFrame(rows)

    # Bin continuous fields before stratification
    df["num_events_bin"] = quantile_bin(df["num_events"], q=5)
    df["num_files_bin"] = quantile_bin(df["num_files"], q=5)

    strata_cols = ["cat", "date", "num_events_bin", "num_files_bin", "type_sec"]

    sampled = proportional_stratified_sample(
        df, strata_cols, n=args.sample_size, random_state=args.seed
    )

    recids = sampled["recid"].tolist()
    print(json.dumps(recids, indent=2))

    # Filter original records
    sampled_records = [r for r in records if r.get("recid") in recids]
    out_path = Path("sampled_metadata.json")
    out_path.write_text(json.dumps(sampled_records, indent=2), encoding="utf-8")
    print(f"Sampled {len(sampled_records)} records saved to {out_path}")


if __name__ == "__main__":
    main()