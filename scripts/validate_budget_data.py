#!/usr/bin/env python3
"""Validate cleaned CT budget data for completeness and consistency."""

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

PROBLEM_SAMPLE_LIMIT = 5


def load_ndjson(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate the cleaned CT budget dataset.")
    parser.add_argument(
        "--input",
        default="data/processed/clean_budget.ndjson",
        help="Path to the cleaned NDJSON file",
    )
    args = parser.parse_args()
    path = Path(args.input).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    total = 0
    missing_description = 0
    missing_amount = 0
    duplicate_counter: Counter[Tuple[Any, ...]] = Counter()
    problem_rows = []

    for row in load_ndjson(path):
        total += 1
        key = (
            row.get("agency"),
            row.get("program"),
            row.get("line_item"),
            row.get("value_label"),
            row.get("fiscal_year"),
        )
        duplicate_counter[key] += 1
        description = (row.get("description") or "").strip()
        amount = row.get("amount")
        if not description:
            missing_description += 1
            if len(problem_rows) < PROBLEM_SAMPLE_LIMIT:
                problem_rows.append({"issue": "missing description", "row": row})
        if not isinstance(amount, (int, float)):
            missing_amount += 1
            if len(problem_rows) < PROBLEM_SAMPLE_LIMIT:
                problem_rows.append({"issue": "missing amount", "row": row})

    duplicates = {k: c for k, c in duplicate_counter.items() if c > 1}

    if total == 0:
        print("No rows found in the dataset.")
        return

    pct_missing_desc = (missing_description / total) * 100
    pct_missing_amount = (missing_amount / total) * 100

    print("Validation summary")
    print("-------------------")
    print(f"Rows analysed: {total:,}")
    print(f"Missing description: {missing_description:,} ({pct_missing_desc:.2f}% )")
    print(f"Missing amount: {missing_amount:,} ({pct_missing_amount:.2f}% )")
    print(f"Duplicate logical rows: {len(duplicates):,}")

    if duplicates:
        top_dups = list(duplicates.items())[:5]
        print("\nSample duplicates (first 5):")
        for key, count in top_dups:
            print(f"  {key} -> {count} rows")

    if problem_rows:
        print("\nExample problematic rows:")
        for sample in problem_rows[:PROBLEM_SAMPLE_LIMIT]:
            print(f"  Issue: {sample['issue']}")
            print(f"    Row: {json.dumps(sample['row'], ensure_ascii=False)}")
    else:
        print("\nNo problematic rows detected based on the configured checks.")


if __name__ == "__main__":
    main()
