#!/usr/bin/env python3
"""Validate cleaned CT budget data for completeness and consistency."""

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

PROBLEM_SAMPLE_LIMIT = 5


def load_ndjson(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def analyze_dataset(path: Path) -> Dict[str, Any]:
    total = 0
    missing_description = 0
    missing_amount = 0
    duplicate_counter: Counter[Tuple[Any, ...]] = Counter()
    problem_rows: List[Dict[str, Any]] = []
    blank_line_item = 0
    long_line_items = 0
    long_examples: List[Tuple[int, str, Any, Any]] = []
    missing_agency = 0
    missing_section = 0
    notes_rows = 0
    value_label_counter: Counter[str] = Counter()

    for row in load_ndjson(path):
        total += 1
        key = (
            row.get("agency"),
            row.get("program"),
            row.get("line_item"),
            row.get("value_label"),
            row.get("fiscal_year"),
            round(float(row.get("amount", 0.0)), 2) if isinstance(row.get("amount"), (int, float)) else None,
        )
        duplicate_counter[key] += 1
        amount = row.get("amount")
        if "description" in row:
            description = (row.get("description") or "").strip()
            if not description:
                missing_description += 1
                if len(problem_rows) < PROBLEM_SAMPLE_LIMIT:
                    problem_rows.append({"issue": "missing description", "row": row})
        if not isinstance(amount, (int, float)):
            missing_amount += 1
            if len(problem_rows) < PROBLEM_SAMPLE_LIMIT:
                problem_rows.append({"issue": "missing amount", "row": row})
        line_item = (row.get("line_item") or "").strip()
        line_length = len(line_item)
        if not line_item:
            blank_line_item += 1
        if line_length > 100:
            long_line_items += 1
            long_examples.append((line_length, line_item[:140], row.get("agency"), row.get("page")))
        if not row.get("agency"):
            missing_agency += 1
        if not row.get("section"):
            missing_section += 1
        if any((row.get(field) or "").strip() for field in ("notes", "context_note") if field in row):
            notes_rows += 1
        value_label_counter[(row.get("value_label") or "").strip()] += 1

    duplicates = [(k, c) for k, c in duplicate_counter.items() if c > 1]
    duplicates.sort(key=lambda item: item[1], reverse=True)
    long_examples.sort(reverse=True)
    value_label_patterns = sorted(value_label_counter.items(), key=lambda item: len(item[0]), reverse=True)[:5]

    pct_missing_desc = (missing_description / total * 100) if total else 0.0
    pct_missing_amount = (missing_amount / total * 100) if total else 0.0

    return {
        "total": total,
        "missing_description": missing_description,
        "missing_amount": missing_amount,
        "pct_missing_desc": pct_missing_desc,
        "pct_missing_amount": pct_missing_amount,
        "duplicates": duplicates,
        "problem_rows": problem_rows[:PROBLEM_SAMPLE_LIMIT],
        "blank_line_item": blank_line_item,
        "long_line_items": long_line_items,
        "long_line_examples": long_examples[:5],
        "missing_agency": missing_agency,
        "missing_section": missing_section,
        "notes_rows": notes_rows,
        "value_label_patterns": value_label_patterns,
    }


def load_audit_rows(path: Optional[Path], limit: int = 5) -> List[Dict[str, str]]:
    if not path or not path.exists():
        return []
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row.get("action") == "merged":
                rows.append(row)
            if len(rows) >= limit:
                break
    return rows


def print_summary(label: str, metrics: Dict[str, Any]) -> None:
    print(label)
    print("-" * len(label))
    print(f"Rows analysed: {metrics['total']:,}")
    print(
        f"Missing description: {metrics['missing_description']:,} ({metrics['pct_missing_desc']:.2f}%)"
    )
    print(f"Missing amount: {metrics['missing_amount']:,} ({metrics['pct_missing_amount']:.2f}%)")
    print(f"Duplicate logical rows: {len(metrics['duplicates']):,}")
    print(f"Blank line_item rows: {metrics.get('blank_line_item', 0):,}")
    print(f"Line_item >100 chars: {metrics.get('long_line_items', 0):,}")
    print(
        f"Rows with notes/context_note: {metrics.get('notes_rows', 0):,} | Missing agency: {metrics.get('missing_agency', 0):,} | Missing section: {metrics.get('missing_section', 0):,}"
    )
    if metrics["duplicates"]:
        print("Top duplicate keys (up to 5):")
        for key, count in metrics["duplicates"][:5]:
            print(f"  {key} -> {count} rows")
    if metrics.get("long_line_examples"):
        print("Longest remaining line_item samples:")
        for length, text, agency, page in metrics["long_line_examples"]:
            preview = text.replace("\n", " ")[:120]
            print(f"  len={length} | agency={agency} page={page} | {preview}")
    if metrics.get("value_label_patterns"):
        print("Top remaining value_label patterns (by length):")
        for label, count in metrics["value_label_patterns"]:
            label_display = label or "(blank)"
            print(f"  '{label_display}' ({len(label_display)} chars) -> {count} rows")
    if metrics["problem_rows"]:
        print("Example problematic rows:")
        for sample in metrics["problem_rows"]:
            print(f"  Issue: {sample['issue']}")
            print(f"    Row: {json.dumps(sample['row'], ensure_ascii=False)}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate the cleaned or deduplicated CT budget dataset.")
    parser.add_argument(
        "--input",
        default="data/processed/clean_budget.ndjson",
        help="Path to the primary NDJSON file to validate",
    )
    parser.add_argument(
        "--baseline",
        help="Optional baseline NDJSON (e.g., pre-deduped) to compare row counts against",
    )
    parser.add_argument(
        "--audit",
        help="Optional audit CSV from the deduplication step for reporting sample merges",
    )
    args = parser.parse_args()

    input_path = Path(args.input).expanduser()
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    current_metrics = analyze_dataset(input_path)
    baseline_metrics = None
    if args.baseline:
        baseline_path = Path(args.baseline).expanduser()
        if not baseline_path.exists():
            raise FileNotFoundError(f"Baseline file not found: {baseline_path}")
        baseline_metrics = analyze_dataset(baseline_path)

    print_summary("Validation summary (target dataset)", current_metrics)

    if baseline_metrics:
        print_summary("Validation summary (baseline dataset)", baseline_metrics)
        removed = baseline_metrics["total"] - current_metrics["total"]
        pct_reduction = (removed / baseline_metrics["total"] * 100) if baseline_metrics["total"] else 0.0
        long_delta = baseline_metrics.get("long_line_items", 0) - current_metrics.get("long_line_items", 0)
        print("Comparison against baseline")
        print("---------------------------")
        print(f"Baseline rows: {baseline_metrics['total']:,}")
        print(f"Target rows: {current_metrics['total']:,}")
        print(f"Rows removed: {removed:,} ({pct_reduction:.2f}%)")
        print(
            f"Line_item >100 chars: {baseline_metrics.get('long_line_items', 0):,} -> {current_metrics.get('long_line_items', 0):,} (change {long_delta:+,})"
        )
        print()

    audit_rows = load_audit_rows(Path(args.audit).expanduser() if args.audit else None)
    if audit_rows:
        print("Sample merged groups from audit (first 5):")
        for row in audit_rows:
            print(
                f"  #{row['group_id']} {row['reason']} {row['merged_rows']} rows -> {row['agency']} | {row['line_item']} | {row['value_label']} | pages {row['pages']}"
            )
            if row.get("notes"):
                print(f"    Notes: {row['notes']}")
        print()

    remaining_duplicates = len(current_metrics["duplicates"])
    if remaining_duplicates:
        print(f"Remaining duplicate patterns to investigate: {remaining_duplicates}")
    else:
        print("No remaining duplicate patterns detected within the configured key.")


if __name__ == "__main__":
    main()
