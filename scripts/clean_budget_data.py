#!/usr/bin/env python3
"""Clean CT budget NDJSON and export analysis-ready datasets."""

import argparse
import csv
import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

SOURCE_DOCUMENT = "FY26-FY27 Connecticut Budget"
DOT_LEADER_RE = re.compile(r"\.{2,}")
WS_RE = re.compile(r"\s+")
BROKEN_WORD_RE = re.compile(r"([A-Za-z]{3,})\s+([A-Za-z])\b")
YEAR_FOUR_RE = re.compile(r"(20\d{2})")
FY_TWO_RE = re.compile(r"FY\s*(\d{2})", re.IGNORECASE)
ALPHA_RE = re.compile(r"[A-Za-z]")

LINE_ITEM_KEYWORDS = ["account", "line item", "line-item", "lineitem", "description", "item", "purpose"]
PROGRAM_KEYWORDS = ["program", "division", "unit", "office", "initiative", "project", "department"]

DATASET_FIELDS = [
    "source_document",
    "agency",
    "section",
    "program",
    "line_item",
    "value_label",
    "fiscal_year",
    "amount",
    "page",
    "description",
]


def clean_label(text: Any) -> str:
    if text is None:
        return ""
    s = str(text)
    s = DOT_LEADER_RE.sub(" ", s)
    s = s.replace("•", " ").replace("—", "-").replace("–", "-")
    s = WS_RE.sub(" ", s)
    s = s.strip(" :-")
    if not s:
        return ""
    if s.lower().startswith("unnamed"):
        return ""
    return s


def clean_text_value(value: Any) -> str:
    if value is None:
        return ""
    s = str(value)
    s = DOT_LEADER_RE.sub(" ", s)
    s = s.replace("•", " ").replace("—", "-").replace("–", "-")
    s = WS_RE.sub(" ", s)
    s = s.strip()
    if not s:
        return ""
    s = BROKEN_WORD_RE.sub(_merge_fragment, s)
    return s


def _merge_fragment(match: re.Match) -> str:
    first, second = match.group(1), match.group(2)
    if first.endswith("."):
        return f"{first} {second}"
    return first + second


def contains_alpha(text: str) -> bool:
    return bool(ALPHA_RE.search(text))


def is_meaningful_label(label: str) -> bool:
    if not label:
        return False
    if len(label.split()) > 12:
        return False
    if not contains_alpha(label) and not label.upper().startswith("FY"):
        return False
    return True


def parse_amount(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s or s in {"-", "--", "…"}:
        return None
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    s = s.replace(",", "")
    s = s.replace("$", "")
    if not s:
        return None
    try:
        num = float(s)
    except ValueError:
        return None
    return -num if neg else num


def extract_year(label: str) -> Optional[int]:
    match = YEAR_FOUR_RE.search(label)
    if match:
        return int(match.group(1))
    match = FY_TWO_RE.search(label)
    if not match:
        return None
    two = int(match.group(1))
    century = 2000 if two < 50 else 1900
    return century + two


def pick_field(row: Dict[str, Any], keywords: Iterable[str]) -> Optional[str]:
    for col, value in row.items():
        label = clean_label(col).lower()
        if any(key in label for key in keywords):
            text = clean_text_value(value)
            if text:
                return text
    return None


class BudgetCleaner:
    def __init__(self) -> None:
        self.unnamed_labels: Dict[str, str] = {}
        self.header_rows = 0
        self.data_rows = 0
        self.skipped_rows = 0

    def run(
        self,
        input_path: Path,
        ndjson_out: Path,
        csv_out: Path,
        sqlite_out: Path,
    ) -> None:
        records: List[Dict[str, Any]] = []
        with input_path.open("r", encoding="utf-8") as src:
            for line in src:
                if not line.strip():
                    continue
                payload = json.loads(line)
                normalized = self._process_payload(payload)
                if normalized is None:
                    continue
                records.extend(normalized)
        if not records:
            raise RuntimeError("No records were produced. Check the input file.")
        self._write_ndjson(records, ndjson_out)
        self._write_csv(records, csv_out)
        self._write_sqlite(records, sqlite_out)
        print(
            f"Cleaned {len(records):,} rows from {self.data_rows:,} data lines. "
            f"Skipped {self.header_rows:,} header-only rows and {self.skipped_rows:,} unusable rows."
        )

    def _process_payload(self, payload: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        row = payload.get("row")
        if not isinstance(row, dict) or not row:
            self.skipped_rows += 1
            return None
        numeric_cells = []
        meaningful_hits = 0
        for col, value in row.items():
            amount = parse_amount(value)
            if amount is None:
                continue
            numeric_cells.append((col, amount))
            label = self._resolve_label(col)
            if is_meaningful_label(label):
                meaningful_hits += 1
        if not numeric_cells or meaningful_hits == 0:
            self._learn_unnamed_labels(row)
            self.header_rows += 1
            return None
        self.data_rows += 1

        agency = clean_text_value(payload.get("agency", "Unknown Agency")) or "Unknown Agency"
        section = clean_text_value(payload.get("section", "General")) or "General"
        page = payload.get("page") or None
        program = pick_field(row, PROGRAM_KEYWORDS) or section
        line_item = pick_field(row, LINE_ITEM_KEYWORDS)

        context_tokens: List[str] = []
        if line_item:
            context_tokens.append(line_item)
        for col, value in row.items():
            if parse_amount(value) is not None:
                continue
            text = clean_text_value(value)
            if not text:
                continue
            label = clean_label(col)
            if label and label.lower() not in {"account", "program"}:
                context_tokens.append(label)
            if text.lower() != label.lower():
                context_tokens.append(text)
        for extra_key in ("background", "governor", "legislative"):
            text = clean_text_value(payload.get(extra_key))
            if text:
                context_tokens.append(text)
        description = WS_RE.sub(" ", " ".join(t for t in context_tokens if t)).strip()
        if not description and line_item:
            description = line_item
        if not line_item:
            line_item = description or "Unspecified"

        results: List[Dict[str, Any]] = []
        for col, amount in numeric_cells:
            label = self._resolve_label(col)
            if not is_meaningful_label(label):
                continue
            record = {
                "source_document": SOURCE_DOCUMENT,
                "agency": agency,
                "section": section,
                "program": program,
                "line_item": line_item,
                "value_label": label,
                "fiscal_year": extract_year(label),
                "amount": round(float(amount), 2),
                "page": page,
                "description": description,
            }
            results.append(record)
        if not results:
            self.skipped_rows += 1
            self._learn_unnamed_labels(row)
            return None
        return results

    def _resolve_label(self, column_name: str) -> str:
        label = clean_label(column_name)
        if label:
            return label
        if column_name.startswith("Unnamed"):
            return self.unnamed_labels.get(column_name, "")
        return ""

    def _learn_unnamed_labels(self, row: Dict[str, Any]) -> None:
        for col, value in row.items():
            if not isinstance(col, str) or not col.startswith("Unnamed"):
                continue
            text = clean_text_value(value)
            if text and contains_alpha(text):
                self.unnamed_labels[col] = text

    def _write_ndjson(self, records: List[Dict[str, Any]], path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            for row in records:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _write_csv(self, records: List[Dict[str, Any]], path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=DATASET_FIELDS)
            writer.writeheader()
            writer.writerows(records)

    def _write_sqlite(self, records: List[Dict[str, Any]], path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()
        conn = sqlite3.connect(path)
        cols_sql = ", ".join(
            [
                "source_document TEXT",
                "agency TEXT",
                "section TEXT",
                "program TEXT",
                "line_item TEXT",
                "value_label TEXT",
                "fiscal_year INTEGER",
                "amount REAL",
                "page INTEGER",
                "description TEXT",
            ]
        )
        conn.execute(f"CREATE TABLE budget ({cols_sql})")
        conn.executemany(
            "INSERT INTO budget VALUES (:source_document,:agency,:section,:program,:line_item,:value_label,:fiscal_year,:amount,:page,:description)",
            records,
        )
        conn.commit()
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Clean CT budget NDJSON into structured datasets.")
    parser.add_argument("--input", default="ct_budget_optimized.ndjson", help="Raw NDJSON input path")
    parser.add_argument(
        "--ndjson-out",
        default="data/processed/clean_budget.ndjson",
        help="Path for cleaned NDJSON output",
    )
    parser.add_argument(
        "--csv-out",
        default="data/processed/clean_budget.csv",
        help="Path for cleaned CSV output",
    )
    parser.add_argument(
        "--sqlite-out",
        default="data/processed/clean_budget.sqlite",
        help="Path for SQLite output",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    cleaner = BudgetCleaner()
    cleaner.run(
        input_path=Path(args.input).expanduser(),
        ndjson_out=Path(args.ndjson_out).expanduser(),
        csv_out=Path(args.csv_out).expanduser(),
        sqlite_out=Path(args.sqlite_out).expanduser(),
    )


if __name__ == "__main__":
    main()
