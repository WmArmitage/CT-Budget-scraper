#!/usr/bin/env python3
"""Finalize CT budget dataset into polished buyer-facing exports."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

FULL_FIELDS = [
    "source_document",
    "agency",
    "section",
    "program",
    "line_item",
    "value_label",
    "original_value_label",
    "fiscal_year",
    "amount",
    "page",
    "description",
    "notes",
    "context_note",
    "merged_row_count",
]

SLIM_FIELDS = [
    "agency",
    "section",
    "program",
    "line_item",
    "value_label",
    "fiscal_year",
    "amount",
    "page",
]

NARRATIVE_KEYWORDS = {
    "reflects",
    "includes",
    "represents",
    "adjust",
    "adjustment",
    "because",
    "due to",
    "associated with",
    "provides",
    "increase",
    "decrease",
    "allocation",
    "transfer",
    "funding",
    "authorizes",
    "requirement",
    "establishes",
    "budget",
    "million",
    "revenue",
    "reserve",
    "threshold",
    "projected",
    "volatility",
    "biennial",
    "act",
}

VALUE_LABEL_MAP = {
    "general fund": "General Fund",
    "other appropriated": "Other Appropriated",
    "all appropriated": "All Appropriated",
    "balance": "Balance",
    "amount": "Amount",
    "legislative": "Legislative",
    "governor recommended": "Governor Recommended",
    "consensus update": "Consensus Update",
    "april consensus": "Consensus Update (April)",
    "difference": "Difference",
}

FY_PATTERN = re.compile(r"fy\s*(\d{2,4})", re.IGNORECASE)
RANGE_PATTERN = re.compile(r"fy\s*(\d{2,4})\s*[-–]\s*fy\s*(\d{2,4})", re.IGNORECASE)
ACTUAL_PATTERN = re.compile(r"actual\s+fy\s*(\d{2,4})", re.IGNORECASE)
APPRO_PATTERN = re.compile(r"appropriation\s+fy\s*(\d{2,4})", re.IGNORECASE)
CURRENT_PATTERN = re.compile(r"fy\s*(\d{2,4})\s+current services", re.IGNORECASE)


class ProductFinalizer:
    def __init__(self) -> None:
        self.lengths_before: List[int] = []
        self.lengths_after: List[int] = []
        self.long_before = 0
        self.long_after = 0
        self.report_rows: List[Dict[str, Any]] = []

    def finalize(self, rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        full: List[Dict[str, Any]] = []
        slim: List[Dict[str, Any]] = []
        for row in rows:
            processed_full = self._process_row(row)
            full.append(processed_full)
            slim.append(self._build_slim(processed_full))
        return full, slim

    def _process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        line_item_raw = self._clean_text(row.get("line_item"))
        description_raw = self._clean_text(row.get("description"))
        self.lengths_before.append(len(line_item_raw))
        if len(line_item_raw) > 100:
            self.long_before += 1

        context_chunks: List[str] = []
        notes_entries: List[str] = []
        actions: List[Tuple[str, str]] = []

        line_is_narr = self._is_narrative(line_item_raw)
        desc_is_narr = self._is_narrative(description_raw)

        new_line_item = line_item_raw
        new_description = description_raw

        if line_is_narr:
            context_chunks.append(line_item_raw)
            candidate = self._choose_label(row, description_raw)
            if candidate and candidate != line_item_raw:
                actions.append(("moved_line_item_prose_to_notes", "Line item narrative moved to context"))
                notes_entries.append("Line item narrative moved to context_note")
                new_line_item = candidate
            else:
                actions.append(("preserved_uncertain_narrative", "Could not find safe replacement label"))
                notes_entries.append("Line item retained; narrative logged in context_note")

        if desc_is_narr:
            if description_raw:
                context_chunks.append(description_raw)
                actions.append(("moved_description_to_notes", "Description narrative moved to context"))
                notes_entries.append("Description moved to context_note")
            new_description = ""
        elif description_raw and description_raw == new_line_item:
            new_description = ""

        context_note = " | ".join(dict.fromkeys(context_chunks))
        if context_note:
            actions.append(("context_note_created", "Narrative preserved in context_note"))

        normalized_value_label, normalization_reason = self._normalize_value_label(row.get("value_label"))
        original_value_label = row.get("value_label") or ""
        if normalization_reason:
            actions.append(("normalized_value_label", normalization_reason))
            notes_entries.append(normalization_reason)
        if not normalized_value_label:
            normalized_value_label = original_value_label

        if not new_description:
            new_description = self._derive_short_description(row, context_note, new_line_item)

        record = {
            "source_document": row.get("source_document"),
            "agency": row.get("agency"),
            "section": row.get("section"),
            "program": row.get("program"),
            "line_item": new_line_item or self._fallback_label(row),
            "value_label": normalized_value_label,
            "original_value_label": original_value_label,
            "fiscal_year": row.get("fiscal_year"),
            "amount": row.get("amount"),
            "page": row.get("page"),
            "description": new_description,
            "notes": "; ".join(dict.fromkeys(notes_entries)),
            "context_note": context_note,
            "merged_row_count": row.get("merged_row_count", 1),
        }

        self.lengths_after.append(len(record["line_item"]))
        if len(record["line_item"]) > 100:
            self.long_after += 1

        for action, reason in actions:
            self._log_action(
                action_type=action,
                reason=reason,
                row=row,
                new_line_item=record["line_item"],
                new_description=record["description"],
                normalized_value_label=record["value_label"],
                notes=record["notes"],
            )

        return record

    def _log_action(
        self,
        action_type: str,
        reason: str,
        row: Dict[str, Any],
        new_line_item: str,
        new_description: str,
        normalized_value_label: str,
        notes: str,
    ) -> None:
        self.report_rows.append(
            {
                "action_type": action_type,
                "reason": reason,
                "agency": row.get("agency", ""),
                "page": row.get("page", ""),
                "original_line_item": self._clean_text(row.get("line_item")),
                "new_line_item": new_line_item,
                "original_description": self._clean_text(row.get("description")),
                "new_description": new_description,
                "original_value_label": row.get("value_label", ""),
                "normalized_value_label": normalized_value_label,
                "notes_added": notes,
            }
        )

    def _clean_text(self, value: Any) -> str:
        if value is None:
            return ""
        text = str(value).replace("\n", " ").replace("\r", " ")
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _is_narrative(self, text: str) -> bool:
        if not text:
            return False
        length = len(text)
        lower = text.lower()
        sentence_like = text.count(".") + text.count(";") >= 1
        keyword_hit = any(keyword in lower for keyword in NARRATIVE_KEYWORDS)
        comma_count = text.count(",")
        space_count = text.count(" ")
        if length > 100:
            return True
        if length > 60 and (sentence_like or keyword_hit or comma_count >= 2 or space_count > 12):
            return True
        return False

    def _choose_label(self, row: Dict[str, Any], description: str) -> Optional[str]:
        candidates = [description, row.get("program"), row.get("section"), row.get("value_label"), row.get("agency")]
        for cand in candidates:
            cleaned = self._clean_text(cand)
            if cleaned and len(cleaned) <= 80 and not self._is_narrative(cleaned):
                return cleaned
        return None

    def _fallback_label(self, row: Dict[str, Any]) -> str:
        for cand in (row.get("program"), row.get("section"), "Unlabeled Item"):
            cleaned = self._clean_text(cand)
            if cleaned:
                return cleaned
        return "Unlabeled Item"

    def _normalize_value_label(self, label: Any) -> Tuple[str, Optional[str]]:
        original = self._clean_text(label)
        if not original:
            return "", None
        lower = original.lower()
        if lower in VALUE_LABEL_MAP:
            return VALUE_LABEL_MAP[lower], f"Value label normalized to {VALUE_LABEL_MAP[lower]}"
        range_match = RANGE_PATTERN.search(lower)
        if range_match:
            y1 = self._to_year(range_match.group(1))
            y2 = self._to_year(range_match.group(2))
            if y1 and y2:
                return f"FY {y1} - FY {y2}", "Converted FY range"
        actual = ACTUAL_PATTERN.search(lower)
        if actual:
            year = self._to_year(actual.group(1))
            if year:
                return f"Actual FY {year}", "Standardized Actual FY label"
        appropriation = APPRO_PATTERN.search(lower)
        if appropriation:
            year = self._to_year(appropriation.group(1))
            if year:
                return f"Appropriation FY {year}", "Standardized Appropriation label"
        current_services = CURRENT_PATTERN.search(lower)
        if current_services:
            year = self._to_year(current_services.group(1))
            if year:
                return f"FY {year} Current Services", "Standardized current services label"
        fy_match = FY_PATTERN.search(lower)
        if fy_match:
            year = self._to_year(fy_match.group(1))
            if year:
                cleaned = original
                cleaned = re.sub(FY_PATTERN, f"FY {year}", cleaned, count=1)
                cleaned = cleaned.replace("$", "").strip()
                return cleaned, "Normalized FY label"
        if lower.endswith("$"):
            trimmed = original.rstrip(" $")
            return trimmed, "Removed trailing currency symbol"
        return original, None

    @staticmethod
    def _to_year(token: str) -> Optional[int]:
        if not token:
            return None
        year = int(token)
        if year < 100:
            return 2000 + year if year <= 40 else 1900 + year
        return year

    def _build_slim(self, full_row: Dict[str, Any]) -> Dict[str, Any]:
        slim = {key: full_row.get(key) for key in SLIM_FIELDS}
        return slim

    def _derive_short_description(self, row: Dict[str, Any], context: str, line_item: str) -> str:
        candidates = [row.get("program"), row.get("section"), row.get("value_label")]
        for cand in candidates:
            cleaned = self._clean_text(cand)
            if cleaned and cleaned != line_item and not self._is_narrative(cleaned) and len(cleaned) <= 120:
                return cleaned
        if context:
            snippet = context[:160]
            return snippet + ("…" if len(context) > 160 else "")
        return ""


def load_ndjson(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def save_ndjson(rows: Sequence[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def save_csv(rows: Sequence[Dict[str, Any]], path: Path, fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def save_sqlite(rows: Sequence[Dict[str, Any]], path: Path, table: str, fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    column_defs = []
    for field in fieldnames:
        if field == "amount":
            column_defs.append(f"{field} REAL")
        elif field.endswith("_count") or field == "page" or field == "fiscal_year":
            column_defs.append(f"{field} INTEGER")
        else:
            column_defs.append(f"{field} TEXT")
    conn.execute(f"CREATE TABLE {table} ({', '.join(column_defs)})")
    placeholders = ",".join(f":{field}" for field in fieldnames)
    conn.executemany(
        f"INSERT INTO {table} VALUES ({placeholders})",
        rows,
    )
    conn.commit()
    conn.close()


def save_report(rows: Sequence[Dict[str, Any]], path: Path) -> None:
    if not rows:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("action_type,reason,agency,page,original_line_item,new_line_item,original_description,new_description,original_value_label,normalized_value_label,notes_added\n", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finalize CT budget deduped dataset into polished exports.")
    parser.add_argument("--input", default="data/processed/clean_budget_deduped.ndjson", help="Deduped NDJSON input path")
    parser.add_argument("--full-csv-out", default="data/processed/clean_budget_deduped_full.csv")
    parser.add_argument("--full-ndjson-out", default="data/processed/clean_budget_deduped_full.ndjson")
    parser.add_argument("--full-sqlite-out", default="data/processed/clean_budget_deduped_full.sqlite")
    parser.add_argument("--slim-csv-out", default="data/processed/clean_budget_deduped_slim.csv")
    parser.add_argument("--slim-ndjson-out", default="data/processed/clean_budget_deduped_slim.ndjson")
    parser.add_argument("--slim-sqlite-out", default="data/processed/clean_budget_deduped_slim.sqlite")
    parser.add_argument("--report-out", default="data/processed/clean_budget_productization_report.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input).expanduser()
    rows = load_ndjson(input_path)
    finalizer = ProductFinalizer()
    full_rows, slim_rows = finalizer.finalize(rows)

    save_ndjson(full_rows, Path(args.full_ndjson_out).expanduser())
    save_csv(full_rows, Path(args.full_csv_out).expanduser(), FULL_FIELDS)
    save_sqlite(full_rows, Path(args.full_sqlite_out).expanduser(), "budget_final_full", FULL_FIELDS)

    save_ndjson(slim_rows, Path(args.slim_ndjson_out).expanduser())
    save_csv(slim_rows, Path(args.slim_csv_out).expanduser(), SLIM_FIELDS)
    save_sqlite(slim_rows, Path(args.slim_sqlite_out).expanduser(), "budget_final_slim", SLIM_FIELDS)

    save_report(finalizer.report_rows, Path(args.report_out).expanduser())

    print(f"Finalized full rows: {len(full_rows):,}")
    print(f"Finalized slim rows: {len(slim_rows):,}")
    print(f"Line items >100 chars before: {finalizer.long_before:,}")
    print(f"Line items >100 chars after : {finalizer.long_after:,}")


if __name__ == "__main__":
    main()
