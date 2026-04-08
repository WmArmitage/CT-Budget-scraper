#!/usr/bin/env python3
"""Deduplicate cleaned CT budget data for buyer-friendly exports."""

import argparse
import csv
import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

EXACT_FIELDS = (
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
)

SUMMARY_KEYWORDS = {
    "general fund": {"general fund", "general funds"},
    "other appropriated": {"other appropriated", "other appr", "other appropriations"},
    "all appropriated": {"all appropriated", "all appropriated funds", "all appropriations"},
}

SUMMARY_CANONICAL_ORDER = (
    "general fund",
    "other appropriated",
    "all appropriated",
)

OUTPUT_FIELDS = [
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
    "merged_row_count",
]


def load_ndjson(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def save_ndjson(rows: Sequence[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def save_csv(rows: Sequence[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def save_sqlite(rows: Sequence[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    columns = ", ".join(
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
            "merged_row_count INTEGER",
        ]
    )
    conn.execute(f"CREATE TABLE budget_deduped ({columns})")
    conn.executemany(
        "INSERT INTO budget_deduped VALUES (:source_document,:agency,:section,:program,:line_item,:value_label,:fiscal_year,:amount,:page,:description,:merged_row_count)",
        rows,
    )
    conn.commit()
    conn.close()


def save_audit(entries: Sequence[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "group_id",
        "action",
        "reason",
        "merged_rows",
        "agency",
        "line_item",
        "value_label",
        "fiscal_year",
        "amount",
        "pages",
        "section_variants",
        "program_variants",
        "notes",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(entries)


def normalize(text: Optional[str]) -> str:
    if text is None:
        return ""
    lowered = text.lower().strip()
    cleaned = " ".join(part for part in lowered.replace("/", " ").split())
    return cleaned.strip(" -:;,")


def canonical_summary_label(label_norm: str) -> Optional[str]:
    if not label_norm:
        return None
    for canonical in SUMMARY_CANONICAL_ORDER:
        for keyword in SUMMARY_KEYWORDS[canonical]:
            if keyword in label_norm:
                return canonical
    return None


class BudgetDeduper:
    def __init__(self) -> None:
        self.audit_entries: List[Dict[str, Any]] = []
        self.group_counter = 1

    def dedupe(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        exact_unique = self._dedupe_exact(rows)
        summary_unique = self._dedupe_summary(exact_unique)
        cleaned: List[Dict[str, Any]] = []
        for row in summary_unique:
            row.pop("_consumed", None)
            pages = row.pop("_source_pages", set())
            if not pages:
                row_page = row.get("page")
                if row_page is not None:
                    pages = {row_page}
            if pages:
                row["page"] = min(pages)
            row.pop("_section_variants", None)
            row.pop("_program_variants", None)
            cleaned.append(row)
        return cleaned

    def _dedupe_exact(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        exact_map: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        for original in rows:
            key = tuple(original.get(field) for field in EXACT_FIELDS)
            if key in exact_map:
                existing = exact_map[key]
                existing["merged_row_count"] += 1
                pages = existing.setdefault("_source_pages", set())
                if original.get("page") is not None:
                    pages.add(original["page"])
                if original.get("section"):
                    existing.setdefault("_section_variants", set()).add(original["section"])
                if original.get("program"):
                    existing.setdefault("_program_variants", set()).add(original["program"])
            else:
                record = dict(original)
                record["merged_row_count"] = 1
                record["_source_pages"] = {record["page"]} if record.get("page") is not None else set()
                record["_section_variants"] = {record.get("section", "")} if record.get("section") else set()
                record["_program_variants"] = {record.get("program", "")} if record.get("program") else set()
                exact_map[key] = record
        for record in exact_map.values():
            if record["merged_row_count"] > 1:
                self._record_audit(record, "merged", "exact_duplicate", record["merged_row_count"], "Identical rows collapsed")
        return list(exact_map.values())

    def _dedupe_summary(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        groups: Dict[Tuple[str, str, str, str, Any, float], List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            key = self._summary_key(row)
            if key:
                groups[key].append(row)
        output: List[Dict[str, Any]] = []
        consumed: Set[int] = set()
        for group_rows in groups.values():
            candidates = [row for row in group_rows if id(row) not in consumed]
            if len(candidates) <= 1:
                continue
            if self._safe_to_merge(candidates):
                merged = self._merge_group(candidates)
                for row in candidates:
                    consumed.add(id(row))
                output.append(merged)
                self._record_audit(
                    merged,
                    "merged",
                    "summary_panel_overlap",
                    merged["merged_row_count"],
                    f"Collapsed {len(candidates)} overlapping summary rows",
                )
            else:
                self._record_uncertain(candidates)
        for row in rows:
            if id(row) not in consumed:
                output.append(row)
        return output

    def _summary_key(self, row: Dict[str, Any]) -> Optional[Tuple[str, str, str, str, Any, float]]:
        label_norm = normalize(row.get("value_label"))
        canonical = canonical_summary_label(label_norm)
        if not canonical:
            return None
        line_item_norm = normalize(row.get("line_item"))
        if not line_item_norm:
            return None
        agency_norm = normalize(row.get("agency"))
        program_norm = normalize(row.get("program"))
        amount = row.get("amount")
        if amount is None:
            return None
        amount_key = round(float(amount), 2)
        fiscal = row.get("fiscal_year") or ""
        return (agency_norm, program_norm, line_item_norm, canonical, fiscal, amount_key)

    def _safe_to_merge(self, rows: Sequence[Dict[str, Any]]) -> bool:
        line_items = {normalize(row.get("line_item")) for row in rows}
        if len(line_items) > 1:
            return False
        descriptions = {normalize(row.get("description")) for row in rows if row.get("description")}
        if len(descriptions) > 3:
            return False
        sections = {normalize(row.get("section")) for row in rows if row.get("section")}
        if len(sections) > 3:
            return False
        programs = {normalize(row.get("program")) for row in rows if row.get("program")}
        if len(programs) > 3:
            return False
        page_values: Set[int] = set()
        for row in rows:
            page_values.update(p for p in row.get("_source_pages", set()) if isinstance(p, int))
            if not row.get("_source_pages") and isinstance(row.get("page"), int):
                page_values.add(row["page"])
        if page_values and (max(page_values) - min(page_values)) > 2:
            return False
        return True

    def _merge_group(self, rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        base = dict(rows[0])
        total = sum(row.get("merged_row_count", 1) for row in rows)
        base["merged_row_count"] = total
        combined_pages: Set[int] = set()
        descriptions: List[str] = []
        for row in rows:
            combined_pages.update(row.get("_source_pages", set()))
            desc = row.get("description")
            if desc and desc not in descriptions:
                descriptions.append(desc)
            section_variants = row.get("_section_variants", set())
            program_variants = row.get("_program_variants", set())
            base.setdefault("_section_variants", set()).update(section_variants or {row.get("section", "")})
            base.setdefault("_program_variants", set()).update(program_variants or {row.get("program", "")})
        base["description"] = " | ".join(descriptions) if descriptions else base.get("description", "")
        base["_source_pages"] = combined_pages
        base["_consumed"] = False
        return base

    def _record_audit(
        self,
        row: Dict[str, Any],
        action: str,
        reason: str,
        merged_rows: int,
        notes: str,
    ) -> None:
        pages = sorted({p for p in row.get("_source_pages", set()) if p is not None})
        entry = {
            "group_id": self.group_counter,
            "action": action,
            "reason": reason,
            "merged_rows": merged_rows,
            "agency": row.get("agency", ""),
            "line_item": row.get("line_item", ""),
            "value_label": row.get("value_label", ""),
            "fiscal_year": row.get("fiscal_year"),
            "amount": row.get("amount"),
            "pages": "|".join(map(str, pages)) if pages else "",
            "section_variants": self._collect_variant_string(row.get("_section_variants"), row.get("section")),
            "program_variants": self._collect_variant_string(row.get("_program_variants"), row.get("program")),
            "notes": notes,
        }
        self.audit_entries.append(entry)
        self.group_counter += 1

    def _record_uncertain(self, rows: Sequence[Dict[str, Any]]) -> None:
        pages = sorted({p for row in rows for p in row.get("_source_pages", set()) if p is not None})
        entry = {
            "group_id": self.group_counter,
            "action": "retained",
            "reason": "summary_panel_uncertain",
            "merged_rows": len(rows),
            "agency": rows[0].get("agency", ""),
            "line_item": rows[0].get("line_item", ""),
            "value_label": rows[0].get("value_label", ""),
            "fiscal_year": rows[0].get("fiscal_year"),
            "amount": rows[0].get("amount"),
            "pages": "|".join(map(str, pages)) if pages else "",
            "section_variants": "|".join(sorted({row.get("section", "") for row in rows if row.get("section")})),
            "program_variants": "|".join(sorted({row.get("program", "") for row in rows if row.get("program")})),
            "notes": "Multiple rows detected but left untouched for safety",
        }
        self.audit_entries.append(entry)
        self.group_counter += 1

    @staticmethod
    def _collect_variant_string(store: Optional[Set[str]], fallback: Optional[str]) -> str:
        values = set()
        if store:
            values.update(v for v in store if v)
        if fallback:
            values.add(fallback)
        return "|".join(sorted(values))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create deduplicated CT budget exports.")
    parser.add_argument("--input", default="data/processed/clean_budget.ndjson", help="Clean NDJSON input path")
    parser.add_argument("--csv-out", default="data/processed/clean_budget_deduped.csv", help="Output CSV path")
    parser.add_argument("--ndjson-out", default="data/processed/clean_budget_deduped.ndjson", help="Output NDJSON path")
    parser.add_argument("--sqlite-out", default="data/processed/clean_budget_deduped.sqlite", help="Output SQLite path")
    parser.add_argument("--audit-out", default="data/processed/clean_budget_deduplication_audit.csv", help="Audit CSV path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input).expanduser()
    rows = load_ndjson(input_path)
    deduper = BudgetDeduper()
    deduped_rows = deduper.dedupe(rows)
    save_ndjson(deduped_rows, Path(args.ndjson_out).expanduser())
    save_csv(deduped_rows, Path(args.csv_out).expanduser())
    save_sqlite(deduped_rows, Path(args.sqlite_out).expanduser())
    save_audit(deduper.audit_entries, Path(args.audit_out).expanduser())
    removed = len(rows) - len(deduped_rows)
    print(f"Deduped dataset written. Removed {removed} rows ({len(rows)} -> {len(deduped_rows)}).")


if __name__ == "__main__":
    main()
