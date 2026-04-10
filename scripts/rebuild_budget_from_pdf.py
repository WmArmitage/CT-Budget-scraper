"""
Rebuild the FY26/FY27 Connecticut budget tables from the OFA PDF.

This script performs three core phases:
1. Page classification (agency detail vs. subcommittee summary vs. policy change vs narrative).
2. Table extraction for each classified page family with schema-aligned outputs.
3. Persistence into SQLite plus CSV artifacts, matching the expectations of
   `scripts/qa_budget_extraction.py`.

Usage (PowerShell friendly):
    python scripts/rebuild_budget_from_pdf.py --pdf-path data/CTBudget.pdf --sqlite-path ct_budget_FY26_FY27.sqlite
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import pandas as pd
import pdfplumber

SCENARIO_MAP = [
    ("actual_fy23", "actual", 2023),
    ("actual_fy24", "actual", 2024),
    ("appropriation_fy25", "appropriation", 2025),
    ("governor_fy26", "governor", 2026),
    ("governor_fy27", "governor", 2027),
    ("legislative_fy26", "legislative", 2026),
    ("legislative_fy27", "legislative", 2027),
]

POLICY_VALUE_COLUMNS = [
    "governor_fy26",
    "governor_fy27",
    "legislative_fy26",
    "legislative_fy27",
    "difference_fy26",
    "difference_fy27",
]

SUBCOMMITTEE_KEYWORDS = [
    "general government",
    "human services",
    "conservation and development",
    "regulation and protection",
    "elementary and secondary education",
    "higher education",
    "judicial and corrections",
    "transportation",
]


@dataclass
class PageClassification:
    page_number: int
    page_type: str
    subcommittee: Optional[str]
    agency: Optional[str]
    confidence: float


@dataclass
class PageGroup:
    start_page: int
    end_page: int
    page_type: str
    subcommittee: Optional[str]
    agency: Optional[str]

    def iter_pages(self) -> Iterator[int]:
        for page in range(self.start_page, self.end_page + 1):
            yield page


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()

def clean_agency_name(text: str | None) -> Optional[str]:
    if not text:
        return None

    text = text.strip()

    # Remove leading codes like OLM10000, APA11000, SOS12500
    text = re.sub(r'^[A-Z]{2,4}\d{4,6}\s*[-:]?\s*', '', text)

    # Remove "General Government A" suffix noise
    text = re.sub(r'\bGeneral Government A\b', '', text, flags=re.IGNORECASE)

    # Remove duplicate words
    words = text.split()
    deduped = []
    for w in words:
        if not deduped or deduped[-1] != w:
            deduped.append(w)

    text = " ".join(deduped)

    return text.strip()


def clean_money(token: str) -> Optional[float]:
    token = token.strip()
    if not token or token in {"-", "--"}:
        return None
    negative = token.startswith("(") and token.endswith(")")
    cleaned = token.replace("$", "").replace(",", "").replace("(", "").replace(")", "")
    try:
        number = float(cleaned)
    except ValueError:
        return None
    return -number if negative else number


def token_is_numeric(token: str) -> bool:
    token = token.strip()
    if not token:
        return False
    if token in {"-", "--"}:
        return True
    return bool(re.fullmatch(r"[-$,(]*[\d,.]+[)]?", token))


def detect_subcommittee(text: str) -> Optional[str]:
    lowered = text.lower()
    for keyword in SUBCOMMITTEE_KEYWORDS:
        if keyword in lowered:
            return keyword.title()
    return None


def detect_agency(text: str) -> Optional[str]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for line in lines[:5]:
        if line.isupper() and len(line.split()) <= 8:
            return normalize_whitespace(line.title())
    return lines[0].title() if lines else None


def classify_page(page_number: int, text: str) -> PageClassification:
    lowered = text.lower()
    subcommittee = detect_subcommittee(text)
    agency = clean_agency_name(detect_agency(lines))
    confidence = 0.2

    if "policy" in lowered and "change" in lowered:
        page_type = "policy_changes"
        confidence = 0.9
    elif "subcommittee" in lowered and "summary" in lowered:
        page_type = "subcommittee_summary"
        confidence = 0.85
    elif "budget summary" in lowered or "positions" in lowered or lowered.count("fy 20") >= 2:
        page_type = "agency_detail"
        confidence = 0.8
    else:
        page_type = "narrative"
        confidence = 0.3

    return PageClassification(
        page_number=page_number,
        page_type=page_type,
        subcommittee=subcommittee,
        agency=agency,
        confidence=confidence,
    )


def group_pages(classifications: List[PageClassification]) -> List[PageGroup]:
    groups: List[PageGroup] = []
    current: Optional[PageGroup] = None
    for cls in classifications:
        if cls.page_type == "narrative":
            current = None
            continue
        if (
            current
            and cls.page_type == current.page_type
            and cls.subcommittee == current.subcommittee
            and cls.agency == current.agency
            and cls.page_number == current.end_page + 1
        ):
            current.end_page = cls.page_number
            continue
        current = PageGroup(
            start_page=cls.page_number,
            end_page=cls.page_number,
            page_type=cls.page_type,
            subcommittee=cls.subcommittee,
            agency=cls.agency,
        )
        groups.append(current)
    return groups


def parse_numeric_row(
    line: str,
    expected_values: int,
) -> Tuple[Optional[str], Optional[List[Optional[float]]]]:
    tokens = line.split()
    values: List[str] = []
    working = tokens[:]
    while working and token_is_numeric(working[-1]) and len(values) < expected_values:
        values.insert(0, working.pop())
    if len(values) != expected_values:
        return None, None
    account = normalize_whitespace(" ".join(working))
    parsed_values = [clean_money(token) for token in values]
    return account if account else None, parsed_values


def extract_lines(pdf: pdfplumber.PDF, group: PageGroup) -> List[str]:
    lines: List[str] = []
    for page_number in group.iter_pages():
        page = pdf.pages[page_number - 1]
        text = page.extract_text() or ""
        lines.extend(line.rstrip() for line in text.splitlines())
    return lines


def detect_fund(line: str) -> Optional[str]:
    if line.lower().endswith("fund"):
        return normalize_whitespace(line)
    return None


def parse_agency_group(
    pdf: pdfplumber.PDF,
    group: PageGroup,
    source_document: str,
) -> List[dict]:
    lines = extract_lines(pdf, group)
    records: List[dict] = []
    current_fund: Optional[str] = None
    for line in lines:
        normalized = normalize_whitespace(line)
        if not normalized:
            continue
        fund_candidate = detect_fund(normalized)
        if fund_candidate and not token_is_numeric(fund_candidate.split()[-1]):
            current_fund = fund_candidate
            continue
        account, values = parse_numeric_row(normalized, len(SCENARIO_MAP))
        if not account or values is None:
            continue
        row_type = "fund_total" if account.lower().startswith("total") else "account"
        record = {
            "source_document": source_document,
            "page": group.start_page,
            "subcommittee": group.subcommittee,
            "agency": group.agency,
            "fund": current_fund,
            "account": account,
            "row_type": row_type,
        }
        for (column, _, _), value in zip(SCENARIO_MAP, values):
            record[column] = value
        records.append(record)
    return records


def parse_subcommittee_group(
    pdf: pdfplumber.PDF,
    group: PageGroup,
    source_document: str,
) -> List[dict]:
    lines = extract_lines(pdf, group)
    rows: List[dict] = []
    current_fund: Optional[str] = None
    for line in lines:
        normalized = normalize_whitespace(line)
        if not normalized:
            continue
        fund_candidate = detect_fund(normalized)
        if fund_candidate and not token_is_numeric(fund_candidate.split()[-1]):
            current_fund = fund_candidate
            continue
        agency, values = parse_numeric_row(normalized, len(SCENARIO_MAP))
        if not agency or values is None:
            continue
        row_type = "appropriated_total" if agency.lower().startswith("total") else "agency_total"
        row = {
            "source_document": source_document,
            "page": group.start_page,
            "subcommittee": group.subcommittee,
            "fund": current_fund,
            "agency": agency,
            "row_type": row_type,
        }
        for (column, _, _), value in zip(SCENARIO_MAP, values):
            row[column] = value
        rows.append(row)
    return rows


def parse_policy_group(
    pdf: pdfplumber.PDF,
    group: PageGroup,
    source_document: str,
) -> List[dict]:
    lines = extract_lines(pdf, group)
    rows: List[dict] = []
    current_policy: Optional[str] = None
    current_fund: Optional[str] = None
    background_parts: List[str] = []
    governor_parts: List[str] = []
    legislative_parts: List[str] = []

    def flush_text() -> Tuple[str, str, str]:
        return (
            "\n".join(background_parts).strip(),
            "\n".join(governor_parts).strip(),
            "\n".join(legislative_parts).strip(),
        )

    for line in lines:
        normalized = normalize_whitespace(line)
        if not normalized:
            continue
        lower = normalized.lower()
        if normalized.isupper() and len(normalized.split()) > 2:
            current_policy = normalized
            background_parts.clear()
            governor_parts.clear()
            legislative_parts.clear()
            current_fund = None
            continue
        if lower.startswith("background"):
            background_parts.append(normalized.partition(":")[2].strip())
            continue
        if lower.startswith("governor"):
            governor_parts.append(normalized.partition(":")[2].strip())
            continue
        if lower.startswith("legislative"):
            legislative_parts.append(normalized.partition(":")[2].strip())
            continue
        fund_candidate = detect_fund(normalized)
        if fund_candidate and not token_is_numeric(fund_candidate.split()[-1]):
            current_fund = fund_candidate
            continue
        account, values = parse_numeric_row(normalized, len(POLICY_VALUE_COLUMNS))
        if not account or values is None:
            continue
        background_text, governor_text, legislative_text = flush_text()
        row = {
            "source_document": source_document,
            "page_start": group.start_page,
            "page_end": group.end_page,
            "subcommittee": group.subcommittee,
            "agency": group.agency,
            "policy_title": current_policy,
            "fund": current_fund,
            "account": account,
            "background_text": background_text,
            "governor_text": governor_text,
            "legislative_text": legislative_text,
        }
        for column, value in zip(POLICY_VALUE_COLUMNS, values):
            row[column] = value
        rows.append(row)
    return rows


def build_long_table(wide_df: pd.DataFrame) -> pd.DataFrame:
    if wide_df.empty:
        columns = [
            "source_document",
            "page",
            "subcommittee",
            "agency",
            "fund",
            "account",
            "row_type",
            "scenario",
            "fiscal_year",
            "amount",
        ]
        return pd.DataFrame(columns=columns)
    long_df = wide_df.melt(
        id_vars=[
            "source_document",
            "page",
            "subcommittee",
            "agency",
            "fund",
            "account",
            "row_type",
        ],
        value_vars=[column for column, _, _ in SCENARIO_MAP],
        var_name="scenario_column",
        value_name="amount",
    )
    scenario_lookup = {column: (scenario, year) for column, scenario, year in SCENARIO_MAP}
    long_df["scenario"] = long_df["scenario_column"].map(lambda col: scenario_lookup[col][0])
    long_df["fiscal_year"] = long_df["scenario_column"].map(lambda col: scenario_lookup[col][1])
    long_df = long_df.drop(columns=["scenario_column"])
    return long_df


def ensure_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    for column in columns:
        if column not in df.columns:
            df[column] = None
    return df[columns]


def write_sqlite(
    sqlite_path: Path,
    wide_df: pd.DataFrame,
    long_df: pd.DataFrame,
    policy_df: pd.DataFrame,
    subcommittee_df: pd.DataFrame,
) -> None:
    with sqlite3.connect(sqlite_path) as conn:
        wide_df.to_sql("budget_agency_accounts_wide", conn, if_exists="replace", index=False)
        long_df.to_sql("budget_agency_accounts_long", conn, if_exists="replace", index=False)
        policy_df.to_sql("budget_policy_changes", conn, if_exists="replace", index=False)
        subcommittee_df.to_sql("budget_subcommittee_summary", conn, if_exists="replace", index=False)


def write_artifacts(
    artifacts_dir: Path,
    classification_df: pd.DataFrame,
    wide_df: pd.DataFrame,
    long_df: pd.DataFrame,
    policy_df: pd.DataFrame,
    subcommittee_df: pd.DataFrame,
) -> None:
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    classification_df.to_csv(artifacts_dir / "page_classification.csv", index=False)
    wide_df.to_csv(artifacts_dir / "budget_agency_accounts_wide.csv", index=False)
    long_df.to_csv(artifacts_dir / "budget_agency_accounts_long.csv", index=False)
    policy_df.to_csv(artifacts_dir / "budget_policy_changes.csv", index=False)
    subcommittee_df.to_csv(artifacts_dir / "budget_subcommittee_summary.csv", index=False)


def run_pipeline(
    pdf_path: Path,
    sqlite_path: Path,
    artifacts_dir: Path,
    source_document: str,
    start_page: int,
) -> None:
    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        classifications: List[PageClassification] = []
        for page_num in range(start_page, total_pages + 1):
            page = pdf.pages[page_num - 1]
            text = page.extract_text() or ""
            classifications.append(classify_page(page_num, text))

        classification_df = pd.DataFrame(dataclasses.asdict(cls) for cls in classifications)
        groups = group_pages(classifications)

        agency_rows: List[dict] = []
        subcommittee_rows: List[dict] = []
        policy_rows: List[dict] = []

        for group in groups:
            if group.page_type == "agency_detail":
                agency_rows.extend(parse_agency_group(pdf, group, source_document))
            elif group.page_type == "subcommittee_summary":
                subcommittee_rows.extend(parse_subcommittee_group(pdf, group, source_document))
            elif group.page_type == "policy_changes":
                policy_rows.extend(parse_policy_group(pdf, group, source_document))

    wide_df = pd.DataFrame(agency_rows)
    long_df = build_long_table(wide_df)
    policy_df = pd.DataFrame(policy_rows)
    subcommittee_df = pd.DataFrame(subcommittee_rows)

    wide_columns = [
        "source_document",
        "page",
        "subcommittee",
        "agency",
        "fund",
        "account",
        "row_type",
    ] + [column for column, _, _ in SCENARIO_MAP]
    policy_columns = [
        "source_document",
        "page_start",
        "page_end",
        "subcommittee",
        "agency",
        "policy_title",
        "fund",
        "account",
    ] + POLICY_VALUE_COLUMNS + [
        "background_text",
        "governor_text",
        "legislative_text",
    ]
    subcommittee_columns = [
        "source_document",
        "page",
        "subcommittee",
        "fund",
        "agency",
        "row_type",
    ] + [column for column, _, _ in SCENARIO_MAP]

    wide_df = ensure_columns(wide_df, wide_columns)
    long_df = ensure_columns(
        long_df,
        [
            "source_document",
            "page",
            "subcommittee",
            "agency",
            "fund",
            "account",
            "row_type",
            "scenario",
            "fiscal_year",
            "amount",
        ],
    )
    policy_df = ensure_columns(policy_df, policy_columns)
    subcommittee_df = ensure_columns(subcommittee_df, subcommittee_columns)

    write_sqlite(sqlite_path, wide_df, long_df, policy_df, subcommittee_df)
    write_artifacts(artifacts_dir, classification_df, wide_df, long_df, policy_df, subcommittee_df)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild CT budget tables from the FY26/FY27 PDF.")
    parser.add_argument("--pdf-path", type=Path, required=True, help="Path to the Connecticut budget PDF.")
    parser.add_argument("--sqlite-path", type=Path, required=True, help="SQLite database output path.")
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=Path("artifacts"),
        help="Directory for CSV artifacts and page_classification.csv.",
    )
    parser.add_argument(
        "--source-document",
        type=str,
        default="2025BB-20250827_FY26_FY27.pdf",
        help="Source document label stored in the output tables.",
    )
    parser.add_argument(
        "--part-ii-start-page",
        type=int,
        default=18,
        help="1-based page number where Part II begins.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    run_pipeline(
        pdf_path=args.pdf_path,
        sqlite_path=args.sqlite_path,
        artifacts_dir=args.artifacts_dir,
        source_document=args.source_document,
        start_page=args.part_ii_start_page,
    )


if __name__ == "__main__":
    main()
