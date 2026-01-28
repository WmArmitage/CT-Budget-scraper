#!/usr/bin/env python3
"""
CT Budget PDF Scraper (optimized + fixes for paragraph-as-table fragmentation)

What this version fixes (vs your current script):
- Prevents narrative text blocks (Background / Governor / Legislative) from being mis-parsed as tables
- Uses two-pass table extraction (lines-first, fallback-to-text)
- Filters out “fake tables” (paragraph text split into many columns)
- No double-append bug (tables are serialized once, after cleaning)
- Drops empty rows/cols before serialization
- Vectorized numeric cleaning (fast)
- More robust header merging + unique column names
- Optional NDJSON streaming output (fast + low memory)

Dependencies:
  pip install pdfplumber pandas
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber
import pandas as pd

# -------------------------
# Precompiled regex patterns
# -------------------------

RE_WS = re.compile(r"\s+")
RE_TRAILING_FOOTNOTE = re.compile(r"(?<=[A-Za-z])\d+$")
RE_HAS_DIGIT = re.compile(r"\d")
RE_HAS_ALPHA = re.compile(r"[A-Za-z]")

RE_AGENCY = re.compile(r"(?:AGENCY|Agency)\s*:\s*([^\n\r:]+)")
RE_SECTION = re.compile(r"(?:SECTION|Section)\s*:\s*([^\n\r:]+)")

# Column name hints for numeric columns (fast path)
RE_COL_HINT = re.compile(r"(?:\bFY\b|20\d{2}|Amount|Total|Appropriated|Estimated|Enacted)", re.IGNORECASE)


# -------------------------
# Text cleaning
# -------------------------

def clean_text(text: Any) -> str:
    """Clean text for headers/labels/cells: normalize whitespace + strip trailing footnote digits."""
    if text is None:
        return ""
    s = str(text)
    s = RE_WS.sub(" ", s).strip()
    s = RE_TRAILING_FOOTNOTE.sub("", s).strip()
    return s


# -------------------------
# Header handling
# -------------------------

def merge_multi_line_headers(rows: List[List[Any]], max_header_rows: int = 3) -> List[List[Any]]:
    """
    Merge top rows if they look like header fragments.
    Heuristic: rows with no digits are treated as header continuation.
    """
    if not rows or len(rows) < 2:
        return rows

    width = max(len(r) for r in rows)
    norm = [list(r) + [None] * (width - len(r)) for r in rows]

    def row_digit_score(r: List[Any]) -> int:
        return sum(1 for c in r if c and RE_HAS_DIGIT.search(str(c)))

    merged = [clean_text(c) for c in norm[0]]
    consumed = 1

    for k in range(1, min(max_header_rows, len(norm))):
        if row_digit_score(norm[k]) == 0:
            add = [clean_text(c) for c in norm[k]]
            merged = [(a + " " + b).strip() if b else a for a, b in zip(merged, add)]
            consumed += 1
        else:
            break

    return [merged] + norm[consumed:]


def uniqueify_columns(cols: List[str]) -> List[str]:
    """Rename duplicate/blank columns to avoid collisions: Header, Header_1, ..."""
    seen: Dict[str, int] = {}
    out: List[str] = []
    for c in cols:
        base = c if c else "Unnamed"
        n = seen.get(base, 0)
        out.append(base if n == 0 else f"{base}_{n}")
        seen[base] = n + 1
    return out


# -------------------------
# Metadata detection (fast crop)
# -------------------------

def detect_metadata_from_header(page: pdfplumber.page.Page, header_height: float = 120) -> Tuple[str, str]:
    """
    Extract agency/section from a cropped header band (faster + less noisy than full page).
    Note: Your PDF may not actually use AGENCY:/SECTION: labels; this is still safe.
    """
    w, h = page.width, page.height
    hh = min(header_height, h)
    header = page.crop((0, 0, w, hh))
    text = header.extract_text() or ""

    agency = "Unknown Agency"
    section = "General Table"

    m_ag = RE_AGENCY.search(text)
    m_se = RE_SECTION.search(text)
    if m_ag:
        agency = clean_text(m_ag.group(1))
    if m_se:
        section = clean_text(m_se.group(1))

    return agency, section


# -------------------------
# Narrative blocks extraction
# -------------------------

def extract_policy_blocks(page: pdfplumber.page.Page) -> Dict[str, str]:
    """
    Extract narrative blocks like:
      Background
      Governor
      Legislative
    Returns keys: background/governor/legislative (if found).
    """
    text = page.extract_text() or ""
    text = text.replace("\r", "\n")

    out: Dict[str, str] = {}
    # Capture each block from label to next label/end
    pattern = r"(?ms)^(Background|Governor|Legislative)\s*\n(.*?)(?=^(Background|Governor|Legislative)\s*\n|\Z)"
    for m in re.finditer(pattern, text):
        label = m.group(1).strip().lower()
        body = RE_WS.sub(" ", m.group(2)).strip()
        if body:
            out[label] = body

    return out


# -------------------------
# Table plausibility / filters
# -------------------------

def table_numeric_density(df: pd.DataFrame) -> float:
    """Fraction of cells containing at least one digit."""
    if df.empty:
        return 0.0
    s = df.astype(str).fillna("")
    total = s.size
    if total == 0:
        return 0.0
    digit_cells = s.applymap(lambda x: bool(RE_HAS_DIGIT.search(x))).values.sum()
    return float(digit_cells) / float(total)


def header_fragmentation_score(columns: List[str]) -> float:
    """Fraction of headers that are very short (common in chopped paragraph-as-table headers)."""
    if not columns:
        return 1.0
    short = 0
    for c in columns:
        c = (c or "").strip()
        if len(c) <= 7:
            short += 1
    return short / max(1, len(columns))


def looks_like_real_table(df: pd.DataFrame) -> bool:
    """
    Conservative filter: reject paragraph-text hallucinated as tables.
    Tuned for budget documents where real tables are numeric-heavy.
    """
    if df is None or df.empty:
        return False

    rows, cols = df.shape

    # Fake tables often have many columns but very few rows
    if cols >= 6 and rows <= 3:
        return False

    # Budget tables should have some numeric density
    if table_numeric_density(df) < 0.08:
        return False

    # Chopped headers tend to be many tiny fragments
    if cols >= 6 and header_fragmentation_score(list(df.columns)) > 0.50:
        return False

    return True


# -------------------------
# Optional "skip non-table pages" heuristic
# -------------------------

def looks_like_table_page(page: pdfplumber.page.Page, sample_band_top: float = 140) -> bool:
    """
    Conservative heuristic to skip pages unlikely to contain tables:
    if the body contains very few digits, it’s likely narrative/dividers.
    """
    w, h = page.width, page.height
    band_top = min(sample_band_top, h * 0.25)
    body = page.crop((0, band_top, w, h))
    text = body.extract_text() or ""
    if not text:
        return False

    digits = sum(ch.isdigit() for ch in text)
    return digits >= 40


# -------------------------
# DataFrame normalization + numeric cleaning
# -------------------------

def normalize_table_to_dataframe(table: List[List[Any]]) -> Optional[pd.DataFrame]:
    """Convert a pdfplumber table (list-of-rows) to a cleaned DataFrame."""
    if not table or len(table) < 2:
        return None

    processed = merge_multi_line_headers(table, max_header_rows=3)
    header = uniqueify_columns([clean_text(c) for c in processed[0]])
    body_rows = processed[1:]
    if not body_rows:
        return None

    df = pd.DataFrame(body_rows, columns=header)

    # Early cleanup to reduce junk
    df.columns = [clean_text(c) for c in df.columns]
    df.replace({"": None}, inplace=True)
    df.dropna(axis=0, how="all", inplace=True)
    df.dropna(axis=1, how="all", inplace=True)

    if df.empty or df.shape[1] == 0:
        return None

    # Re-uniqueify after dropping columns
    df.columns = uniqueify_columns(list(df.columns))
    return df


def is_numeric_column(col_name: str, series: pd.Series, sample_size: int = 25) -> bool:
    """Detect numeric columns by name hint + content sampling."""
    name = str(col_name or "")
    if RE_COL_HINT.search(name):
        return True

    s = series.dropna()
    if s.empty:
        return False

    sample = s.astype(str).head(sample_size).str.strip()
    if sample.empty:
        return False

    has_digit_ratio = sample.map(lambda x: bool(RE_HAS_DIGIT.search(x))).mean()
    has_alpha_ratio = sample.map(lambda x: bool(RE_HAS_ALPHA.search(x))).mean()

    return (has_digit_ratio >= 0.65) and (has_alpha_ratio <= 0.25)


def clean_numeric_series(series: pd.Series) -> pd.Series:
    """
    Vectorized numeric cleaning:
      - blanks / '-' / N/A -> 0
      - parentheses -> negative
      - strip currency/commas/extra symbols
    """
    s = series.fillna("").astype(str).str.strip()

    s = s.replace({"-": "", "None": "", "null": "", "N/A": "", "n/a": "", "NA": ""})

    neg = s.str.match(r"^\(.*\)$")
    s = s.str.replace(r"^\(|\)$", "", regex=True)

    s = s.str.replace(r"[^0-9.\-]", "", regex=True)

    num = pd.to_numeric(s, errors="coerce").fillna(0.0)
    num.loc[neg] = -num.loc[neg].abs()
    return num


# -------------------------
# Main scrape function
# -------------------------

def scrape_budget(
    pdf_path: Path,
    output_path: Path,
    *,
    pretty: bool = False,
    ndjson: bool = False,
    skip_non_table_pages: bool = False,
    header_height: float = 120,
    progress_every: int = 10,
) -> None:
    """
    If ndjson=True, writes one JSON object per table row:
      {"agency":..., "section":..., "page":..., "row":{...}, "background":..., ...}

    Else writes nested JSON:
      {agency: {section: {"tables":[...], "notes":[{"page":..,"background":..}, ...]}}}
    """

    # Two-pass table extraction settings
    table_settings_lines = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "snap_tolerance": 3,
        "intersection_x_tolerance": 10,
        "intersection_y_tolerance": 10,
    }

    table_settings_text = {
        "vertical_strategy": "text",
        "horizontal_strategy": "text",
        "snap_tolerance": 4,
        "intersection_x_tolerance": 15,
        "text_x_tolerance": 2,
        "text_y_tolerance": 2,
    }

    if ndjson:
        out_f = output_path.open("w", encoding="utf-8")
    else:
        structured: Dict[str, Dict[str, Dict[str, Any]]] = {}

    with pdfplumber.open(str(pdf_path)) as pdf:
        last_agency = "General Fund / Statewide"

        for i, page in enumerate(pdf.pages):
            if skip_non_table_pages and not looks_like_table_page(page):
                if (i + 1) % progress_every == 0:
                    print(f"Skipped (heuristic) {i+1} pages...")
                continue

            agency, section = detect_metadata_from_header(page, header_height=header_height)
            if agency == "Unknown Agency":
                agency = last_agency
            else:
                last_agency = agency

            # Extract narrative policy blocks safely (not as tables)
            policy_blocks = extract_policy_blocks(page)

            # Extract tables (two-pass)
            tables = page.extract_tables(table_settings_lines) or []
            if not tables:
                tables = page.extract_tables(table_settings_text) or []

            if not tables:
                # Still store notes (optional) even if no tables
                if (not ndjson) and policy_blocks:
                    structured.setdefault(agency, {}).setdefault(section, {}).setdefault("notes", [])
                    structured[agency][section]["notes"].append({"page": i + 1, **policy_blocks})

                if (i + 1) % progress_every == 0:
                    print(f"Processed {i+1} pages (no tables)...")
                continue

            for table in tables:
                df = normalize_table_to_dataframe(table)
                if df is None:
                    continue

                # Filter out paragraph-text hallucinated as tables
                if not looks_like_real_table(df):
                    continue

                # Clean columns (vectorized numeric where appropriate)
                for col in list(df.columns):
                    if is_numeric_column(col, df[col]):
                        df[col] = clean_numeric_series(df[col])
                    else:
                        df[col] = df[col].map(clean_text)

                records = df.to_dict(orient="records")

                if ndjson:
                    for r in records:
                        out_obj = {
                            "agency": agency,
                            "section": section,
                            "page": i + 1,
                            "row": r,
                            **policy_blocks,
                        }
                        out_f.write(json.dumps(out_obj, ensure_ascii=False) + "\n")
                else:
                    structured.setdefault(agency, {}).setdefault(section, {}).setdefault("tables", []).extend(records)
                    if policy_blocks:
                        structured[agency][section].setdefault("notes", [])
                        structured[agency][section]["notes"].append({"page": i + 1, **policy_blocks})

            if (i + 1) % progress_every == 0:
                print(f"Validated and processed {i+1} pages...")

    if ndjson:
        out_f.close()
        print(f"Success. NDJSON exported to {output_path}")
        return

    with output_path.open("w", encoding="utf-8") as f:
        if pretty:
            json.dump(structured, f, indent=2, ensure_ascii=False)
        else:
            json.dump(structured, f, ensure_ascii=False)

    print(f"Success. Cleaned data exported to {output_path}")


# -------------------------
# CLI
# -------------------------

def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Scrape CT Budget PDF tables + narrative notes to JSON.")
    p.add_argument("--input-pdf", required=True, help="Path to input PDF")
    p.add_argument("--output", required=True, help="Output .json (or .ndjson with --ndjson)")
    p.add_argument("--pretty", action="store_true", help="Pretty-print JSON (slower/larger)")
    p.add_argument("--ndjson", action="store_true", help="Write newline-delimited JSON (streaming-friendly)")
    p.add_argument("--skip-non-table-pages", action="store_true",
                   help="Heuristic skip pages unlikely to contain tables (faster, small risk of skipping)")
    p.add_argument("--header-height", type=float, default=120, help="Header crop height for metadata scan")
    p.add_argument("--progress-every", type=int, default=10, help="Progress print interval (pages)")
    return p


def main() -> None:
    args = build_argparser().parse_args()
    pdf_path = Path(args.input_pdf).expanduser().resolve()
    out_path = Path(args.output).expanduser().resolve()

    scrape_budget(
        pdf_path=pdf_path,
        output_path=out_path,
        pretty=args.pretty,
        ndjson=args.ndjson,
        skip_non_table_pages=args.skip_non_table_pages,
        header_height=args.header_height,
        progress_every=max(1, args.progress_every),
    )


if __name__ == "__main__":
    main()
