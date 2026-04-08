# CT-Budget-scraper

This project turns the FY26–FY27 Connecticut budget PDF into a structured dataset that can be sold or plugged directly into analytic tools.

## Pipeline overview

1. **Extract** – `scrape_budget_v2.py` walks the PDF with `pdfplumber`, merges headers, filters fake tables, and exports raw NDJSON (one JSON row per PDF row) keeping page, agency, and section metadata intact.
2. **Clean** – `scripts/clean_budget_data.py` loads the raw NDJSON, removes OCR artifacts (dotted leaders, broken words), maps unlabeled columns (`Unnamed_*`) to their real headers, promotes account/program fields, and explodes each numeric column into a normalized row. It produces:
   - `data/processed/clean_budget.ndjson`
   - `data/processed/clean_budget.csv`
   - `data/processed/clean_budget.sqlite`
3. **Deduplicate (buyer-facing tier)** – `scripts/dedupe_budget_data.py` runs a conservative two-stage deduplication (exact match removal plus summary-panel consolidation) and adds `merged_row_count` + an audit trail. It produces:
   - `data/processed/clean_budget_deduped.ndjson`
   - `data/processed/clean_budget_deduped.csv`
   - `data/processed/clean_budget_deduped.sqlite`
   - `data/processed/clean_budget_deduplication_audit.csv`
4. **Finalize (product tier)** – `scripts/finalize_budget_product.py` splits short line-item labels from long narrative text, normalizes value labels, keeps narrative context in `context_note`, and produces both a full and slim buyer dataset plus a productization report.
5. **Validate** – `scripts/validate_budget_data.py` reports row counts, % missing description/amount, duplicate patterns, line-item length stats, and (optionally) compares tiers while sampling audit logs.

See `docs/deduplication_method.md` for the full dedupe methodology and `docs/product_outputs.md` for field definitions + tier guidance.

## Two product tiers

| Tier | Files | What it’s for |
| --- | --- | --- |
| **Clean / source-faithful** | `data/processed/clean_budget.*` | Mirrors the PDF as closely as possible, best for trace-back or reprocessing. |
| **Deduped / analyst-ready** | `data/processed/clean_budget_deduped.*` + audit | Removes repeated summary panels (General Fund / Other Appropriated / All Appropriated) and identical clones while preserving unique figures. Every deduped row includes `merged_row_count` for provenance. |
| **Finalized / full** | `data/processed/clean_budget_deduped_full.*` + `clean_budget_productization_report.csv` | Short line-item labels, normalized value labels, optional `notes`/`context_note` columns, and provenance fields for premium buyers. |
| **Finalized / slim** | `data/processed/clean_budget_deduped_slim.*` | Minimal Excel/Sheets-ready columns (agency/section/program/line_item/value_label/fiscal_year/amount/page). |

## Running the pipeline

From the repository root:

```powershell
# 1) Optional – rebuild the raw NDJSON if the PDF or scraper changed.
python scrape_budget_v2.py --input-pdf "2025BB-20250827_FY 26 and FY 27 Connecticut Budget.pdf" --output ct_budget_optimized.ndjson --ndjson

# 2) Clean + export CSV/NDJSON/SQLite (defaults target data/processed/).
python scripts/clean_budget_data.py --input ct_budget_optimized.ndjson `
  --ndjson-out data/processed/clean_budget.ndjson `
  --csv-out data/processed/clean_budget.csv `
  --sqlite-out data/processed/clean_budget.sqlite

# 3) Create buyer-friendly deduped exports + audit trail.
python scripts/dedupe_budget_data.py `
  --input data/processed/clean_budget.ndjson `
  --ndjson-out data/processed/clean_budget_deduped.ndjson `
  --csv-out data/processed/clean_budget_deduped.csv `
  --sqlite-out data/processed/clean_budget_deduped.sqlite `
  --audit-out data/processed/clean_budget_deduplication_audit.csv

# 4) Finalize into full (rich) + slim (minimal) buyer datasets plus the productization report.
python scripts/finalize_budget_product.py `
  --input data/processed/clean_budget_deduped.ndjson `
  --full-ndjson-out data/processed/clean_budget_deduped_full.ndjson `
  --full-csv-out data/processed/clean_budget_deduped_full.csv `
  --full-sqlite-out data/processed/clean_budget_deduped_full.sqlite `
  --slim-ndjson-out data/processed/clean_budget_deduped_slim.ndjson `
  --slim-csv-out data/processed/clean_budget_deduped_slim.csv `
  --slim-sqlite-out data/processed/clean_budget_deduped_slim.sqlite `
  --report-out data/processed/clean_budget_productization_report.csv

# 5) Validate whichever tier you plan to deliver (baseline flag optional but recommended).
python scripts/validate_budget_data.py `
  --input data/processed/clean_budget_deduped_full.ndjson `
  --baseline data/processed/clean_budget_deduped.ndjson

python scripts/validate_budget_data.py `
  --input data/processed/clean_budget_deduped_slim.ndjson `
  --baseline data/processed/clean_budget_deduped_full.ndjson
```

## Output schema

Every cleaned record includes (minimum required fields plus helpful extras):

| Field | Description |
| --- | --- |
| `source_document` | Fixed label (`FY26-FY27 Connecticut Budget`). |
| `agency`, `section` | Detected from PDF headers. |
| `program` | Program/division inferred from table text (falls back to section). |
| `line_item` | Account/line description derived from the leftmost descriptive column. |
| `value_label` | Cleaned/normalized column header (`Actual FY 2024`, `General Fund`, etc.). |
| `original_value_label` | (Finalized full tier) Raw header preserved for traceability when normalization occurs. |
| `fiscal_year` | Parsed four-digit year (if available). |
| `amount` | Numeric value (floats, parentheses converted to negatives). |
| `page` | PDF page number for traceability. |
| `description` | Short context string (summarized from the PDF or section/program labels). |
| `notes` | (Finalized full tier) Transformation notes (e.g., “Line item narrative moved to context_note”). |
| `context_note` | (Finalized full tier) Full narrative/prose preserved from the PDF. |
| `merged_row_count` | (Deduped + finalized full tiers) how many source rows collapsed into this record. |

The CSV mirrors these columns and the SQLite databases store them in `budget` (clean) and `budget_deduped` (buyer tier) tables for direct loading into BI tools.

## Improvements vs. legacy NDJSON

- Dotted leader junk (e.g., `..... 4`) is stripped and never shows up as a column.
- Columns formerly named `Unnamed_*` now inherit the nearest textual header (like `FY 26 ($)`).
- Line-item text that was split across columns (`OVER VIE` + `W`) is merged to proper words.
- Output is delivered in NDJSON, CSV, SQLite, and a deduped buyer tier, making it trivial to ingest into Excel, DuckDB, or warehouses.
- Validation stats call out sections that still export zeros or lack descriptions so you can triage re-extraction if needed and quantify the dedupe reduction.

## Next steps

If specific agencies still misbehave (e.g., tables rendered as prose), rerun scrape_budget_v2.py with a tighter page subset or tweak its table heuristics, then run the clean + validate steps again to refresh the polished dataset.
