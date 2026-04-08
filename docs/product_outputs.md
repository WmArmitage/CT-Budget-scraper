# Product Output Tiers

We now publish four distinct dataset tiers so buyers can pick the level of fidelity vs. polish they need.

| Tier | Files | Purpose |
| --- | --- | --- |
| **Clean (source-faithful)** | `data/processed/clean_budget.*` | Close to the PDF layout for traceability and re-processing. |
| **Deduped (analysis-ready)** | `data/processed/clean_budget_deduped.*` + `clean_budget_deduplication_audit.csv` | Removes literal duplicate rows and overlapping summary panels while keeping every actual figure. |
| **Finalized – Full** | `data/processed/clean_budget_deduped_full.*` | Buyer-facing dataset with short labels, contextual notes, normalized value labels, and provenance fields. Narrative prose is moved to `context_note`, and `notes` lists the transformations applied. |
| **Finalized – Slim** | `data/processed/clean_budget_deduped_slim.*` | Minimal sheet-friendly view (agency/section/program/line_item/value_label/fiscal_year/amount/page) for quick analysis without narrative columns. |

## Finalization highlights

- **Line item sanitization:** values above 60–100 characters or with sentence-like structure are treated as narrative. Long text is relocated to `context_note`; a short label is chosen from description/program/section/value_label.
- **Description policy:** if the descriptive field was narrative, it is summarized or replaced with a short contextual label. No finalized rows ship with a blank `description`.
- **Value-label normalization:** FY tokens and common headers (`General Fund`, `Other Appropriated`, `Actual FY 2024`, etc.) are standardized while the original text remains in `original_value_label`.
- **Reporting:** `data/processed/clean_budget_productization_report.csv` logs every change (line-item moves, description promotions, value-label normalization) with the affected agency and page for auditability.

## Commands

```powershell
# Deduplicate (if needed)
python scripts/dedupe_budget_data.py `
  --input data/processed/clean_budget.ndjson `
  --ndjson-out data/processed/clean_budget_deduped.ndjson `
  --csv-out data/processed/clean_budget_deduped.csv `
  --sqlite-out data/processed/clean_budget_deduped.sqlite `
  --audit-out data/processed/clean_budget_deduplication_audit.csv

# Finalize into full + slim exports + productization report
python scripts/finalize_budget_product.py `
  --input data/processed/clean_budget_deduped.ndjson `
  --full-ndjson-out data/processed/clean_budget_deduped_full.ndjson `
  --full-csv-out data/processed/clean_budget_deduped_full.csv `
  --full-sqlite-out data/processed/clean_budget_deduped_full.sqlite `
  --slim-ndjson-out data/processed/clean_budget_deduped_slim.ndjson `
  --slim-csv-out data/processed/clean_budget_deduped_slim.csv `
  --slim-sqlite-out data/processed/clean_budget_deduped_slim.sqlite `
  --report-out data/processed/clean_budget_productization_report.csv

# Validate any tier (examples)
python scripts/validate_budget_data.py --input data/processed/clean_budget_deduped_full.ndjson `
  --baseline data/processed/clean_budget_deduped.ndjson
python scripts/validate_budget_data.py --input data/processed/clean_budget_deduped_slim.ndjson `
  --baseline data/processed/clean_budget_deduped_full.ndjson
```

## Field reference

**Full export** columns: `source_document, agency, section, program, line_item, value_label, original_value_label, fiscal_year, amount, page, description, notes, context_note, merged_row_count`.

**Slim export** columns: `agency, section, program, line_item, value_label, fiscal_year, amount, page`.

Notes:
- `context_note` stores long-form narratives migrated from line-item/description.
- `notes` lists transformations applied (e.g., value-label normalization, narrative relocation).
- `merged_row_count` comes from the deduped tier so buyers know how many source rows contributed to a figure.
