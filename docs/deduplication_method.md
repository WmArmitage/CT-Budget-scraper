# Deduplication Methodology

We now publish two dataset tiers:

1. **Clean (source-faithful)** – `data/processed/clean_budget.*` mirrors the PDF tables with minimal interference.
2. **Deduped (buyer-friendly)** – `data/processed/clean_budget_deduped.*` removes mechanical repeats from summary panels while keeping every distinct budget figure.

## How deduplication works

1. **Exact duplicate sweep** – rows that match across all exported fields (including section, line item, value label, fiscal year, amount, page, and description) are collapsed. Their `merged_row_count` reflects how many identical records were removed.
2. **Summary-panel consolidation** – for the high-priority summary labels (General Fund, Other Appropriated, All Appropriated) we normalize agency/program/line-item text and collapse groups only when:
   - the numeric amount, fiscal year/label, and normalized line item agree,
   - sections/programs/descriptions vary by at most a few normalized values, and
   - the rows live on the same page band (within ±2 pages).
   Groups that do not meet these safety tests remain untouched and are logged as "retained" in the audit file.
3. **Traceability** – deduped rows always include `merged_row_count`, and every merge (or skipped-but-detected group) is recorded in `data/processed/clean_budget_deduplication_audit.csv` with the pages and context that were reviewed.

## Outputs produced

- `data/processed/clean_budget_deduped.ndjson`
- `data/processed/clean_budget_deduped.csv`
- `data/processed/clean_budget_deduped.sqlite` (table `budget_deduped`)
- `data/processed/clean_budget_deduplication_audit.csv` – contains `group_id`, merge/retain action, reason, affected pages, and notes.

## Known limitations

- Some OCR artifacts (for example `Other Current Expenses` rows split into dozens of subtokens) still repeat even after deduping because they fail the safety thresholds.
- Deduping is intentionally conservative; if the numeric series or textual context diverges meaningfully, the rows are left intact.
- Missing descriptions remain where the PDF never provided a descriptive cell; these require upstream extraction fixes rather than deduplication.

Run the deduper anytime you refresh the clean export:

```powershell
python scripts/dedupe_budget_data.py `
  --input data/processed/clean_budget.ndjson `
  --ndjson-out data/processed/clean_budget_deduped.ndjson `
  --csv-out data/processed/clean_budget_deduped.csv `
  --sqlite-out data/processed/clean_budget_deduped.sqlite `
  --audit-out data/processed/clean_budget_deduplication_audit.csv
```

Then re-run validation to compare tiers:

```powershell
python scripts/validate_budget_data.py `
  --input data/processed/clean_budget_deduped.ndjson `
  --baseline data/processed/clean_budget.ndjson `
  --audit data/processed/clean_budget_deduplication_audit.csv
```
