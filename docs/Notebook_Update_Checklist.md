# Notebook Update Follow-Up Checklist

> Last updated: 2026-03-29

When you change the Python files inside any of the following notebooks:

- `src/helper_functions_1.py.sql`
- `src/helper_functions_2.py.sql`
- `src/helper_functions_3.py.sql`
- `src/batch_processing.py.sql`

you **must** complete the steps below before opening a pull request. These actions keep the documentation, validation logic, and automated tests in sync with the updated helper code.

## 1. Refresh Published Examples
Update every example that showcases the affected helper behavior so that users see the latest syntax and outputs.

| File | Actions |
| --- | --- |
| `docs/CORE_COMPONENTS_REFERENCE.md` | Refresh walkthroughs, before/after snippets, and narrative descriptions that reference the new or changed helper functionality. |
| `docs/METADATA_GENERATION_GUIDE.md` | Align end-to-end examples and conversion workflow with the updated notebook logic. |
| `docs/TRANSFORMATION_PATTERNS_REFERENCE.md` | Update transformation SQL INSERT examples if transformation attributes changed. |
| `docs/METADATA_SQL_FILE_FORMAT.md` | Update SQL file format rules if notebook structure requirements changed. |

## 2. Sync the Metadata Reference and Helper Index
`docs/METADATA_REFERENCE.md` is the authoritative source for metadata configuration details. The split workflow guides (`DATA_INGESTION_GUIDE.md`, `DATA_TRANSFORMATION_GUIDE.md`, `CORE_COMPONENTS_REFERENCE.md`, etc.) remain the walkthrough and scenario guides. After a notebook change:

- Update [METADATA_REFERENCE.md](METADATA_REFERENCE.md) so configuration names, attributes, and accepted values reflect the new behavior.
- Amend the **Helper Functions** list to describe any new helpers, removed helpers, or signature changes.

## 3. Update Validation Logic
`.github/skills/metadata-validation/validate_metadata_sql.py` validates that metadata SQL files only use accepted values from [METADATA_REFERENCE.md](METADATA_REFERENCE.md). If you add new metadata parameters, you must update this validator script to include them. Ensure it:

- Accepts any new configuration attributes in `VALID_ATTRIBUTES`.
- Includes new valid values in `VALID_VALUES`.
- Adds any new required attributes to `REQUIRED_ATTRIBUTES`.
- Updates cross-configuration validations if behavior changes.
- Add new unit tests in `test_validate_metadata_sql.py` for any new validations.
- Run unit tests: `python -m pytest test_validate_metadata_sql.py -v`

## 4. Re-align Unit and Integration Tests
Inside `integration_tests/notebooks/`:

1. Add or update tests that cover the new helper behavior.
2. Remove or rewrite assertions that relied on the old implementation.
3. Re-run the suite locally to confirm the notebooks still pass their regression checks.

## 5. Verification Checklist
Before submitting the change:

- [ ] Examples and quick references in both docs are current.
- [ ] Metadata reference and helper function inventory are accurate.
- [ ] `validate_metadata_sql.py` enforces the right set of rules and all tests pass.
- [ ] `integration_tests/notebooks` has been updated and executed.

Keeping these artifacts in sync prevents regressions and gives downstream authors the correct guidance immediately after a helper change.
