---
applyTo: "integration_tests/**"
---

# Integration Test Instructions

> Auto-applied when working on integration test files.

---

## Test Structure

All core components have tests in `integration_tests/`:

| Component | Test Location | Examples |
|-----------|---------------|----------|
| **Notebooks** | `integration_tests/notebooks/` | test_transformations_basic.py, test_merge.py, test_scd2.py, test_file_ingestion.py |
| **Pipelines** | `integration_tests/pipelines/` | test_file_ingestion_pipeline.py |
| **Stored Procedures** | `integration_tests/stored_procedures/` | test_get_watermark_value.py, test_log_data_movement.py, test_pivot_primary_config.py |

---

## Rules

- **When modifying any core component, update the corresponding tests**
- Tests must validate both positive cases and edge cases
- Follow existing test patterns and naming conventions
- Add tests for any new transformations, DQ checks, or merge behaviors

---

## Related Documentation

| Topic | File | Search Pattern |
|-------|------|----------------|
| Notebook Update Checklist | `docs/Notebook_Update_Checklist.md` | `# Notebook Update Follow-Up Checklist` |
| Extending the Accelerator | `docs/Extending_the_Accelerator.md` | `# Extending the Accelerator` |
