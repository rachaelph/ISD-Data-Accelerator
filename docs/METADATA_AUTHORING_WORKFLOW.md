# Metadata Authoring Workflow

> Last updated: 2026-04-15

This is the canonical procedural workflow for creating or editing metadata in the accelerator.

Use this file when the question is: how should metadata be authored, updated, validated, or delivered?

## Workflow

1. Choose the target medallion layer and processing goal.
   Decide whether the work is ingestion, transformation, or orchestration-only.

2. Define the orchestration record.
   Set `Trigger_Name`, `Order_Of_Operations`, `Table_ID`, `Target_Datastore`, `Target_Entity`, `Primary_Keys`, `Processing_Method`, and `Ingestion_Active`.

3. Choose the source and write pattern.
   Use [INGESTION_PATTERNS_REFERENCE.md](INGESTION_PATTERNS_REFERENCE.md) for ingestion patterns and [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) if custom code is needed.

4. Populate primary configuration.
   Use [METADATA_REFERENCE.md](METADATA_REFERENCE.md) for valid categories, names, accepted values, and configuration constraints.

5. Add advanced configuration only when needed.
   Use [TRANSFORMATION_DECISION_TREE.md](TRANSFORMATION_DECISION_TREE.md) to decide whether built-ins are enough before introducing custom functions.

6. Validate before delivery.
   Run `python .github/skills/metadata-validation/validate_metadata_sql.py <file>` for metadata SQL changes.

7. Deliver both metadata and code assets when custom code is used.
   If metadata references a custom function file, create or update the `.py` file in the same change set.

## Authoring Guardrails

- Do not invent configuration names or accepted values.
- Use exact accepted values from the canonical metadata reference.
- Keep metadata environment-agnostic by using datastore references instead of hardcoded IDs.
- Prefer built-in transformations before custom code.
- Keep custom-function selection and metadata structure aligned with the validator.

## Canonical References

- [METADATA_REFERENCE.md](METADATA_REFERENCE.md)
- [TRANSFORMATION_DECISION_TREE.md](TRANSFORMATION_DECISION_TREE.md)
- [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md)
- [INGESTION_PATTERNS_REFERENCE.md](INGESTION_PATTERNS_REFERENCE.md)
- [METADATA_GENERATION_GUIDE.md](METADATA_GENERATION_GUIDE.md)
