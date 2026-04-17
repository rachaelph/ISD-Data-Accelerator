# Custom Function Selection

> Last updated: 2026-03-29

This is the canonical decision reference for choosing between `custom_source_function`, `custom_staging_function`, `custom_table_ingestion_function`, `custom_file_ingestion_function`, and `custom_transformation_function`.

Use this file when the question is: which custom function should be used for this scenario?

## Canonical Selection Rules

```text
CUSTOM_FUNCTION_SELECTION_RULES

Rule 0: If built-in metadata transformations can solve the requirement,
use built-in transformations and DO NOT use a custom function.

Rule 1: If custom logic runs after ingestion on an already-loaded DataFrame,
use custom_transformation_function.

Rule 2: If custom logic runs during ingestion and the framework has already
discovered file paths for you, use custom_file_ingestion_function.

Rule 3: If custom logic runs during ingestion and the source is an internal
Unity Catalog table, use custom_table_ingestion_function.
The framework injects watermark_filter, watermark_column_name, and watermark_value.

Rule 4: If custom logic runs during ingestion and the source is an external
system (API, SDK, service, external database), then:

4a. If the source is an API (especially returning JSON/XML), prefer
custom_staging_function to land raw responses as-is, then parse into Bronze.
This follows medallion best practice: land raw data first, then transform.

4b. If the function must write files into staging and control file names,
formats, or folder structure for non-API sources, use custom_staging_function.

4c. If the function should return a DataFrame directly and raw file
preservation is not needed, use custom_source_function.

Rule 5: Do not use custom_source_function together with staging_folder_path,
wildcard_folder_path, or table_name.

Rule 6: Do not use custom_table_ingestion_function and custom_source_function
for the same Table_ID. Only one source-side custom function can be active.

Rule 7: Use custom_source_function for external extraction logic.
Use custom_table_ingestion_function for internal Unity Catalog table reads.
These are not interchangeable watermark contracts.

Rule 8: For API sources (especially JSON/XML), prefer custom_staging_function
to land raw responses as-is in UC volumes before parsing into Bronze
(medallion best practice). Use custom_source_function only when the source
returns small, structured tabular data and raw file preservation is not needed.

Rule 9 (trade-offs): Custom functions (.py files) give full Python control
but typically require Databricks secrets for credentials.
```

## Short Decision Tree

1. Can built-in transformations solve it?
   If yes, stop. Do not use a custom function.
2. Does the custom logic run after ingestion on `new_data`?
   If yes, use `custom_transformation_function`.
3. Does the framework already have file paths and you only need custom parsing?
   If yes, use `custom_file_ingestion_function`.
4. Are you reading from an internal Unity Catalog table with custom logic?
   If yes, use `custom_table_ingestion_function`.
5. Are you reading from an external source?
   If yes, decide between:
   `custom_staging_function` **(preferred for APIs)** — land raw responses as-is, then parse into Bronze.
   `custom_source_function` when raw file preservation is not needed and a DataFrame return suffices.

## Quick-Reference Comparison

| Custom Function | Metadata Key | Signature | Returns | Watermark | Owns File I/O? | Typical Use Case |
|---|---|---|---|---|---|---|
| **Transformation** | `custom_transformation_function` | `fn(new_data, metadata, spark)` | DataFrame | N/A (post-ingestion) | No | Reshape, enrich, ML inference after data lands |
| **Source** | `custom_source_function` | `fn(metadata, spark)` | DataFrame _or_ (DataFrame, next_wm) | Function-managed via `watermark_value` | No | External APIs/SDKs returning tabular data |
| **Staging** | `custom_staging_function` | `fn(metadata, spark)` | dict (`rows_copied`, `next_watermark_value`) | Function-managed via `watermark_value` | **Yes** | External sources needing file-level control |
| **Table Ingestion** | `custom_table_ingestion_function` | `fn(metadata, spark)` | DataFrame | Framework-injected `watermark_filter` | No | Custom reads from internal Unity Catalog tables |
| **File Ingestion** | `custom_file_ingestion_function` | `fn(file_paths, all_metadata, spark)` | DataFrame | Framework-managed (file discovery) | No | Custom parsing of framework-discovered files |

## Key Distinctions

- Source vs Staging: both pull from external systems. For API sources (especially JSON/XML), prefer `custom_staging_function` to land raw responses as-is before parsing into Bronze — this follows medallion best practice of preserving source fidelity and enables reprocessing without re-calling the API. Use `custom_source_function` when the source returns small, structured tabular data and raw file preservation is not needed.
- Source vs Table Ingestion: both return DataFrames. Use `custom_source_function` for external systems where the function manages its own watermark. Use `custom_table_ingestion_function` for internal Unity Catalog tables where the framework injects `watermark_filter` and `watermark_column_name`.

## Related References

- [TRANSFORMATION_DECISION_TREE.md](TRANSFORMATION_DECISION_TREE.md)
- [METADATA_AUTHORING_WORKFLOW.md](METADATA_AUTHORING_WORKFLOW.md)
- [resources/Custom_Source_Function.py](resources/Custom_Source_Function.py)
- [resources/Custom_Staging_Function.py](resources/Custom_Staging_Function.py)
- [resources/Custom_Table_Ingestion_Function.py](resources/Custom_Table_Ingestion_Function.py)
