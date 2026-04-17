# Metadata Authoring Reference

Use this shared reference for metadata generation, conversion, and non-trivial edit workflows instead of embedding large lookup tables in prompts or instructions.

## Authoritative Contract

`.github/contracts/metadata-schema.v1.json` is the machine-readable authority for:

- `orchestration_config` — allowed `processing_methods`, `target_datastores`, and staging-capable methods
- `category_placement` — which categories belong in Primary vs Advanced Config
- `valid_attributes` — complete attribute list per category (e.g. `source_details`, `target_details`, `data_transformation_steps`)
- `transformation_attributes` — optional attributes available on each built-in transformation
- `required_attributes` — attributes that MUST be present on each transformation (e.g. `join_data` requires `join_condition`, `join_type`, `left_columns`, `right_columns`, `right_table_name`)

**When authoring or editing metadata, consult the contract first for exact values, defaults, and required attributes.** Markdown guides below explain patterns and provide examples; the contract wins on precision.

## Reference Docs

Load only what the request needs.

| Topic | File | How to load |
|-------|------|-------------|
| Metadata contract (accepted values, required attributes) | `.github/contracts/metadata-schema.v1.json` | `grep_search` for the specific category or transformation name |
| Metadata explanation and examples | `docs/METADATA_REFERENCE.md` | read targeted section when the contract is not sufficient |
| Metadata authoring workflow | `docs/METADATA_AUTHORING_WORKFLOW.md` | `read_file` entire file |
| Transformation escalation | `docs/TRANSFORMATION_DECISION_TREE.md` | `read_file` entire file |
| Custom function selection | `docs/CUSTOM_FUNCTION_SELECTION.md` | `read_file` entire file |
| Ingestion pattern selection | `docs/INGESTION_PATTERNS_REFERENCE.md` | `read_file` entire file |
| Medallion architecture | `docs/Medallion_Architecture_Best_Practices.md` | `read_file` entire file |
| Complex transformation patterns | `docs/Complex_Transformation_Patterns.md` | `read_file` entire file |
| Legacy conversion mapping | `docs/LEGACY_CODE_CONVERSION_GUIDE.md` | `read_file` entire file |
| SQL file format | `docs/METADATA_SQL_FILE_FORMAT.md` | grep `## SQL File Format and Structure` then read section |
| Error prevention | `docs/METADATA_SQL_FILE_FORMAT.md` | grep `## Error Prevention` then read section |
| End-to-end examples | `docs/METADATA_GENERATION_GUIDE.md` | grep `## Complete End-to-End Examples` then read section |

## Transformation Hierarchy

See [TRANSFORMATION_DECISION_TREE.md](../../docs/TRANSFORMATION_DECISION_TREE.md) for the canonical 3-tier escalation rules (Tier 1 built-in → Tier 2 custom notebooks → Tier 3 standalone).

## Built-In Transformations

All use `docs/TRANSFORMATION_PATTERNS_REFERENCE.md`.

| Transformation | Search pattern |
|----------------|----------------|
| `columns_to_rename` | `##### 1. columns_to_rename` |
| `filter_data` | `##### 3. filter_data` |
| `drop_duplicates` | `##### 4. drop_duplicates` |
| `change_data_types` | `##### 5. change_data_types` |
| `join_data` | `##### 6. join_data` |
| `aggregate_data` | `##### 7. aggregate_data` |
| `create_surrogate_key` | `##### 8. create_surrogate_key` |
| `attach_dimension_surrogate_key` | `##### 9. attach_dimension_surrogate_key` |
| `derived_column` | `##### 11. derived_column` |
| `add_row_hash` | `##### 12. add_row_hash` |
| `remove_columns` | `##### 13. remove_columns` |
| `select_columns` | `##### 14. select_columns` |
| `apply_null_handling` | `##### 15. apply_null_handling` |
| `replace_values` | `##### 16. replace_values` |
| `mask_sensitive_data` | `##### 17. mask_sensitive_data` |
| `pivot_data` | `##### 18. pivot_data` |
| `unpivot_data` | `##### 19. unpivot_data` |
| `add_window_function` | `##### 20. add_window_function` |
| `transform_datetime` | `##### 21. transform_datetime` |
| `union_data` | `##### 22. union_data` |
| `entity_resolution` | `##### 23. entity_resolution` |
| `sort_data` | `##### 27. sort_data` |
| `explode_array` | `##### 28. explode_array` |
| `flatten_struct` | `##### 29. flatten_struct` |
| `split_column` | `##### 30. split_column` |
| `concat_columns` | `##### 31. concat_columns` |
| `conditional_column` | `##### 32. conditional_column` |
| `string_functions` | `##### 33. string_functions` |
| `normalize_text` | `##### 34. normalize_text` |

## Tier 2 Custom Code

See [CUSTOM_FUNCTION_SELECTION.md](../../docs/CUSTOM_FUNCTION_SELECTION.md) for the canonical decision tree, function signatures, and comparison table for all five custom function types.

## Other Configuration Lookups

For ingestion pattern routing, see [INGESTION_PATTERNS_REFERENCE.md](../../docs/INGESTION_PATTERNS_REFERENCE.md).

All lookups below use `docs/METADATA_GENERATION_GUIDE.md` unless noted.

| Topic | Search pattern |
|-------|----------------|
| Write or destination | ``#### E. `target_details` (How to Write Data)`` |
| File output | ``#### E2. `target_details` (Writing to Files Instead of Delta Tables)`` |
| Incremental watermark | ``#### D. `watermark_details` (Incremental Loading)`` |
| Column cleansing | ``#### G. `column_cleansing` (Column Name Standardization)`` |
| Data quality checks | ``#### A. `data_quality` Checks`` |
| SCD Type 2 | `### Pattern 5: Create Dimension Table with SCD Type 2` |
| Fact and surrogate keys | `### Pattern 4: Silver` |
| Dimension lookups | `##### 9. attach_dimension_surrogate_key` |
| Change data feed | `### Pattern 6: Delta Tables with Change Data Feed` |
| Cross-workspace writes | `docs/FAQ.md` -> `### How do I write data to a different workspace?` |