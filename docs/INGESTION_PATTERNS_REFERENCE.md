# Ingestion Patterns Reference

> Last updated: 2026-03-29

This file is the canonical pattern map for choosing an ingestion approach.

Use this file when the question is: which ingestion pattern should be used for this source or movement?

## Pattern Map

| Pattern | Use When | Primary Decision Reference |
|---|---|---|
| External database via pipeline staging | Source is Azure SQL, SQL Server, Oracle, PostgreSQL, MySQL, DB2 | [METADATA_REFERENCE.md](METADATA_REFERENCE.md) for valid attributes and [DATA_INGESTION_GUIDE.md](DATA_INGESTION_GUIDE.md#ingest-data-from-an-external-database-eg-azure-sql-oracle-postgresql-into-a-delta-table) for stage-bypass guidance |
| REST API or SFTP via pipeline staging | Source is supported by PL_02 routing | [the source-type extension guide](FAQ.md#how-do-i-add-a-new-ingestion-source-type) and [METADATA_REFERENCE.md](METADATA_REFERENCE.md) |
| Files to Delta | Source is CSV, JSON, Excel, XML, Parquet, or similar | [DATA_INGESTION_GUIDE.md](DATA_INGESTION_GUIDE.md#ingest-file-data-from-the-catalog-files-section-into-a-delta-table) and [METADATA_REFERENCE.md](METADATA_REFERENCE.md) |
| Delta-to-Delta or internal table read | Source is an existing Fabric table | [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) when custom logic is needed |
| Custom source function | External source returns rows directly as a DataFrame | [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) |
| Custom staging function | External source must land files with function-owned file I/O | [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) |
| Custom file ingestion function | Framework has files, but parsing must be custom | [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) |
| Custom table ingestion function | Internal Fabric table read needs custom logic | [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) |

## Selection Rules

1. Use built-in ingestion patterns first.
2. Use custom source-side functions only when the built-in pattern cannot express the source or parsing behavior.
3. Use [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) for source-side custom function choice.
4. Use [TRANSFORMATION_DECISION_TREE.md](TRANSFORMATION_DECISION_TREE.md) when the real question is transformation escalation rather than ingestion pattern choice.

## Related References

- [METADATA_REFERENCE.md](METADATA_REFERENCE.md)
- [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md)
- [DATA_INGESTION_GUIDE.md](DATA_INGESTION_GUIDE.md) (ingestion walkthroughs and scenarios)
- [FAQ.md](FAQ.md#how-do-i-add-a-new-ingestion-source-type)
