# SQL Warehouse Metadata Refresh

Load this reference only when a live Databricks SQL Warehouse result still disagrees with metadata evidence such as `SchemaDetails`, and only when the user is specifically troubleshooting stale or incorrect SQL Warehouse output.

**Never run this by default.**

## When to Use

Use this reference only after all of the following are true:

1. The user says SQL Warehouse output looks stale, a column should exist but does not appear, or live data looks wrong.
2. You compared the SQL-visible schema or data with metadata evidence such as `invoke_metadata_query.py --query-type SchemaDetails`.
3. The mismatch still points to a stale Delta cache or stale catalog metadata rather than unsupported Spark-only columns or a real data issue.

## How to Refresh on Databricks

Databricks SQL Warehouses do not expose a separate `refreshMetadata` REST API. Stale-table situations are addressed by invalidating the catalog or Delta cache through SQL.

```sql
-- Force a re-read of the latest Delta log for a single table
REFRESH TABLE <catalog>.<schema>.<table>;

-- Clear the SQL Warehouse's local Delta cache (per-warehouse, optional)
CLEAR CACHE;
```

Both statements are safe and read-only with respect to data — they only invalidate cached metadata or data on the warehouse.

## Safe Usage Notes

- Prefer `REFRESH TABLE` scoped to the specific table the user is investigating; it is the smallest-impact action.
- `CLEAR CACHE` affects every running query on that warehouse — call it out before suggesting it.
- Do not present a metadata refresh as the first fix for missing columns. First verify the mismatch with `invoke_target_query.py --query-type Schema` and `invoke_metadata_query.py --query-type SchemaDetails`.
- If `REFRESH TABLE` does not resolve the mismatch, the issue is almost always upstream (table not yet written, wrong catalog or schema resolution, or a Spark-only data type that the JDBC client elides).
