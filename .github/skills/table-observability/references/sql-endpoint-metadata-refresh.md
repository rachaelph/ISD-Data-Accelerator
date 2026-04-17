# SQL Endpoint Metadata Refresh

Load this reference only when a live SQL endpoint result still disagrees with metadata evidence such as `SchemaDetails`, and only when the user is specifically troubleshooting stale or incorrect SQL endpoint output.

**Never run this by default. Never set `recreateTables` to `true`.**

## When to Use

Use this reference only after all of the following are true:

1. The user says SQL endpoint output looks stale, a column should exist but does not appear, or live data looks wrong.
2. You compared the SQL-visible schema or data with metadata evidence such as `invoke_metadata_query.py --query-type SchemaDetails`.
3. The mismatch still points to stale SQL endpoint metadata rather than unsupported Spark-only columns or a real data issue.

## REST Shape

```http
POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/sqlEndpoints/{sqlEndpointId}/refreshMetadata
Authorization: Bearer {token}
Content-Type: application/json

{
  "recreateTables": false,
  "timeout": {
    "timeUnit": "Minutes",
    "value": 15
  }
}
```

## Safe Usage Notes

- This refreshes metadata for all tables in the SQL analytics endpoint.
- Required permission is contributor or higher on the workspace.
- The API can return `200 OK` when the refresh completes inline or `202 Accepted` for a long-running operation.
- If you mention the request body in prose, state explicitly that `recreateTables` remains `false`.
- Do not present this as the first fix for missing columns. First verify the mismatch with `invoke_target_query.py --query-type Schema` and `invoke_metadata_query.py --query-type SchemaDetails`.