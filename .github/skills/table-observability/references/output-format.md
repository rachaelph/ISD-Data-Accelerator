# Output Format

Use this reference when composing the response after query execution.

## Core Rules

- Minimal tier: return the result directly with a 1-2 sentence interpretation.
- Standard tier: summarize after each query and add findings and recommendations.
- Full tier: build the dashboard incrementally and present a consolidated view at the end.
- For query-backed answers in chat, show the key result rows in a compact Markdown table before the prose interpretation.
- For single-run investigations, the first table should include the selected run context: `Trigger_Name`, `Table_ID`, `Ingestion_Status`, `Processing_Phase`, `Records_Processed`, `Quarantined_Records`, and `Ingestion_End_Time`.
- For DQ investigations, add a second Markdown table with one row per DQ notification including `Data_Quality_Category`, `Data_Quality_Message`, `Data_Quality_Result`, `Rows_Impacted`, `Data_Quarantined`, and `Rows_Quarantined`.
- When the answer includes more than one remediation path, present the resolution options in a compact Markdown table instead of prose-only lists.
- When there is a single clear fix but the answer also needs to show where the change belongs and why, prefer a one-row Markdown table over a dense paragraph.
- If overlapping DQ rules make the per-rule quarantine counts exceed the unique quarantined row count in the run log, call that out explicitly immediately below the table.
- Keep SQL queries ready to re-run with concrete values filled in.

## Status Dashboard

```text
Table: {Table_Name} (Table_ID: {Table_ID})
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Last Run:     {status} ({timestamp}) | {rows} rows | {quarantined} quarantined
DQ Alerts:    {warnings} warnings, {failures} failures in last 7 days
Schema:       {change_summary}
Trend:        Avg {rows_per_day} rows/day
Lineage:      {upstream_count} upstream -> {downstream_count} downstream
Config:       Source: {source} | Merge: {merge_type} | Active: {yes_no}
```

## Markdown Table Templates

Use these as the default chat presentation for metadata-query results.

### Single Run Summary

| Trigger | Table_ID | Status | Phase | Rows Processed | Quarantined | Completed UTC |
|---------|----------|--------|-------|----------------|-------------|---------------|
| {Trigger_Name} | {Table_ID} | {Ingestion_Status} | {Processing_Phase} | {Records_Processed} | {Quarantined_Records} | {Ingestion_End_Time} |

### DQ Rule Breakdown

| Category | Message | Result | Rows Impacted | Quarantined? | Rows Quarantined |
|----------|---------|--------|---------------|--------------|------------------|
| {Data_Quality_Category} | {Data_Quality_Message} | {Data_Quality_Result} | {Rows_Impacted} | {Data_Quarantined} | {Rows_Quarantined} |

### Resolution Options

| Option | Where to change | Why it works | Recommended |
|--------|-----------------|--------------|-------------|
| {Fix option} | {Metadata / source / target / orchestration} | {Short rationale} | {Yes / No / Conditional} |

## Lineage Output

- Prefer a Mermaid graph for lineage investigations.
- Show only direct upstreams, the target table, and direct downstreams unless the user asks for a broader graph.
- If both lineage directions return zero rows, render a target-only graph and tell the user to run `NB_Generate_Lineage` to generate lineage, then re-run the investigation.
- Follow the prompt's contrast and styling rules.

## Live Data Summary

```text
Table: {Table_Name} (Table_ID: {Table_ID})
Datastore: {Datastore_Name} ({Datastore_Type}) | Environment: {ENV}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Rows: {row_count} | Columns: {column_count} | Last Modified: {last_modified}
Source: {EDA_or_live}
```

## SQL Query Section

- Minimal tier: inline SQL is enough.
- Standard and Full tiers: use fenced SQL blocks.

## First Query After Bootstrap

If the first query in a conversation is combined with bootstrap output, terminal capture can be unreliable for `Format-Table` even when the SQL succeeded. For small result sets, prefer:

```powershell
Write-Host "Rows returned: $($results.Count)"
foreach ($row in $results) {
	Write-Host ($row | Out-String).Trim()
}
```

For later queries in the same session, `Format-Table -AutoSize | Out-String -Width 300` is still the default.

## Large Text Fields

If the query returns JSON, XML, or another long text column, do not rely on `Format-Table` or `Format-List`. Output the fields individually with `Write-Host`.

## Recommendations

- Use `/fdp-06-troubleshoot` for deep failure analysis.
- Use `/fdp-03-author` for DQ-rule changes, metadata changes, or custom Python/PySpark function updates.
- Use `/fdp-05-run` to re-run after investigation.
- Use `/fdp-04-commit` to deploy metadata or code changes.
