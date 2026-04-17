# Restarting Pipelines From Failure

> Last updated: 2026-03-04

How to restart a trigger pipeline after a mid-run failure with the smallest amount of re-work.

---

## Executive Summary

### What happens when a trigger fails mid-run?

If a step fails, all remaining steps are **skipped** automatically. Within the failed step, tables that already succeeded have their watermarks updated — tables that failed keep their old watermark, so they'll re-process the correct data on retry.

### What is the recommended restart approach?

Use the built-in `start_with_step` and `table_ids` parameters on `Trigger Step Orchestrator`. These are **runtime parameters** — they skip completed work and target only the failures without touching any metadata.

| Parameter | Default | Purpose |
|---|---|---|
| `start_with_step` | `0` (all steps) | Skip completed steps — jump to the failed step |
| `table_ids` | `0` (all tables) | Filter to only the Table_IDs that failed |

### How do I find out what failed?

Query the pipeline logs:

```sql
SELECT  Table_ID, Trigger_Step, Ingestion_Status
FROM    dbo.Data_Pipeline_Logs
WHERE   Trigger_Name = 'YourTrigger'
AND     Trigger_Execution_Start_Time = (
            SELECT MAX(Trigger_Execution_Start_Time)
            FROM   dbo.Data_Pipeline_Logs
            WHERE  Trigger_Name = 'YourTrigger'
        )
ORDER BY Trigger_Step, Table_ID
```

### What does a restart look like in practice?

If step 2 of 5 failed with Table_IDs 30 and 40 failing:

```
-- Call 1: Re-run only the failed tables in the failed step
Trigger Step Orchestrator
  trigger_name    = "YourTrigger"
  start_with_step = "2"
  table_ids       = "30,40"

-- Call 2: Resume remaining steps for all tables
Trigger Step Orchestrator
  trigger_name    = "YourTrigger"
  start_with_step = "3"
  table_ids       = "0"
```

Or if you'd rather not identify individual failures — just re-run from the failed step:

```
Trigger Step Orchestrator
  trigger_name    = "YourTrigger"
  start_with_step = "2"
  table_ids       = "0"
```

Already-succeeded tables will re-process safely (idempotent merges, watermarks already advanced).

### Is it safe to re-process tables that already succeeded?

Yes. Merge/upsert writes are idempotent. Overwrite writes replace the target. Watermarks for succeeded tables already advanced, so incremental tables process an empty or minimal delta.

### Why not toggle Ingestion_Active to exclude succeeded tables?

That approach requires changing **metadata records** — persistent state managed through CI/CD. It creates drift between source control and production, risks forgetting to revert (silently disabling tables on the next run), and affects any other trigger that shares those Table_IDs. The `start_with_step` / `table_ids` parameters exist precisely so you never need to touch metadata for operational restarts.

| Concern | `start_with_step` + `table_ids` | Toggle `Ingestion_Active` |
|---|---|---|
| **What changes** | Pipeline parameters (runtime) | Metadata records (persistent state) |
| **Metadata re-promotion needed** | No | Yes |
| **Risk of forgetting to revert** | None | High — tables silently stop processing |
| **CI/CD drift** | None | Direct production edit conflicts with deployed metadata |
| **Cross-trigger impact** | None | Other triggers sharing those Table_IDs also lose them |

### When does toggling Ingestion_Active make sense?

For **planned configuration changes** — permanently decommissioning a table, pausing a table for days while the source team fixes an upstream issue, or phased rollouts. Not for operational restart scenarios.

### What if I get a FIFO lock error on restart?

This happens when a staging table's previous run was interrupted and left an orphaned `'Started'` log record. The error message includes the exact SQL query to find the orphaned Log_IDs — run it, verify the records are from the interrupted run, delete them, then retry.

---

### At a Glance

| Aspect | Answer |
|---|---|
| **What to use** | `start_with_step` and `table_ids` parameters on `Trigger Step Orchestrator` |
| **Why** | Skips completed work without touching metadata or CI/CD |
| **How to find failures** | Query `Data_Pipeline_Logs` for the last trigger execution |
| **Re-processing safe?** | Yes — idempotent merges, watermarks protect incremental loads |
| **Ingestion_Active toggle?** | Avoid — creates CI/CD drift, risk of silent table deactivation |
| **FIFO lock on restart?** | Delete orphaned `'Started'` log records from interrupted runs |
| **Preventive design** | Granular triggers reduce blast radius of failures |

---
---

# Technical Deep-Dive (Optional)

> The sections below provide detailed diagrams, failure propagation mechanics, strategy comparisons, and worked examples. **The Executive Summary above covers all key decisions and takeaways — the following is supplementary reference material.**

---

## How Failure Propagates in PL_01a

```
Trigger Step Orchestrator
│
├─► Get_Pipeline_Tables (stored procedure)
│     Returns rows grouped by Order_Of_Operations
│
└─► For Each Step Within Trigger (sequential loop)
      │
      ├─► Step 1: PL_01b runs all entities in parallel (batch of 50)  ✅ Succeeded
      ├─► Step 2: PL_01b runs all entities in parallel                ❌ Some failed
      │     │
      │     ├─► Table 10  ✅ Processed (watermark updated, logs written)
      │     ├─► Table 20  ✅ Processed
      │     ├─► Table 30  ❌ Failed (watermark NOT updated, failure logged)
      │     └─► Table 40  ❌ Failed
      │
      ├─► variable 'an_activity_failed' = 'Yes'
      │
      ├─► Step 3: SKIPPED (failure flag is checked before execution)
      ├─► Step 4: SKIPPED
      └─► Step 5: SKIPPED
```

- Steps are processed **sequentially**. Within each step, entities run in **parallel**.
- When any entity fails, all subsequent steps are **skipped**.
- Succeeded tables have their watermarks updated; failed tables keep their old watermark.

---

## Restart Strategies in Detail

### Strategy A — Surgical Restart (Minimum Re-Work)

**When to use:** You know exactly which Table_IDs failed, and remaining steps also need to run.

Requires two pipeline calls because `table_ids` filters globally across all steps:

**Call 1 — Failed Table_IDs only:**

| Parameter | Value |
|---|---|
| `start_with_step` | `"2"` (failed step) |
| `table_ids` | `"30,40"` (only failures) |

**Call 2 — Resume remaining steps:**

| Parameter | Value |
|---|---|
| `start_with_step` | `"3"` (next step) |
| `table_ids` | `"0"` (all tables) |

### Strategy B — Restart From Failed Step

**When to use:** You don't need to identify individual failures, or re-processing succeeded tables is acceptable.

| Parameter | Value |
|---|---|
| `start_with_step` | `"2"` (failed step) |
| `table_ids` | `"0"` (all tables) |

### Strategy C — Full Re-Run

**When to use:** Failure was in step 1, or you want a clean slate.

| Parameter | Value |
|---|---|
| `start_with_step` | `"0"` |
| `table_ids` | `"0"` |

---

## Decision Tree

```
Failure in the middle of a trigger
│
├─► Know which Table_IDs failed?
│   ├─► YES + remaining steps needed → Strategy A (two calls)
│   ├─► YES + no remaining steps     → Strategy A (single call)
│   └─► NO                           → Strategy B
│
├─► Re-processing succeeded tables OK?
│   ├─► YES → Strategy B (simpler)
│   └─► NO  → Strategy A (surgical)
│
└─► Failure was in step 1?
    └─► Strategy C (nothing to skip)
```

---

## FIFO Lock Cleanup

If a staging table's pipeline was interrupted, the `'Started'` log record may still exist, blocking the next run.

The error message provides the exact query. After verifying the records are from the interrupted run:

```sql
-- Find orphaned Log_IDs
SELECT  [Log_ID]
FROM    dbo.Data_Pipeline_Logs
WHERE   [Table_ID] = 30
AND     [Log_ID] IN (
            SELECT      [Log_ID]
            FROM        dbo.Data_Pipeline_Logs
            WHERE       [Table_ID] = 30
            AND         [Processing_Phase] = 'Staging'
            GROUP BY    [Log_ID]
            HAVING      MIN([Ingestion_Status]) = 'Started'
        )
AND     [Ingestion_Start_Time] > DATEADD(hh, -24, GETUTCDATE())

-- Delete after confirming they're from the interrupted run
DELETE FROM dbo.Data_Pipeline_Logs
WHERE  [Log_ID] IN ('<log_id_1>', '<log_id_2>')
```

---

## Worked Example

**Trigger:** `Daily_Revenue` with 3 steps

| Step | Tables | Status After Failure |
|---|---|---|
| 1 (Bronze) | Table_IDs: 1, 2, 3 | ✅ All succeeded |
| 2 (Silver) | Table_IDs: 10, 11, 12 | ⚠️ 10 succeeded, 11 and 12 failed |
| 3 (Gold) | Table_IDs: 20, 21 | ⏭️ Skipped |

**Surgical restart:**

```
-- Call 1: Fix the failed silver tables
trigger_name = "Daily_Revenue", start_with_step = "2", table_ids = "11,12"

-- Call 2: Run the gold step
trigger_name = "Daily_Revenue", start_with_step = "3", table_ids = "0"
```

**Simple restart:**

```
-- Re-run everything from step 2 onward
trigger_name = "Daily_Revenue", start_with_step = "2", table_ids = "0"
```

---

> 📖 **Preventive Design:** Granular trigger design can minimize restart scope. See [Trigger Design and Dependency Management — Preventive Design](Trigger_Design_and_Dependency_Management.md#preventive-design-for-restarts) for patterns that reduce restart impact.

## Quick Reference

| I Want To... | Parameters |
|---|---|
| Re-run entire trigger | `start_with_step = "0"`, `table_ids = "0"` |
| Skip completed steps | `start_with_step = "<failed_step>"`, `table_ids = "0"` |
| Re-run specific failed tables only | `start_with_step = "<failed_step>"`, `table_ids = "<id1,id2>"` |
| Resume remaining steps after fixing failures | `start_with_step = "<next_step>"`, `table_ids = "0"` |
