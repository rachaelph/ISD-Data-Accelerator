# Trigger Design and Dependency Management

> Last updated: 2026-03-29

How to design triggers, control parallelism, and manage dependencies across bronze, silver, and gold layers in the Databricks Data Engineering Accelerator.

---

## Overview

A common question when adopting the accelerator is how it handles **parallelism between tables** and **dependencies between layers** (e.g., silver and gold). The core concern: if a single trigger contains many tables across silver and gold layers, gold processing must wait for _all_ silver tables to complete — even if some silver tables are unrelated to each other.

**The accelerator fully supports granular trigger design that eliminates unnecessary dependencies.** Triggers can be as broad or as narrow as needed — down to a single table's end-to-end silver→gold chain. Organizations using enterprise schedulers (IWS, Control-M, Autosys, Azure DevOps, etc.) can design triggers to match their existing job structure, so each job runs its own chain independently in parallel.

This document explains the three orchestration controls available, illustrates common trigger design patterns, and shows how external schedulers integrate with the accelerator.

---

## The Three Orchestration Controls

The accelerator provides three independent levers for controlling execution order and parallelism. Together they support any orchestration pattern — from a single trigger processing everything sequentially to hundreds of independent triggers running in parallel.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      ORCHESTRATION CONTROLS                             │
│                                                                         │
│   1. TRIGGER NAME          What group of tables to process              │
│   ──────────────           Each trigger runs independently              │
│                            Multiple triggers run in parallel             │
│                                                                         │
│   2. ORDER OF OPERATIONS   What sequence within a trigger               │
│   ──────────────────────   Same order = parallel execution              │
│                            Different order = sequential execution        │
│                            (1 completes before 2 starts)                │
│                                                                         │
│   3. WRAPPER PIPELINE      What dependencies between triggers           │
│   ──────────────────       Chain triggers: shared sources first,        │
│                            then data product triggers in parallel        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Control 1 — Trigger Name

A trigger is a named group of Table_IDs. When you run the orchestration pipeline, you pass a `Trigger_Name` parameter. The pipeline processes only the tables assigned to that trigger.

**Key behavior:** Triggers are completely independent. Multiple triggers can run simultaneously without interfering with each other. The accelerator's built-in FIFO queue prevents concurrent writes to the same Table_ID if two triggers happen to share one.

### Control 2 — Order of Operations

Within a single trigger, `Order_Of_Operations` controls sequencing:

| Order_Of_Operations Value | Behavior |
|---|---|
| Same value (e.g., all = 1) | **All tables run in parallel** |
| Different values (e.g., 1, 2, 3) | **Sequential** — all tables at step 1 must complete before step 2 begins |

This is the batch/step mechanism. Tables with the same order number form a concurrent batch. The next batch starts only after the previous batch finishes entirely.

### Control 3 — Wrapper Pipeline

For dependencies _between_ triggers, a lightweight wrapper pipeline calls `Trigger Step Orchestrator` multiple times with different trigger names. This allows patterns like "load shared sources first, then run data product triggers in parallel."

---

## Why Broad Triggers Can Create Unnecessary Waiting

Consider a common scenario: a single trigger with 20 silver tables and their corresponding gold tables:

```
Trigger: "DailyLoad"

Order 1 (parallel):  Silver_A  Silver_B  Silver_C  ... Silver_T  (20 tables)
                        │         │         │              │
                        ▼         ▼         ▼              ▼
                     (ALL must complete before Order 2 starts)
                        │
Order 2 (parallel):  Gold_A    Gold_B    Gold_C    ... Gold_T    (20 tables)
```

**The problem:** Silver_A might finish in 2 minutes, but Silver_T takes 45 minutes. Gold_A cannot start until Silver_T completes — even though Gold_A has no dependency on Silver_T whatsoever. Gold_A waits 43 minutes for no reason.

This design creates more dependencies than exist in the real world: every gold table is forced to wait for every silver table, even unrelated ones.

---

## The Solution: Granular Trigger Design

Instead of one broad trigger, break the work into **granular triggers that match the actual dependency chains**. Each trigger carries a table from source through to gold independently.

> **Platform limitation:** Databricks supports up to 20 schedule triggers per job. New distinct schedules require creating a new job.

```
Trigger: "Table_A_Load"                    Trigger: "Table_B_Load"
                                           
Order 1: Silver_A                          Order 1: Silver_B
Order 2: Gold_A                            Order 2: Gold_B
                                           
  (Gold_A starts immediately                 (Gold_B starts immediately
   when Silver_A finishes)                    when Silver_B finishes)
                                           
         ▲                                          ▲
         │                                          │
    Runs in parallel ──────────────────────── Runs in parallel
```

**Result:** Gold_A starts as soon as Silver_A completes. It does not wait for Silver_B, Silver_C, or any other unrelated table. This mirrors how most enterprise schedulers manage work: each job runs its own chain independently.

### Side-by-Side Comparison

| Design | Silver_A Duration | Silver_T Duration | Gold_A Starts At | Gold_A Wait Time |
|---|---|---|---|---|
| **Broad trigger** (1 trigger, all tables) | 2 min | 45 min | 45 min | 43 min unnecessary wait |
| **Granular triggers** (1 trigger per chain) | 2 min | 45 min | 2 min | 0 min unnecessary wait |

---

## Available Trigger Design Patterns

The accelerator does not prescribe a specific trigger structure. The right design depends on actual data dependencies, how your existing scheduler jobs are organized, and what tradeoffs you prefer between granularity and manageability. Below are three patterns that can be mixed and matched freely.

### Pattern 1 — Per-Table Triggers (Finest Granularity)

Works well when: each table has its own scheduler job today and there are no shared dependencies between silver tables feeding the same gold table.

Each table gets its own trigger containing two steps:

| Trigger Name | Step | Layer | Table | Runs When |
|---|---|---|---|---|
| OracleCAP_TableA | 1 | Silver | cap_table_a | Immediately |
| OracleCAP_TableA | 2 | Gold | cap_table_a_dim | After step 1 completes |
| OracleCAP_TableB | 1 | Silver | cap_table_b | Immediately |
| OracleCAP_TableB | 2 | Gold | cap_table_b_fact | After step 1 completes |

Because these are separate triggers, Table A and Table B run their entire silver→gold chains **independently and in parallel**.

### Pattern 2 — Data Product Triggers (Grouped by Business Domain)

Works well when: a gold table depends on multiple silver tables that must all complete first.

| Trigger Name | Step | Layer | Table | Runs When |
|---|---|---|---|---|
| Revenue_DataProduct | 1 | Silver | orders | Immediately (parallel) |
| Revenue_DataProduct | 1 | Silver | customers | Immediately (parallel) |
| Revenue_DataProduct | 1 | Silver | products | Immediately (parallel) |
| Revenue_DataProduct | 2 | Gold | revenue_fact | After all step 1 tables complete |

Here the wait is intentional — the gold table genuinely needs all three silver tables before it can run.

### Pattern 3 — Wrapper Pipeline (Shared Sources + Independent Data Products)

Works well when: multiple data products share common source tables, and each data product has independent gold processing.

```
PL_Daily_Orchestration (wrapper pipeline)
│
├─► PL_01a (Trigger = 'SharedSources_Bronze')     ← Shared ingestion runs first
│         │
│         ▼ (on success)
│   ┌─────┴──────────────┐
│   │                    │
│   ▼                    ▼
├─► PL_01a               PL_01a
│   (Trigger =           (Trigger =
│   'Revenue_Product')   'Fleet_Product')
│   [parallel]           [parallel]
```

This ensures shared bronze tables load once, then each data product processes its silver→gold chain independently and in parallel.

---

## External Scheduler Integration

The accelerator integrates with any enterprise scheduler (IWS, Control-M, Autosys, Azure DevOps, etc.) through the workspace REST API or Databricks CLI.

### How It Works

```
┌─────────────────────────────────────────────────────────────────────────┐
│   Enterprise Scheduler                                                  │
│                                                                         │
│   Job: Source_TableA_Load                                               │
│     └─► Databricks CLI: run pipeline                                        │
│           --workspace "<Your_Workspace>"                                │
│           --pipeline  "Trigger Step Orchestrator"                    │
│           --parameters '{"Trigger_Name": "Source_TableA"}'              │
│                                                                         │
│   Job: Source_TableB_Load  (parallel, no scheduler dependency)          │
│     └─► Databricks CLI: run pipeline                                        │
│           --workspace "<Your_Workspace>"                                │
│           --pipeline  "Trigger Step Orchestrator"                    │
│           --parameters '{"Trigger_Name": "Source_TableB"}'              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**Key points:**

| Aspect | How It Works |
|---|---|
| Pipeline name | Always `Trigger Step Orchestrator` — one generic pipeline for everything |
| Trigger name | Passed as a pipeline parameter — controls which tables run |
| Parallelism | Scheduler fires multiple jobs in parallel; each trigger runs independently |
| Dependencies between jobs | Managed in the scheduler — Job B depends on Job A |
| Full reload | Pass additional parameter `Full_Reload = True` to override incremental processing |

### How the Scheduler Calls the Pipeline

The external scheduler calls the workspace REST API (or Databricks CLI) with two pieces of information:

1. **Which pipeline to run** — always the same generic orchestration pipeline
2. **Which trigger name to pass** — controls which tables are processed

The API returns a job instance ID that the scheduler can poll for completion status before starting dependent jobs. This is the same pattern used to call any Databricks job — the only addition is the trigger name parameter.

---

## Migration Path: Existing Scheduler Jobs → Accelerator Triggers

The mapping from existing pipeline jobs to accelerator triggers is straightforward:

| Current State | Future State (Accelerator) |
|---|---|
| 1 scheduler job per pipeline per source table | 1 trigger per logical data chain |
| Pipeline contains hardcoded notebook references | Trigger references metadata-configured Table_IDs |
| Dependencies managed in scheduler job stream | Dependencies managed in scheduler **and/or** within trigger via Order_Of_Operations |
| Each pipeline is custom-built | One generic pipeline, behavior controlled by metadata |

**What changes for the scheduler administrator:**
- Pipeline name changes from individual pipeline names to a single `Trigger Step Orchestrator`
- A `Trigger_Name` parameter is added to each job
- Everything else — job scheduling, dependencies, monitoring — stays the same

---

## Frequently Asked Questions

| Question | Answer |
|---|---|
| "If I pass a trigger with 100 tables, is there no parallelism?" | **All tables with the same Order_Of_Operations run in parallel.** 100 tables at order 1 = 100 concurrent executions. |
| "Gold waits until ALL silver tables complete?" | **Only within the same trigger and only across order boundaries.** Use separate triggers to eliminate cross-table waiting. |
| "Silver A finishes — can Gold A run immediately without waiting for Silver B, C?" | **Yes — put Silver A and Gold A in their own trigger.** Gold A starts as soon as Silver A completes. |
| "How do I pass the trigger name from an external scheduler?" | **Pipeline parameter via workspace REST API or Databricks CLI.** Same mechanism as calling any Databricks job. |
| "Are we creating more dependencies than we have today?" | **Not necessarily.** The accelerator supports any level of granularity — triggers can be as broad or as narrow as needed. The team decides what grouping makes sense based on actual data dependencies. |
| "What if a trigger fails mid-run? Do I have to restart everything?" | **No.** Use `start_with_step` and `table_ids` parameters to restart from the failed step and target only failed Table_IDs. See [Restarting Pipelines From Failure](Restarting_Pipelines_From_Failure.md). |

---

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Too many granular triggers become hard to manage | Group related tables into data-product-level triggers where real dependencies exist. Use naming conventions (e.g., `OracleCAP_*`, `SFTP_*`) for organization. |
| Scheduler administrator unfamiliar with new parameter-passing pattern | Provide a runbook with exact CLI/API commands. Validate with a pilot trigger before full rollout. |
| Two triggers write to the same target table concurrently | Built-in FIFO queue prevents concurrent writes to the same Table_ID. Second trigger waits automatically. |
| Trigger proliferation leads to scheduling conflicts | Monitor cluster capacity utilization. Use scheduling windows to stagger high-volume triggers if capacity is constrained. |
| Order_Of_Operations misunderstood as table-level dependency | Document clearly: order of operations is a batch boundary, not a table-to-table dependency. For table-level dependencies, use separate triggers. |

---

## Suggested Next Steps

| Step | What Happens | Who Does It |
|---|---|---|
| **1. Review current scheduler job structure** | Walk through existing scheduler jobs and dependency chains to understand the current orchestration model | Data engineering team |
| **2. Decide on trigger granularity** | Determine which pattern (or combination) best fits each data domain based on actual dependencies and operational preferences | Data engineering team + platform team |
| **3. Pilot with a few triggers** | Configure and run a small set of triggers to validate the pattern end-to-end before scaling | Platform team |
| **4. Validate scheduler integration** | Execute a Databricks CLI call from the scheduler with a trigger name parameter to confirm the handoff works | Platform team + scheduler administrator |
| **5. Agree on naming conventions** | Establish trigger naming standards that make sense for the organization's structure | Data engineering team |
| **6. Review and adjust** | After the pilot, assess whether the trigger design is working as expected and refine as needed | Full team |

---

## Preventive Design for Restarts

Granular trigger design directly reduces restart scope when failures occur. The table below summarises the relationship between design choices and restart impact.

| Design Choice | Benefit for Restarts |
|---|---|
| **Per-table triggers** | Failure affects one table's chain only — restart is a simple full re-run |
| **Data product triggers** | Failures scoped to one domain — use `start_with_step` to skip completed layers |
| **Granular Order_Of_Operations** | More steps = more checkpoints to skip on restart |

For restart procedures and parameter usage, see [Restarting Pipelines From Failure](Restarting_Pipelines_From_Failure.md).
