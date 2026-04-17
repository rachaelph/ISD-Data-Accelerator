---
applyTo: "**/*.Warehouse/**"
---

# Warehouse Instructions

> Auto-applied when working on warehouse stored procedures and table definitions.

---

## How Metadata Flows Through the System

```
┌─────────────────────────────────────────────────────────────┐
│  AUTHORING                                                  │
│  User creates: <git_folder>/metadata/metadata_*.Notebook/            │
│  Contains: INSERT INTO dbo.Data_Pipeline_Metadata_*         │
│  Validated by: validate_metadata_sql.py                     │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  DEPLOYMENT                                                 │
│  CI/CD or manual execution runs the notebook SQL            │
│  → Populates Data_Pipeline_Metadata_* tables in warehouse   │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  RUNTIME                                                    │
│  Pipelines call Stored Procedures                           │
│  → SPs query metadata tables → Return config to notebooks   │
└─────────────────────────────────────────────────────────────┘
```

> 💡 **Key insight:** If metadata is wrong at runtime, check the source `metadata_*.Notebook` files first, then verify deployment succeeded.

---

## Stored Procedures

| Stored Procedure | Purpose |
|------------------|---------|
| `Get_Watermark_Value` | Retrieves watermark for incremental loading |
| `Get_Advanced_Metadata` | Gets advanced configuration for a Table_ID |
| `Get_Last_Run_Status` | Gets last run status for monitoring |
| `Get_Schema_Details` | Retrieves schema information |
| `Log_Data_Movement` | Logs pipeline execution details |
| `Log_New_Schema` | Logs schema changes |
| `Pivot_Primary_Config` | Pivots primary configuration for processing |
| `Create_Date_Dimension` | Creates/populates date dimension table |
| `FIFO_Status` | Gets FIFO queue status |
| `Get_Exploratory_Analysis_Input` | Gets input for EDA |
| `Get_Datastore_Details` | Retrieves datastore configuration for connections |
| `Get_Pipeline_Tables` | Gets table list for a pipeline trigger |

---

## Metadata Tables

| Table | Purpose |
|-------|---------|
| `Data_Pipeline_Metadata_Orchestration` | Main orchestration config (Trigger_Name, Table_ID, Target_Entity, etc.) |
| `Data_Pipeline_Metadata_Primary_Configuration` | Primary config attributes (source_details, target_details, etc.) |
| `Data_Pipeline_Metadata_Advanced_Configuration` | Advanced config (transformations, DQ rules, etc.) |
| `Data_Pipeline_Logs` | Execution logs |
| `Data_Pipeline_Lineage` | Data lineage tracking |
| `Schema_Changes` / `Schema_Logs` | Schema change tracking |
| `Date_Dimension` | Date dimension for reporting |
| `Data_Quality_Notifications` | DQ alert history |
| `Datastore_Configuration` | Environment-specific connection details |
| `Exploratory_Data_Analysis_Results` | EDA profiling results |

---

## Key Relationships (Impact Matrix)

| If You Change... | You May Also Need to Change... |
|------------------|-------------------------------|
| **Metadata Table schema** | Stored procedures that read/write it, notebooks that consume the data |
| **Stored Procedure output** | Pipelines that call it, notebooks that use the results |
| **Config attributes** | Metadata reference docs, validation script, all consuming code |

---

## Documentation Lookup

Grep across `docs/` for the user's keywords, then read only the matched section.
