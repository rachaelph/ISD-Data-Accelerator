---
applyTo: "**/metadata/**"
---

# Metadata SQL Instructions

> Auto-applied when working on metadata SQL files (`metadata/` folder under the workspace Git folder).

---

## What These Files Are

Metadata SQL files (`metadata/metadata_<Trigger>.sql`) define the data ingestion configuration for the accelerator. They populate three tables in the Databricks SQL warehouse:

| Table | Purpose |
|-------|---------|
| `Data_Pipeline_Metadata_Orchestration` | Trigger → Table_ID mapping, ordering, target entity |
| `Data_Pipeline_Metadata_Primary_Configuration` | Source, target, watermark, column cleansing config |
| `Data_Pipeline_Metadata_Advanced_Configuration` | Transformations, DQ rules, SCD2, custom code |

---

## Non-Negotiables

1. **SELF-REVIEW** — Re-read source, list all patterns, verify each is covered before delivering
2. **Custom Only When Needed** — Use built-in transformations for **all standard tasks** (renames, casting, filtering, joins, aggregations, column cleansing, etc.). Only use Tier 2 **for the specific logic that cannot be expressed with built-ins**. Everything else must remain Tier 1. Custom code adds maintenance burden and generates additional unit tests.
3. **RUN VALIDATOR** — See [Mandatory Metadata Validation](#mandatory-metadata-validation) below.

---

## 🚫 FORBIDDEN (Metadata-Specific)

| ❌ Never Do This | Why |
|------------------|-----|
| Omit semicolons from INSERT statements | Each INSERT must end with `;` on the last VALUES row |
| Remove existing entities unless asked | Preserve user's work |
| Put DQ checks in Bronze or skip Silver | Bronze = raw; DQ belongs in Silver or Gold |

---

## Mandatory Metadata Validation

> **🔴 EVERY metadata SQL file MUST be verified and validated before delivery. NO EXCEPTIONS.**
> This applies when **authoring** metadata (creating new or editing existing SQL) — i.e., `/fdp-03-author` and `/fdp-03-convert` workflows. It does **NOT** apply to `/fdp-04-commit`, which deploys already-validated files.

After creating or editing ANY metadata SQL file, you MUST:

1. **Output a sanity check** (visible in your response):
   ```
   📋 SANITY CHECK:
   Source entities: [what was requested/found in source]
   Created Table_IDs: [what you generated]
   Custom notebooks created: [list custom_functions/*.py files, or "None needed"]
   Coverage: ✅ Complete | ⚠️ Gaps: [list any]
   ```

   > ⚠️ **If Coverage shows gaps requiring custom code, STOP and implement the custom notebook BEFORE proceeding!**

2. **Run the validator**:
   ```powershell
   python .github/skills/metadata-validation/validate_metadata_sql.py <path_to_sql_file>
   ```

3. **Fix ALL errors** — Re-run until you see `✅ All validation checks passed!`

4. **Only then deliver** the file to the user

---

## Documentation Lookup

Use the shared metadata authoring reference in `../common-patterns/metadata-authoring-reference.md`.

Most relevant sections:
- Reference docs
- Transformation hierarchy
- Built-in transformations
- Tier 2 custom code
- Other configuration lookups

---

## Edit Guidance

| Edit Request | Action |
|--------------|--------|
| Add new table | Add Table_ID in Orchestration → Primary Config → (optionally) Advanced Config |
| Add DQ checks | Add rows to Advanced Configuration for that Table_ID |
| Change merge type | Update `target_details`/`merge_type` row |
| Remove Table_ID | Remove ALL rows referencing it from all tables |

When editing existing metadata:
- **PRESERVE** all existing Table_IDs unless asked to remove
- **PRESERVE** existing instance_number sequences
- **Match** existing formatting
- Adding entity? Continue Table_ID sequence
- Adding transform? Continue instance_number sequence
- Adding DQ? ONE condition per validate_condition instance

---

## Validator

```powershell
# Validate single file
python .github/skills/metadata-validation/validate_metadata_sql.py <file>

# Validate all metadata notebooks in the metadata folder
python .github/skills/metadata-validation/validate_metadata_sql.py
```

The validator automatically checks **40+ rules** including:
- All attribute names and values against the canonical metadata reference
- Category placement (Primary vs Advanced)
- Instance numbering and sequencing
- Required attributes per transformation type
- Cross-config consistency (SCD2, merge types, etc.)
- SQL syntax issues (semicolons, multi-line VALUES, etc.)

**Fix ALL errors before delivery. No exceptions.**
