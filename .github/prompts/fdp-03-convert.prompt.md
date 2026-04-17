---
agent: agent
description: "Convert SQL/Python/ETL code into accelerator metadata SQL using medallion architecture"
---

# Convert Code to Metadata SQL

Convert the following code into metadata SQL for the Databricks Data Engineering Accelerator:

{{ legacy_code }}

## Execution Order

- Run `python automation_scripts/agents/preflight.py --python-only` once per session before generating or validating metadata. This verifies the active interpreter is Python 3.10+ (required by the validator and verification tools). Skip when already ready in this session.
- After the route is clear, load only the minimum conversion guidance needed for the supplied source material.
- Preserve source behavior; conversion is decomposition and mapping, not a greenfield redesign.

## `vscode_askQuestions`-First Router

- Question: `What do you want to convert into accelerator metadata?`
- Options:
	- `SQL or stored procedure logic`
	- `Python, notebook, or ETL code`
	- `Review the conversion scope before generating metadata`
- Follow-up dimensions if still broad: source file location vs inline code, full vs targeted conversion, metadata SQL only vs metadata plus custom notebooks.

## Workflow

> **🔴 Conversion ≠ Generation!** When user provides existing code, you must DECOMPOSE it first.

1. Load all conversion guidance in parallel before generating anything.
	- **Parallel batch 1** (skills): Load `.github/skills/conversion-verification/SKILL.md` and `.github/skills/metadata-validation/SKILL.md`.
	- **Parallel batch 2** (docs, after skills): Read `docs/LEGACY_CODE_CONVERSION_GUIDE.md`, read `docs/Medallion_Architecture_Best_Practices.md`, and read `../common-patterns/metadata-authoring-reference.md` — all in one batch.
	- Read `docs/Complex_Transformation_Patterns.md` only when the source logic is genuinely complex (can join batch 2 if already known).
2. Decompose the legacy source before generating anything.
	- List source tables or files, operations, calculated columns, and intermediate constructs.
	- Identify loops, recursion, cursors, UDFs, or similar patterns that may require custom notebook assets.
	- Produce a numbered operation list so every source behavior is accounted for.
3. Assign the decomposed logic to medallion layers.
	- Bronze is raw extraction only.
	- Silver handles cleansing, validation, and basic joins.
	- Gold handles business logic, aggregations, and dimensional modeling.
	- Output an operation-to-layer mapping before generating metadata.
4. Map operations to accelerator behavior.
	- Use built-in transformations when they fit.
	- If no built-in exists, add `custom_transformation_function` or `custom_table_ingestion_function` assets instead of dropping behavior.
5. Generate metadata SQL and any required notebook assets.
	- Create layer-appropriate `Table_ID` values and instance ordering.
	- Follow standard metadata SQL structure.
6. Verify completeness with the conversion-verification skill.
	- Run `python .github/skills/conversion-verification/verify_conversion.py --source <source_files> --metadata <generated.sql>`.
	- Review FAIL and WARN findings against the skill's false-positive guidance.
	- Fix missing coverage and re-run until no undismissed FAIL remains.
7. Validate the final metadata SQL.
	- Run `python .github/skills/metadata-validation/validate_metadata_sql.py <file>`.
	- Fix all validation failures before delivery.

## Conversion Guardrails

- Preserve all source logic; do not optimize away behavior during conversion.
- Treat conversion as decomposition and mapping, not freeform redesign.
- Split single-pass legacy logic across Bronze, Silver, and Gold when needed instead of collapsing it into one layer.
- If a source pattern cannot be represented with built-ins, create the required custom asset rather than omitting coverage.
