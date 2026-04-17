---
agent: agent
description: "Author pipeline behavior — create or edit metadata SQL, add entities, update transforms and DQ rules, and implement custom Python/PySpark function notebooks when built-ins are not enough"
---

# Author Pipelines and Custom Functions

**{{ task_description }}**

## Execution Order

- Run `python automation_scripts/agents/preflight.py --python-only` once per session before generating or validating metadata. This verifies the active interpreter is Python 3.10+ (required by the validator and other agent scripts). Skip when already ready in this session.
- After the route is clear, load only the minimum authoring guidance needed for the selected path.
- Metadata validation remains mandatory for any new or edited metadata SQL file.
- Any newly generated or explicitly requested custom function notebook review must go through the inline custom notebook review contract in this prompt before delivery.

## `vscode_askQuestions`-First Router

- Question: `What are you trying to author?`
- Options:
    - `Create new metadata for a source or pipeline`
    - `Edit existing metadata SQL`
    - `Add a custom notebook, transformation, or DQ rule`
- Follow-up dimensions if still broad: create vs edit mode, target medallion layer, metadata SQL vs notebook asset, new custom notebook vs existing notebook review.

## Quick Paths

- New source, new transformation flow, or new custom function request: use **Generate Mode**
- Existing metadata file or targeted change request: use **Edit Mode**
- Existing custom notebook optimization or pattern-compliance request: use **Custom Notebook Review Mode**
- Validation step for metadata edits: prefer `.github/skills/metadata-validation/validate_metadata_sql.py`

## Determine Mode

Infer the mode from the user's message:
- User attached or referenced an **existing metadata file** → **EDIT MODE**
- User describes a **new data source** to onboard → **GENERATE MODE**
- User attached or referenced an **existing NB_*.Notebook/notebook-content.py** asset or asked to improve custom function code → **CUSTOM NOTEBOOK REVIEW MODE**
- **Ambiguous?** → Ask: *"Are you creating new metadata or editing an existing file?"*

---

## GENERATE MODE

Use this path when creating metadata from scratch for a new data source, or when you need a custom Python or PySpark notebook because built-in transformations are not sufficient.

1. Load only the needed guidance — **batch all reads in parallel**.
    - Read `docs/Medallion_Architecture_Best_Practices.md`, search `docs/METADATA_GENERATION_GUIDE.md` for needed sections, and read `../common-patterns/metadata-authoring-reference.md` in one parallel batch.
    - Read `docs/Complex_Transformation_Patterns.md` only when the source logic is genuinely complex (can go in the same batch if already known).
2. Create or locate the metadata asset path.
    - Find the target metadata folder.
    - Create `metadata_<Trigger>.Notebook/notebook-content.sql` and `.platform` when needed.
    - If the source logic cannot be expressed with built-ins, create the referenced `NB_*.Notebook/notebook-content.py` asset too.
3. Generate the SQL payload.
    - Use `Trigger_Name` subqueries in DELETE statements instead of hardcoded IDs.
    - Add orchestration, primary configuration, and advanced configuration rows as needed.
    - End each INSERT block with a semicolon on the final row.
4. Optimize any generated custom notebook.
    - If this generate flow created `NB_*.Notebook/notebook-content.py`, immediately run the custom notebook review contract in this prompt in the same turn.
    - Treat the optimization pass as mandatory, not optional feedback.
    - Do not deliver freshly generated custom function code without applying must-fix performance, correctness, and pattern-compliance improvements.
5. Perform a mandatory self-review.
    - Re-read the source logic.
    - List the source patterns that must be covered.
    - If metadata alone does not cover the source behavior, add the required custom notebook before validation.
6. Continue to VALIDATE below.

---

## EDIT MODE

Use this path when modifying an existing metadata SQL file.

1. Understand the edit scope.
    - Read the entire existing metadata SQL file.
    - Identify the existing `Table_ID` values, edit type, and any related entities.
    - If the change affects Silver or Gold design, load `docs/Medallion_Architecture_Best_Practices.md`.
2. Load only the relevant guidance.
    - Use `../common-patterns/metadata-authoring-reference.md` for transformations.
    - Grep the generation guide only for the exact DQ, SCD2, source, or target pattern you need.
    - Skip doc loading for simple, well-known config edits.
3. Plan the edit conservatively.
    - Preserve existing `Table_ID` values and instance-number sequences unless removal is explicitly requested.
    - Match the existing formatting and naming style.
    - Continue the next sequence only for new entities, transforms, or DQ rules.
4. Make surgical edits.
    - Add entities across orchestration, primary configuration, and advanced configuration as needed.
    - Add transformations or DQ rules in advanced configuration, using one `validate_condition` per instance.
    - Update merge, source, target, or watermark settings only where requested.
    - Remove a table only by removing all rows for that `Table_ID` from all three metadata tables.
    - Keep DELETE statements keyed by `Trigger_Name` subquery rather than hardcoded IDs.
5. Continue to VALIDATE below.

---

## CUSTOM NOTEBOOK REVIEW MODE

Use this path when the user wants to improve or review an existing custom Python or PySpark notebook, or when Generate Mode created one.

1. Identify the target.
    - Resolve the notebook from the user's file, selection, or notebook name.
    - Determine the custom function type from the function signature.
2. Load minimum context.
    - Read the target code.
    - Load the matching template from `docs/resources/Custom_*_Function.ipynb` in parallel when the function type is known.
    - Search `docs/CUSTOM_FUNCTION_SELECTION.md` only when the function type is ambiguous.
    - Search `docs/FAQ.md` only when pattern-compliance guidance is needed.
3. Review against the contract.
    - Check signature, return type, metadata keys, and function type first.
    - Check accelerator rules: imports inside the function body, no wildcard imports, no top-level executable state, use `metadata.get('function_config', {})`, and use shared helpers instead of reimplementing them.
    - Check Spark execution patterns: avoid unnecessary actions, avoid row-by-row logic, prefer native column expressions, filter early, and only use repartitioning, caching, broadcast hints, or windows when justified.
    - Check correctness: null handling, schema stability, deterministic behavior, watermark semantics, and downstream column compatibility.
4. Apply fixes directly.
    - Fix must-fix issues before advisory improvements.
    - Preserve the function signature, return contract, and business behavior unless it is provably incorrect.
    - Keep edits surgical and avoid reformatting unrelated code.
    - If a fix intentionally changes observable behavior, flag it for user confirmation.
5. Validate the result.
    - Confirm imports remain function-local.
    - Confirm no new top-level executable code was introduced.
    - Confirm the return contract still matches the detected function type.
6. Deliver.
    - If code changes were requested or this review came from Generate Mode, edit the code before delivering.
    - If reviewing existing code, present must-fix findings before any summary.
    - If no material improvements are found, say so explicitly and call out any residual risks or test gaps.

## Preservation Rules

- Never reformat existing SQL unless the user asked for it.
- Never renumber existing `Table_ID` values or instance numbers.
- Never remove entities, transforms, or DQ checks unless explicitly requested.
- Never change existing configuration values unless that is the requested edit.
- If the user says to add something, add it without restructuring unrelated metadata.

---

## VALIDATE (Both Modes)

- Run `python .github/skills/metadata-validation/validate_metadata_sql.py <file>`.
- Fix validation failures and re-run until all checks pass.
- Deliver the result only after validation succeeds.
