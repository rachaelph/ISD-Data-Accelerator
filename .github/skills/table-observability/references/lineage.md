# Lineage — Query Templates

> Part of the [Table Observability Skill](../SKILL.md). Load this file when the investigation sub-mode routes to an LN-* template.
>
> **Table-lineage execution rule:** For a table-scoped lineage ask such as "generate a lineage diagram for table X", treat upstream and downstream as one investigation. Execute LN-1 and LN-2 together in one batched step, then build and render a single Mermaid diagram with `renderMermaidDiagram`.

## LN-1: Upstream Dependencies (What Feeds This Table?)

```sql
-- What sources feed into Table_ID = {Table_ID}?
SELECT
    Source_Table_ID,
    Source_Entity,
    Source_Datastore,
    Source_Medallion_Layer,
    Source_Type,
    Source_Workspace_Name,
    Relationship_Type,
    Transformation_Applied,
    Lineage_Depth,
    Lineage_Path,
    Source_Lineage_Narrative
FROM Data_Pipeline_Lineage
WHERE Target_Table_ID = {Table_ID}
ORDER BY Lineage_Depth ASC;
```
> **When to use:** User asks "what feeds into table X?" or "where does this data come from?"
>
> **Zero-row interpretation:** If this query returns no rows, tell the user to run `NB_Generate_Lineage` to generate lineage, then re-run the investigation.

## LN-2: Downstream Dependents (What Does This Table Feed?)

```sql
-- What does Table_ID = {Table_ID} feed into?
SELECT
    Target_Table_ID,
    Target_Entity,
    Target_Datastore,
    Target_Medallion_Layer,
    Target_Type,
    Target_Workspace_Name,
    Relationship_Type,
    Transformation_Applied,
    Lineage_Depth,
    Lineage_Path,
    Target_Lineage_Narrative
FROM Data_Pipeline_Lineage
WHERE Source_Table_ID = {Table_ID}
ORDER BY Lineage_Depth ASC;
```
> **When to use:** User asks "what depends on table X?" or "what would break if this table changes?"
>
> **Zero-row interpretation:** If this query returns no rows, tell the user to run `NB_Generate_Lineage` to generate lineage, then re-run the investigation.

> **Batching note:** For table-scoped lineage diagrams, LN-1 and LN-2 should be batched together by default. Do not make the user wait through separate upstream and downstream investigation turns when both are needed for one lineage diagram.

## LN-3: Full Lineage Path for a Trigger

```sql
-- Complete lineage graph for Trigger_Name = '{Trigger_Name}'
SELECT
    Lineage_ID,
    Source_Entity,
    Source_Medallion_Layer,
    Target_Entity,
    Target_Medallion_Layer,
    Relationship_Type,
    Transformation_Applied,
    Lineage_Depth,
    Lineage_Path,
    Lineage_Group_Narrative,
    Is_Cross_Trigger,
    Cross_Trigger_Dependencies
FROM Data_Pipeline_Lineage
WHERE Trigger_Name = '{Trigger_Name}'
ORDER BY Lineage_Depth ASC, Lineage_Path ASC;
```
> **When to use:** User asks "show me the full lineage for trigger X" or "what does this pipeline do end-to-end?"
>
> **Zero-row interpretation:** If this query returns no rows, tell the user to run `NB_Generate_Lineage` to generate lineage, then re-run the investigation.

## Diagram Output Contract

- If lineage rows exist, produce a compact Mermaid flowchart that includes the resolved table and its immediate upstream and downstream relationships.
- Render the flowchart with `renderMermaidDiagram` rather than returning only fenced Mermaid text.
- Keep the prose brief after the diagram and use the evidence rows only to label edges, transformations, and relationship types.
