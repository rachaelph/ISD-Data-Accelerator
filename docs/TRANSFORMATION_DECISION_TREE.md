# Transformation Decision Tree

> Last updated: 2026-03-29

This is the canonical escalation guide for deciding when to use built-in transformations, when to escalate to custom functions, and when to fall back to standalone execution.

## Transformation Hierarchy

> Always start with Tier 1. Never skip levels.

| Priority | Approach | When to Use | Examples |
|---|---|---|---|
| **Tier 1** | Built-in `data_transformation_steps` | Always try first | `derived_column`, `filter_data`, `join_data`, `aggregate_data`, `pivot_data`, `add_window_function`, `conditional_column`, `string_functions` |
| **Tier 2** | Custom notebooks inside the framework | Only if built-ins cannot achieve the requirement | Complex Python/PySpark logic, ML inference, external API calls, recursive hierarchies, 10+ source tables, custom data staging |
| **Tier 3** | Standalone Fabric notebook or dataflow | Absolute last resort | Existing external notebooks, legacy migration, cross-workspace execution that cannot fit Tier 2 |

## Tier Escalation Rules

1. Exhaust Tier 1 options before suggesting Tier 2.
2. Exhaust Tier 2 options before suggesting Tier 3.
3. State why lower tiers do not work when escalating.
4. Tier 3 breaks lineage tracking and should stay rare.

## Quick Decision Checklist

- Can any combination of built-in transformations solve this? If yes, use Tier 1.
- Are there 10+ source tables to join or multi-pass logic that becomes unreadable in metadata? If yes, consider Tier 2.
- Does the requirement need custom Python/PySpark logic not covered by built-ins? If yes, consider Tier 2.
- Do you need help choosing the specific Tier 2 function? Use [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md).
- Is this only orchestrating an existing external notebook or dataflow that cannot be refactored into the framework? If yes, use Tier 3.

## Tier 2 Choice

When Tier 2 is necessary, do not choose a custom function ad hoc. Use [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) as the single source of truth for selecting the correct custom function type.

## Related References

- [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md)
- [METADATA_GENERATION_GUIDE.md](METADATA_GENERATION_GUIDE.md)
- [.github/common-patterns/metadata-authoring-reference.md](../.github/common-patterns/metadata-authoring-reference.md)
