---
agent: agent
description: "Maintain core accelerator internals (notebooks, pipelines, stored procedures) with full impact analysis"
---

# Maintain Accelerator Internals

Make the following change to the accelerator: **{{ change_description }}**

## Execution Order

Prompt-specific input: `{{ change_description }}`.

## `vscode_askQuestions`-First Router

Use bounded `vscode_askQuestions` calls to identify the internal surface area before loading checklists or tracing dependencies.

- Top-level maintenance `vscode_askQuestions` call
    - Question: `Which internal area are you changing?`
    - Options:
        - `Notebooks or helper functions`
        - `Pipelines or orchestration flow`
        - `Stored procedures, schemas, or metadata contracts`
- If the request is still broad, ask one more bounded `vscode_askQuestions` call for the missing detail:
    - bug fix vs enhancement
    - single component vs cross-cutting flow
    - code change only vs code plus docs/tests
- Once the scope is specific, continue with the full impact-analysis workflow below.

## Workflow

```
┌─────────────────────────────────────────────────────────────┐
│  STEP 1: Load the Checklist                                 │
├─────────────────────────────────────────────────────────────┤
│  READ: docs/Notebook_Update_Checklist.md                    │
│  Understand all required sync points                        │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  STEP 2: Trace the Data Flow                                │
├─────────────────────────────────────────────────────────────┤
│  • WHERE does this component sit in the flow?               │
│    Orchestration → Metadata → Data Movement → Processing    │
│  • WHAT calls it? (upstream dependencies)                   │
│  • WHAT does it call? (downstream dependencies)             │
│  • WHAT data does it read/write?                            │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  STEP 3: Make the Code Changes                              │
├─────────────────────────────────────────────────────────────┤
│  • Follow existing code patterns                            │
│  • Use consistent naming conventions                        │
│  • Add appropriate logging                                  │
│  • NB_Batch_Processing is orchestration ONLY — all code     │
│    logic (transforms, serialization, computation) MUST go   │
│    in helper functions (NB_Helper_Functions_1/2/3)           │
│  • Update ALL affected components (see Key Relationships)   │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  STEP 4: Update All References                              │
├─────────────────────────────────────────────────────────────┤
│  • Split guides (DATA_INGESTION_GUIDE.md, DATA_TRANSFORMATION_GUIDE.md,  │
│    DATA_MODELING_GUIDE.md, CORE_COMPONENTS_REFERENCE.md,                  │
│    MONITORING_AND_LOGGING.md) — walkthroughs, snippets                    │
│  • METADATA_GENERATION_GUIDE.md (conversion, examples)          │
│  • TRANSFORMATION_PATTERNS_REFERENCE.md (patterns, templates)    │
│  • METADATA_SQL_FILE_FORMAT.md (SQL format, error prevention)    │
│  • Metadata Reference (if config attributes changed)        │
│  • Metadata validation logic / wrapper flow (if config      │
│    attributes changed)                                      │
│  • Integration tests for affected components                │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  STEP 5: Deliver with `vscode_askQuestions`-Backed Next Actions │
├─────────────────────────────────────────────────────────────┤
│  • List ALL components changed (not just notebooks!)        │
│  • List what user must manually verify/complete             │
│  • Provide test steps to validate changes                   │
│  • Put next actions in the `vscode_askQuestions` call      │
│    instead of a prose-only follow-up list                   │
│  • Follow the shared continuation `vscode_askQuestions` contract in        │
│    `../common-patterns/continuation-vscode-askquestions-call-pattern.md`      │
└─────────────────────────────────────────────────────────────┘
```

## Key Relationships (Impact Matrix)

| If You Change... | You May Also Need to Change... |
|------------------|-------------------------------|
| **Metadata Table schema** | Stored procedures that read/write it, notebooks that consume the data |
| **Stored Procedure output** | Pipelines that call it, notebooks that use the results |
| **Pipeline parameters** | Stored procedures providing the params, notebooks receiving them |
| **Notebook helper functions** | Other notebooks that call them, tests that validate them |
| **Config attributes** | Metadata Reference docs, validation script, all consuming code |

## Verification Checklist

Before completing the request, confirm:

- [ ] Documentation examples are updated (if applicable)
- [ ] Metadata Reference reflects any new/changed attributes
- [ ] Metadata validation accepts new configurations (if applicable)
- [ ] Unit tests added/updated for changed behavior
- [ ] User is informed of any manual follow-up needed

## Documentation Reference

| Topic | File | Search Pattern |
|-------|------|----------------|
| Notebook Update Checklist | `docs/Notebook_Update_Checklist.md` | `# Notebook Update Follow-Up Checklist` |
| Notebooks for Data Movement | `docs/CORE_COMPONENTS_REFERENCE.md` | `## Notebooks for Data Movement` |
| Metadata Reference | `docs/METADATA_REFERENCE.md` | `# Metadata Reference` |
| Extending the Accelerator | `docs/Extending_the_Accelerator.md` | `# Extending the Accelerator` |
| Trigger Design & Dependencies | `docs/Trigger_Design_and_Dependency_Management.md` | `# Trigger Design and Dependency Management` |