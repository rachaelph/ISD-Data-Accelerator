---
agent: agent
description: "Scan accelerator documentation for gaps, outdated content, and missing examples"
---

# Audit Documentation for Gaps

Review the accelerator documentation for gaps, outdated content, and missing examples. Focus on: **{{ scope_or_recent_changes }}**

## Execution Order

Prompt-specific input: `{{ scope_or_recent_changes }}`.

## `vscode_askQuestions`-First Router

Use bounded `vscode_askQuestions` calls to scope the audit before scanning files.

- Top-level audit `vscode_askQuestions` call
    - Question: `What kind of content audit do you want?`
    - Options:
        - `Audit recently changed docs or prompts`
        - `Audit a workflow end-to-end for gaps`
        - `Find outdated references or missing examples`
- If the request is still broad, ask one more bounded `vscode_askQuestions` call for the missing dimension:
    - docs vs prompts/instructions
    - specific files vs repo-wide scan
    - review-only vs implement fixes
- Once the audit target is specific, continue with the checklist below.

## Workflow

```
STEP 1: Determine Scope
├── If user specifies files/topics → focus on those
├── If no scope given → scan recent changes or the current conversation history
└── Identify which docs and instruction files are relevant

STEP 2: Evaluate Against Checklist
For each relevant area, check:
├── Are there edge cases not covered in docs/ or instruction files?
│   └── Only flag if the scenario is likely to recur for other users
├── Is any guidance ambiguous enough to cause different implementations?
│   └── Only flag if multiple plausible interpretations exist
├── Would a concrete example (SQL snippet, JSON sample) reduce confusion?
│   └── Flag when an example would save significant time for future tasks
├── Is any referenced information potentially outdated?
│   └── Flag if version drift (Databricks changes, renamed notebooks, deprecated tables) could mislead
└── Should any Q&A be added to docs/FAQ.md?
    └── Only if: likely to recur, repo-specific, non-trivial, not already covered

STEP 3: Produce Gap Report
│  | Gap Type | File | Section | Proposed Change |
│  |----------|------|---------|-----------------|
│  | [type]   | [path] | [section] | [specific text or description] |

STEP 4: Implement (if user approves)
├── Make the documentation edits
├── Verify cross-references still work
└── Confirm no instruction files need updating
```

## Gap Types

| Type | Description |
|------|-------------|
| Missing edge case | Behavior or scenario likely to recur but not documented |
| Ambiguous guidance | Multiple plausible interpretations that could change implementation |
| Missing example | A concrete snippet would significantly reduce confusion |
| Outdated reference | Version drift or renamed components that could mislead |
| FAQ candidate | Non-trivial, repo-specific Q&A likely to recur |
| Stale cross-reference | A doc references a file, section, or pattern that no longer exists |