---
agent: agent
description: "Refactor prompt and customization files toward HVE principles: thin routers, progressive disclosure, shared references, and script-first workflows"
---

# Prompt

Refactor the selected customization files for **{{ target_files_or_scope }}** so they align better with HVE principles while preserving workflow behavior.

## Execution Order

Prompt-specific input: `{{ target_files_or_scope }}`.

## `vscode_askQuestions`-First Router

Use bounded `vscode_askQuestions` calls to clarify customization scope before auditing prompt files.

- Top-level customization `vscode_askQuestions` call
	- Question: `What customization refactor do you want?`
	- Options:
		- `Refactor one prompt or instruction file`
		- `Normalize several prompt files to one pattern`
		- `Review prompt, skill, or instruction drift first`
- If the request is still broad, ask one more bounded `vscode_askQuestions` call for the missing scope:
	- prompt vs instruction vs skill
	- single file vs prompt family
	- review-only vs edit-in-place
- Once the scope is specific, continue with the HVE audit and conservative refactor workflow below.

## Goal

Make the target customization files thinner, more reusable, and less context-heavy.

Preserve the intended workflow, but reduce prompt bloat, duplicated guidance, and prose-heavy operational instructions.

## HVE Principles

Apply these principles during the refactor:

- Prefer thin prompts and thin skill routers over large all-in-one instruction dumps
- Use progressive disclosure: keep only the minimum always-on guidance inline
- Move bulky lookup tables, repeated examples, and reference material into shared references when reused
- Prefer script-first or tool-first execution for repeated workflows instead of reconstructing commands from prose
- Reduce duplicated terminal-safety, validation, and documentation-lookup guidance across prompts
- Keep prompt bodies focused on routing, decisions, guardrails, and required outcomes
- Reuse existing shared references and helper scripts before creating new files
- Keep fallback/raw commands only where they are intentionally useful

## Repository-Specific Expectations

- Prompt files in `.github/prompts/` should act as task routers, not encyclopedias
- Shared reusable guidance belongs in `.github/common-patterns/` or skill `references/`
- If a prompt explicitly defines continuation `vscode_askQuestions` behavior or next-action `vscode_askQuestions` call semantics, reference `../common-patterns/continuation-vscode-askquestions-call-pattern.md` instead of restating the full contract inline
- `vscode_askQuestions` guidance should standardize on exactly 3 fixed options plus freeform input enabled as the fourth path unless a specific safety reason requires freeform to be disabled
- Deterministic repeated execution patterns should prefer wrapper scripts in `automation_scripts/` or skill `scripts/`
- Keep prompt-specific rules inline only when they are unique to that prompt
- Preserve repo terminology, Fabric workflow semantics, and safety constraints
- Do not change behavior just to make wording shorter

## Workflow

```
STEP 0: Determine Scope
├── Identify the selected prompt/customization files from {{ target_files_or_scope }}
├── If the scope is broad, prioritize `.github/prompts/*.prompt.md`
└── Load only the files needed for the active refactor

STEP 1: Audit for HVE Drift
├── Identify oversized sections that should not stay always-on
├── Identify duplicated guidance across multiple prompts
├── Identify duplicated continuation `vscode_askQuestions` call contract language that should point to `../common-patterns/continuation-vscode-askquestions-call-pattern.md`
├── Identify repeated raw command patterns that should use wrappers/scripts
├── Identify large embedded doc lookup tables and reference catalogs
└── Identify content that should remain inline because it is prompt-specific

STEP 2: Refactor Conservatively
├── Keep a concise top-level router or quick path section
├── Replace repeated prose with references to shared docs or scripts when available
├── Prefer existing wrapper scripts for deterministic workflows
├── Preserve critical safety constraints and required execution order
└── Create new shared references only when reuse is clear and justified

STEP 3: Validate
├── Validate all touched customization files
├── Fix malformed frontmatter, broken markdown, or invalid references
└── Do not leave the prompt in a partially migrated state

STEP 4: Report
├── Summarize what was slimmed
├── Summarize what was extracted or reused
├── State what remains intentionally inline and why
└── List any remaining non-blocking cleanup opportunities
```

## Keep Inline

Keep these inline when they are specific to the target prompt:

- frontmatter and description
- task framing and user intent handling
- quick router / mode selection
- prompt-specific safety rules
- output requirements
- decision points that change behavior

## Move Out When Appropriate

Prefer shared references or scripts for:

- large documentation lookup tables
- repeated terminal execution guidance
- repeated validation instructions
- repeated raw script invocations
- bulky transformation or reference catalogs
- long example sections reused elsewhere

## Avoid

- unnecessary new files
- changing workflow semantics without reason
- duplicating guidance already covered by instructions, skills, or shared refs
- adding framework theory that does not change model behavior
- broad always-on instructions when on-demand references are enough

## Output Contract

Return:

1. Files changed
2. Why each change improves HVE alignment
3. What stayed inline and why
4. What was moved, replaced, or reused
5. Residual cleanup candidates, if any

## Success Criteria

- smaller always-on context
- clearer routing
- more reuse of shared references and scripts
- preserved workflow behavior
- valid frontmatter and markdown
- no unnecessary prompt bloat added during the cleanup