# Execution Order Contract

Use this shared reference for prompt files in `.github/prompts/` that must route the user before loading docs, reading files, or starting a workflow.

## Core Rule

- A bare slash-command invocation does not mean the user has already supplied enough scope to begin deeper research or execution.
- If the prompt input is empty, missing, generic, or still category-level broad, start with a bounded `vscode_askQuestions` call.
- The first action must be either:
  - direct routing from a clearly stated goal, or
  - a bounded `vscode_askQuestions` call when a decision is still missing

## Before Routing

- Do not run doc lookups, `grep_search`, `read_file`, file edits, or broader research before the first routing action.
- Keep the chat text brief and let the `vscode_askQuestions` call carry the choice whenever possible.
- Prefer one more bounded `vscode_askQuestions` call over an open-ended text question when the next step is still unclear.

## After Routing

- Once the scope is specific, load only the minimum docs, references, skills, or files required for that path.
- Let the target workflow load its own deeper references instead of preloading broad background material.
- If the target workflow has a direct execution path and one or two key inputs are missing, ask a bounded `vscode_askQuestions` call for those inputs rather than searching the workspace or docs to infer them.
- Do not substitute repo exploration for missing user inputs when a fast path exists. Ask for the smallest missing decision and then execute the fast path.

## Prompt-Specific Exceptions Stay Inline

- Keep workflow-specific mode switches, safety exceptions, and output requirements in the prompt.
- If a prompt needs stricter rules than this contract, add only the delta inline.

## Tie-Breaker

- If there is any conflict between early research and first-turn routing, routing wins.