---
agent: agent
description: "Route a new user to the right first accelerator workflow with `vscode_askQuestions`-first onboarding; only provide the full onboarding plan when the user explicitly asks for it"
---

# New User Onboarding

Route a new user based on: **{{ newcomer_context }}**

Use `../common-patterns/onboarding-pickers-reference.md` for the canonical onboarding picker definitions.

## Execution Order

- Explicit onboarding mode requires an explicit ask for onboarding content such as a checklist, week-one plan, training plan, or structured onboarding.
- Do not infer explicit onboarding mode from the prompt name alone.

## `vscode_askQuestions`-First Router

- Generic or empty `{{ newcomer_context }}`: use the top-level onboarding picker from `../common-patterns/onboarding-pickers-reference.md`.
- Specific user goal: route directly; only ask another picker if still category-level broad.
- Explicit onboarding ask: skip router mode and run the full onboarding workflow.
- After each picker choice, use the next canonical picker from `onboarding-pickers-reference.md`. When the subtopic is concrete, give lightweight guidance or route to the matching workflow command.

## Explicit Onboarding Workflow

Run this only when the user explicitly asks for onboarding content such as a checklist, week-one plan, training path, or structured learning path.

1. Understand onboarding context
	- Capture role, experience level, immediate goal, and blockers.
	- Confirm environment constraints such as Databricks workspace access, workspace permissions, and Copilot availability.
2. Build a day-0 checklist
	- Clone or open the repo, verify Copilot, and confirm prerequisites and access.
3. Create a week-1 learning path
	- Day 1 architecture and repo orientation
	- Day 2 deployment fundamentals
	- Day 3 first metadata pipeline end-to-end
	- Day 4 transformations and data quality
	- Day 5 troubleshooting and real-world scenarios
4. Deliver one practical first win
	- Give one realistic end-to-end task with exact files or commands and clear success criteria.
5. End with follow-up actions
	- Tailor the required continuation `vscode_askQuestions` call to the user's next likely task.

## Routing Outcomes

- Understanding the accelerator or architecture: give lightweight orientation and, if needed, use the next onboarding picker from the shared reference.
- Metadata-driven pipelines or authoring: continue with lightweight guidance and recommend `/fdp-03-author`.
- Deployment, workspaces, or feature branches: continue with lightweight guidance and recommend `/fdp-02-deploy` or `/fdp-02-feature`.
- Investigation or troubleshooting: recommend `/fdp-06-investigate` or `/fdp-06-troubleshoot` based on whether the user is asking for status or root-cause debugging.
- Structured onboarding: stay in this prompt and run the explicit onboarding workflow.

## Documentation Use

Only consult docs after STEP 1 identifies a concrete need for explanatory content.

- For `vscode_askQuestions`-only routing, do not load onboarding docs first.
- For direct routing to another workflow, route first; let the target workflow load its own docs.
- For lightweight accelerator orientation, search `docs/FAQ.md` first for the user's actual question, then load a deeper doc only if the FAQ is insufficient.
- For explicit onboarding mode, `grep_search` across `docs/` for the user's keywords to locate the minimum specific sections needed.

## Output Contract

Return one of these shapes based on the user's intent:

1. First-question router mode:
	 - brief orientation at most
	 - if `{{ newcomer_context }}` is empty or generic, the first output must be the top-level `vscode_askQuestions` popup
	 - if the user already gave a specific goal, route directly from that goal with no unnecessary `vscode_askQuestions` call
	 - if the chosen path is still broad, ask the next bounded `vscode_askQuestions` call immediately
	 - once the intent is specific, provide concise guidance or route to another prompt
	 - no full week-1 plan unless the user explicitly asked for onboarding content
2. Explicit onboarding mode:
	 - day-0 checklist
	 - week-1 learning path
	 - first practical win
	 - follow-up tasks and escalation paths
	 - continuation behavior must follow `../common-patterns/continuation-vscode-askquestions-call-pattern.md`

## Guardrails

- Never treat the slash command name by itself as evidence that the user wants explicit onboarding mode.
- When in doubt between router mode and explicit onboarding mode, choose router mode.
- For broad first-turn onboarding, make understanding the accelerator the first suggested path.
- Do not provide a detailed onboarding explanation immediately after `Help me understand this accelerator`; narrow the topic first.
- If `vscode_askQuestions` behavior appears inconsistent, treat as a runtime issue and avoid chaining additional calls until the inconsistency is surfaced.
- Preserve the deeper onboarding plan for users who explicitly want structured onboarding.
