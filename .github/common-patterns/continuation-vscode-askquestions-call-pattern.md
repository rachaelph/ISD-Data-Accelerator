# `vscode_askQuestions` Pattern

Shared contract for routing and continuation `vscode_askQuestions` calls across all prompts.

## Shared Interaction Contract

- Use a `vscode_askQuestions` call whenever the next step depends on a user choice, clarification, confirmation, or routing decision.
- A completed response does not need to end with a continuation `vscode_askQuestions` call when the answer already resolves the request, when the host renders the picker too early, or when the user does not want picker-driven follow-ups.
- If the `vscode_askQuestions` tool is unavailable, fails, or the call is cancelled, explicitly say so in the response.

## `vscode_askQuestions` Roles

- Routing or clarification `vscode_askQuestions` calls are same-turn controls. If the user selects a concrete action there, carry it out in the same turn.
- Continuation `vscode_askQuestions` calls are next-turn controls. Their selections start the next assistant turn.

## Router Rules

- If the user request is empty, generic, or still category-level broad, ask a bounded `vscode_askQuestions` call before loading docs, reading files, or running tools.
- If the user already gave a specific actionable goal, route directly from that goal and only ask another `vscode_askQuestions` call for the missing decision.
- Prefer chained bounded `vscode_askQuestions` calls over open-ended questions.
- Enable freeform input on `vscode_askQuestions` calls by default so the user always has a fourth path beyond the 3 fixed options.
- Keep pre-`vscode_askQuestions` call prose brief and let the `vscode_askQuestions` call carry the choice whenever possible.

## Sticky Approval Rule

- If the user has already approved proceeding for the same resolved target and action in the current conversation, treat that approval as still valid.
- Do not ask again for retries, reruns, sync-only follow-ups, deploy-only follow-ups, or the immediate next stage of the same workflow when the workspace, environment, branch, git folder, and risk level are unchanged.
- Ask again only when a required parameter is still unresolved, the target or risk materially changes, or the user explicitly cancels or revokes the earlier approval.

## Definition Of Done

- A turn is complete when the requested answer or deliverable has been fully provided.
- A prose-only ending is acceptable when no picker is needed or when the host renders pickers before the answer text.
- Do not use a continuation `vscode_askQuestions` call as a substitute for finishing the work.
- If the `vscode_askQuestions` call is cancelled, state that outcome in the response before ending the turn.

## Why This Exists

- New users often do not know what to do next after they get an answer.
- A continuation `vscode_askQuestions` call lowers the navigation burden by presenting the most relevant next steps directly.
- In this repo, the continuation picker is part of the workflow UX, especially for onboarding and multi-step accelerator tasks.
- Treat it as an optional UX enhancement, not a completion requirement.

## Core Rule

- Use a continuation `vscode_askQuestions` call only when it clearly improves the next step UX.
- Never use a continuation `vscode_askQuestions` call when the current host surfaces the picker before the answer is visible, when work is still in progress, or when the user has indicated they do not want picker-driven follow-ups.
- A concise prose ending is preferred over a disruptive picker.
- If you emit a continuation `vscode_askQuestions` call, do it only after the response content is complete and only in hosts where that ordering is actually preserved for the user.
- If the `vscode_askQuestions` tool is unavailable, fails, or is cancelled, explicitly say so in the response.

## Continuation `vscode_askQuestions` Call Rules

- If used, offer exactly 3 concrete fixed options.
- If used, keep freeform input enabled as the fourth path unless a documented safety reason requires it to be disabled.
- Match the options to the work just completed.
- Do not use placeholders such as `Other`, `More options`, or `None of these`.
- If prose mentions next steps and a continuation `vscode_askQuestions` call is used, those same next steps should appear in the options.

## Option Quality Gate

Every option must pass this test before inclusion:

- **Would the user need Copilot to do this?** If the user can do it faster alone (e.g., commit, open a file, run a command they already know), do not offer it.
- **Is this a genuine next step the user is likely to want?** Do not pad to 3 options with generic filler like "commit these changes", "test this now", or "keep going". If you cannot think of 3 genuinely useful options, skip the continuation picker entirely.
- **Does the option advance the user's goal?** Options should move the user's actual task forward, not just narrate the development lifecycle (commit → test → deploy).

If the work is self-contained and the user's next step is obvious or outside Copilot's scope, end with a brief prose summary instead of forcing a picker.

## When Not To Use One

- Do not use a continuation `vscode_askQuestions` call as a substitute for carrying out an already-selected routing action.
- Do not rely on a continuation `vscode_askQuestions` call to produce more output in the already-completed turn.
- Do not use one when the work is still running or when follow-up investigation is still pending.
- Do not use one when the user has said the picker interrupts reading or is otherwise unwanted.
- Do not use one in hosts where the picker renders before the answer body.

## Recommended Validation

- Validate that the requested answer was fully delivered before considering any continuation picker.
- If a continuation `vscode_askQuestions` call is used, validate that it added value and did not replace the answer.
- If a routing `vscode_askQuestions` call selection earlier in the turn was action-labeled, validate that the action was actually carried out before the response ends.

## End-Of-Turn Check

Before ending any completed response, verify all of the following:

1. The requested answer or deliverable has been provided.
2. Any follow-up control did not interrupt or precede the substantive answer.
3. If a continuation `vscode_askQuestions` call was used, it contains exactly 3 concrete options and keeps freeform input enabled unless a documented safety reason requires disabling it.
4. If the tool failed or was cancelled, that outcome is stated explicitly in the response.

## Common Failure Modes

- Options are generic lifecycle steps (commit/test/deploy) rather than actions the user actually needs Copilot for.
- A continuation picker is shown before the answer body, so the user is interrupted while the work is still finishing.
- A continuation `vscode_askQuestions` call is used even though the user has said they do not want one.
- The assistant keeps working after presenting a continuation picker, so the picker is acting like a premature stop signal.
- The response mentions next steps in prose, but those same next steps do not appear as concrete `vscode_askQuestions` options when a continuation picker is used.
- The assistant claims the tool is unavailable even though `vscode_askQuestions` succeeded earlier in the same turn.

## Suggested Evaluation Cases

1. Router flow with one or more same-turn `vscode_askQuestions` calls and no closing continuation picker because the answer already fully resolves the request
2. Direct answer with a concise prose ending and no picker because the host would surface it too early
3. Direct answer with a useful continuation `vscode_askQuestions` call in a host where the ordering is preserved
4. `vscode_askQuestions` tool failure path with an explicit failure note
5. Routing `vscode_askQuestions` call selection that triggers work immediately in the same turn

## What Stays Inline In Each Prompt

- frontmatter and description
- task framing and scope
- prompt-specific router options
- prompt-specific safety rules
- execution-order exceptions unique to the workflow
- output requirements unique to the workflow