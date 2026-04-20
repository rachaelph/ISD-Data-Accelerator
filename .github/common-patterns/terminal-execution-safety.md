# Terminal Execution Safety

Use this shared reference for prompts that execute PowerShell, SQL, or Databricks operations in the terminal.

## Core Rules

1. Always use `isBackground: false` for investigation and deployment commands unless the workflow explicitly requires a long-running background process.
2. Use an explicit timeout. Do not use `timeout: 0`.
3. For long-running foreground workflows, the tool timeout must exceed the script's own wait budget. Do not set a terminal timeout that is shorter than the command's expected completion window.
4. Never use a 5-minute timeout for Databricks job or pipeline workflows that poll for completion. A short tool timeout truncates the local watcher and makes an in-progress Databricks run look like the local command stopped.
5. Do not treat `120000` as a general default for deployment, workspace sync, or metadata deployment workflows. Two minutes is only appropriate for short commands.
6. If output appears stuck, wait rather than re-running the command.
7. Append `2>&1` when invoking `.ps1` scripts so PowerShell errors are visible.
8. For agent workflows (`/fdp-04-commit`, `/fdp-05-run`, `/fdp-06-investigate`, `/fdp-06-troubleshoot`), run `python automation_scripts/agents/preflight.py` once per session and continue only when it returns `"ready": true`. The preflight verifies:
    - **Python interpreter 3.10+** on the active `python` command. Several agent scripts use PEP 604 union syntax and builtin generic subscripting; 3.9 or earlier will not import them.
    - **Azure CLI** installed and signed in (`az login`). Agent scripts authenticate to Azure Databricks via Azure AD tokens obtained from the Azure CLI.
    - **PowerShell 7 (`pwsh`)** on PATH **and as the active shell**. Windows PowerShell 5.1 (a) does not stream long-running command output reliably into Copilot Chat and (b) writes redirected files as UTF-16LE by default, which corrupts JSON output files like `run_output.json`. The repo ships [.vscode/settings.json](.vscode/settings.json) that pins the default terminal profile to PowerShell 7; if a user has opened an older terminal tab, preflight flags it as `wrong-shell` and the fix is to close the tab and open a new one.
    - **Python packages** from `automation_scripts/requirements-agent.txt` (`azure-identity`, `databricks-sdk`) importable in the active interpreter.
    - If any are missing, re-run with `--install` to bootstrap (prompts y/N before each install; pass `--yes` to skip prompts). Example: `python automation_scripts/agents/preflight.py --install --pretty`.
9. For authoring workflows (`/fdp-03-author`, `/fdp-03-convert`) that do not call Azure or the workspace, run the lightweight `python automation_scripts/agents/preflight.py --python-only` once per session instead. It checks only the Python interpreter version and skips Azure/package checks, so offline authors are not forced through `az login`.
10. Treat the auth preflight as session-idempotent: the first check may surface install or `az login` guidance, but later checks in the same session should return `ready` immediately without repeating those steps.
11. For one-shot Python CLIs, run the real command directly instead of adding a warmup or verification call.
12. For long-running Python CLIs that poll remote Databricks work, prefer human-readable progress on stdout, for example by passing `--pretty` when supported.

## Long-Running Foreground Rule

When a workflow intentionally waits for a remote Databricks job to finish in the foreground, size the terminal timeout from the workflow's own wait settings instead of guessing.

1. If the script has a `--max-wait-minutes` or equivalent argument, the terminal timeout must be longer than that budget.
2. Include headroom for startup, polling, and final result emission.
3. For `automation_scripts/agents/run_pipeline.py` with the default `--max-wait-minutes 60`, use at least `timeout: 3900000`.
4. If the workflow explicitly lowers `--max-wait-minutes`, recompute the terminal timeout so it still exceeds the script wait budget.

## Deployment Timeout Rule

For commit, workspace sync, and metadata deployment flows, choose a generous timeout based on expected change volume.

1. Do not default to `120000` for `automation_scripts/agents/commit_pipeline.py`.
2. Use at least `timeout: 300000` for normal `/fdp-04-commit` runs.
3. Increase the timeout further when many workspace items, module changes, or metadata files may be deployed.

## Connection and Session Rules

1. Acquire tokens or bootstrap connection state once per conversation when the workflow is session-based.
2. Keep bootstrap and the first real query in the same terminal call when session variables must persist.
3. Reuse the same terminal session for follow-up commands.
4. Do not add verification commands just to confirm variables exist; run the real command.

## Output Rules

1. Keep execution and output formatting in the same terminal call.
2. If bootstrap noise makes formatted output unreliable, prefer explicit `Write-Host` output.
3. For large text fields, output individual fields instead of relying on `Format-Table` or `Format-List`.

## Budget Rule

If a workflow is spending multiple terminal calls on setup, verification, or re-display, consolidate. Repeated round-trips are usually a workflow design problem, not a user requirement.
