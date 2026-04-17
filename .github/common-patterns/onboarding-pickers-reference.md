# Onboarding Pickers Reference

Use these canonical `vscode_askQuestions` picker definitions for `.github/prompts/fdp-01-onboarding.prompt.md`.

## Top-Level Onboarding Picker

- Question: `What do you want to do first in the accelerator?`
- Options:
  - `Help me understand this accelerator`
  - `Build or troubleshoot data pipelines`
  - `Deploy or manage workspaces`

## Understanding Picker

- Ask only after `Help me understand this accelerator`.
- Question: `Which part do you want to understand first?`
- Options:
  - `Architecture and medallion flow`
  - `Metadata-driven pipelines and authoring`
  - `DevOps, observability, or repo layout`

## Understanding Drill-In Picker

- Ask only after `DevOps, observability, or repo layout`.
- Question: `Which area do you want to understand first?`
- Options:
  - `DevOps, workspaces, and deployment`
  - `Troubleshooting and observability`
  - `Repo layout and where to start`

## Data Workflow Picker

- Ask only after `Build or troubleshoot data pipelines`.
- Question: `Which data workflow do you want next?`
- Options:
  - `Build my first metadata-driven pipeline`
  - `Investigate a pipeline or table issue`
  - `More pipeline options`

## Environment Task Picker

- Ask only after `Deploy or manage workspaces`.
- Question: `Which environment task do you want next?`
- Options:
  - `Deploy the accelerator IP`
  - `Set up a feature workspace`
  - `More workspace options`

## Fallback Picker

- Use when the previous 3 fixed options were too narrow and you need one more bounded follow-up before relying on freeform input.
- Question: `Which of these is closest to what you want?`
- Options:
  - `I want a structured onboarding plan`
  - `I know the workflow but need help choosing the right command`
  - `I want a different onboarding path`