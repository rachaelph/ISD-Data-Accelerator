---
agent: agent
description: "Deploy the accelerator IP into a Fabric workspace and guide the first-time setup steps that make the deployed workspace usable with Git, local cloning, and ongoing accelerator development"
---

# Deploy And Set Up Accelerator IP

Deploy the Fabric Data Platform Accelerator into a Fabric workspace, then guide the required first-time setup steps for: **{{ deployment_context }}**

## Execution Order

Prompt-specific input: `{{ deployment_context }}`.

## `vscode_askQuestions`-First Router

- Question: `What deployment task do you need?`
- Options:
	- `Deploy the accelerator IP`
	- `Deploy and set up the working repo`
	- `Re-deploy or repair an existing workspace`
	- `Choose the right deployment shape`
- Follow-up dimensions if still broad: deployment scope (`single` vs `split`), lifecycle stage (`first-time` vs `re-deployment`), blocker type (`access`, `git setup`, `notebook import`, or `parameter choices`).

## Workflow

```
STEP 1: Confirm Deployment Shape
├── Determine whether the user wants a single-workspace or multi-workspace deployment
├── Confirm whether this is first-time deployment or re-deployment of the core IP
├── Identify required workspace IDs, naming choices, and target git/repo layout
└── Call out any blockers early (missing Fabric access, missing repo access, missing notebooks, or unclear working-repo structure)

STEP 2: Load the Deployment Guidance (parallel batch)
├── grep_search: `How do I deploy the IP` in docs/FAQ.md
├── read_file: docs/Deployment_Guide.md (deployment steps and parameters)
└── grep_search: `# 1. Initial Setup & Prerequisites|## Workspace Deployment` in docs/DevOps.md
All three lookups are independent — run them in one parallel batch, then read_file the matched FAQ section.

STEP 3: Build the Deployment Plan
├── List the notebooks that must be imported into Fabric
├── Explain the deployment parameters the user needs to set in CreatingArtifacts_Orchestrator.ipynb
├── Explain how the working repository should be set up after deployment, including local clone and Fabric Git integration
├── Highlight default single-workspace behavior vs optional multi-workspace overrides
└── Warn when the user's workspace split or repo layout would complicate CI/CD or ID replacement

STEP 4: Guide the Initial Deployment
├── Tell the user to run CreatingArtifacts_Orchestrator.ipynb in Fabric
├── Explain what artifacts will be created
├── Explain how to move the accelerator support folders into the cloned working repo
├── Warn the user not to copy the accelerator repo's artifact folder into the target repo, because that will conflict with the target repo's real Fabric artifact tree and confuse Copilot automation
├── Use the target repo's actual top-level Fabric artifact folder in guidance, typically the folder connected to the workspace in Fabric Git integration and often named after the workspace
├── Give success criteria for a healthy first deployment and Git connection
└── Point to post-deployment metadata configuration as the next required step

STEP 5: End with Continuation `vscode_askQuestions` Call
└── Follow the shared continuation contract
```

## Documentation Reference

Use `grep_search` first, then `read_file` for the matched section unless the file below is already intentionally listed for direct reading.

| Topic | File | Search Pattern |
|-------|------|----------------|
| Deployment FAQ summary | `docs/FAQ.md` | `### How do I deploy the IP \(Intellectual Property\)\?` |
| Initial deployment guide | `docs/Deployment_Guide.md` | Direct read |
| Initial setup prerequisites | `docs/DevOps.md` | `# 1. Initial Setup & Prerequisites` |
| Post-deploy Git repo setup | `docs/Deployment_Guide.md` | `4. \*\*Set up Git and your target repository\*\*` |
| Workspace deployment constraints | `docs/DevOps.md` | `## Workspace Deployment` |
| Feature workspace limitations | `docs/FAQ.md` | `### Why isn't feature workspace deployment fully automated\?` |
| Post-deployment artifact review | `docs/DevOps.md` | `# Post Initial Deployment Artifact Review` |

## Repo Layout Rules

- Treat Git/repo setup as part of first-time deployment guidance, not as a separate workflow, unless the user is explicitly asking for broad onboarding.
- Use the target repo's actual top-level Fabric artifact folder in user-facing guidance. Do not assume a fixed folder name.
- If the user has a separate example-artifacts repo, keep that distinction explicit: the working repo should contain the deployed workspace artifacts plus the copied accelerator support folders needed for Copilot guidance, docs, and automation.
- When explaining folder moves, be explicit about which folders belong in the cloned working repo: `.github/`, `docs/`, and any needed `automation_scripts/`, `workspace_cicd/`, CI/CD definitions, and `integration_tests/`.
- Be explicit that the accelerator repo's artifact folder should not be copied into the target repo. The target repo's own artifact folder must remain the only Fabric artifact root so Copilot automation resolves the correct files.

## Output Contract

Return:

1. A deployment plan tailored to the user's context
2. The exact notebooks/artifacts they need to work with
3. The key deployment parameters they must set or leave blank
4. The recommended deployment pattern (single workspace vs split)
5. The concrete post-deployment repo and Git setup steps, using the user's actual artifact folder layout