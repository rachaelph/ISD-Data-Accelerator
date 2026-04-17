---
applyTo: "ado_pipelines/**,automation_scripts/**,.github/workflows/**,workspace_cicd/**"
---

# DevOps & CI/CD Instructions

> Auto-applied when working on CI/CD pipelines, deployment scripts, and workflow files.

---

## Non-Negotiables

1. **METADATA IN GIT** — All metadata SQL files must be version controlled
2. **NEVER EDIT PROD DIRECTLY** — Always go through Git → CI/CD → deployment
3. **TEST IN FEATURE WORKSPACE** — Validate changes before merging to dev

---

## 🚫 FORBIDDEN

| ❌ Never Do This | Why |
|------------------|-----|
| Edit metadata directly in production | Bypasses version control and audit trail |
| Hardcode Table_IDs in DELETE statements | Use Trigger_Name subquery for environment portability |
| Skip feature workspace testing | Catches issues before they affect shared environments |
| Manually deploy to higher environments | CI/CD ensures consistency and traceability |

---

## Naming Conventions

| Context | Convention | Example |
|---------|-----------|---------|
| Pipeline parameters | `snake_case` | `workspace_id` |
| Pipeline output variables | `snake_case` | `metadata_changed` |
| Environment/repository variables | `UPPER_SNAKE_CASE` | `WORKSPACE_ID` |
| PowerShell parameters | `PascalCase` | `$WorkspaceId` |
| Stage/job/deployment names | `PascalCase` | `DeployMetadata` |
| Template parameters (ADO) | `UPPER_SNAKE_CASE` | `SERVICE_CONNECTION` |
| Reusable workflow inputs (GH) | `snake_case` | `workspace_id` |

---

## GitHub Environment-Scoped Variables

- Use `WORKSPACE_ID` as an environment-level variable (different per environment)
- Use `DEV_WORKSPACE_ID` as a repo-level variable (needed by QA/PROD releases for source ID mapping)
- Use `SERVICE_PRINCIPAL_CLIENT_ID` as an environment-level secret (no `DEV_` prefix needed)

---

## Key Topics

### Git Workflow for Metadata SQL Notebooks
- Metadata files live in `metadata/*.Notebook/notebook-content.sql` (one notebook per trigger)
- Branch strategy: feature → dev → main
- Merge conflict resolution for metadata

### Azure DevOps Pipelines
- Pipeline templates in `ado_pipelines/`
- Variable groups and service connections
- Deployment stages (Dev → Test → Prod)

### GitHub Actions
- Workflow configuration in `.github/workflows/`
- Secrets management
- Environment protection rules

### Multi-Developer Environment
- Feature workspace setup via `NB_Setup_Feature_Workspace` notebook (automated) or manual steps
- Isolated development
- Table_ID range coordination (Dev1: 100-199, Dev2: 200-299, etc.)
- Merge strategies

### Workspace Deployment
- Fabric workspace management
- Artifact deployment
- Configuration promotion
- Generalizing CI/CD to deploy any workspace with a mapping CSV (see `docs/Generalizing_Workspace_CICD.md`)

### Full Reloads
- When to trigger (schema changes, data corrections)
- Pipeline configurations in `data_pipelines/full_reloads/`
- Impact on downstream processes

---

## Repository Structure for DevOps

```
ado_pipelines/
├── DataAccessControl/
├── ManagedPrivateEndpoints/
├── Shortcuts/
├── TriggerFullReloads/
├── Workspace_CICD/
├── WorkspaceAccessControl/
├── WorkspaceBulkEdit/
└── WorkspaceDeployment/

workspace_cicd/
└── workspace_cicd.py

data_pipelines/
└── full_reloads/
```

---

## Best Practices

| Practice | Why |
|----------|-----|
| Use separate workspaces for Dev/Test/Prod | Environment isolation |
| Version metadata SQL files in Git | Traceability and rollback capability |
| Automate deployments, avoid manual changes in Prod | Consistency and audit trail |
| Use full reload triggers after schema changes | Ensures data consistency |
| Coordinate Table_ID ranges across developers | Prevents merge conflicts |
| Run metadata validation during authoring/conversion before committing | Catches errors early without adding validation to `/fdp-04-commit` |

---

## Documentation Lookup

Grep across `docs/` for the user's keywords, then read only the matched section.

Most relevant sections for DevOps work:
- Metadata and authoring
- Code and deployment assets
- Feature workspace testing
- CI/CD and deployment FAQ topics
