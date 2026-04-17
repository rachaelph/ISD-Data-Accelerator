# Security Guidance for Reviewers — Databricks Data Engineering Accelerator

> Last updated: 2026-03-20

> **Audience:** Security consultants, penetration testers, and code reviewers analyzing this repository  
> **Purpose:** Provide architectural context that prevents common false positives when scanning Databricks metadata-driven solutions  
> **Maintained by:** Accelerator IP team

---

## Before You Start — Read This First

This repository is a **metadata-driven data engineering platform** built on Databricks. It is **not** a web application, API service, or user-facing product. Security scanners and manual code reviews routinely flag patterns in this codebase that are false positives because the tool or reviewer is applying a web/application security mental model to a data platform.

This document explains the architecture, the patterns you will encounter, and why they are expected. We encourage every reviewer to read the relevant section before filing a finding.

**What this solution is:**
- An internal data ingestion and processing framework used by data engineers
- Metadata-driven: pipelines are configured via SQL records in a governed warehouse, not code changes
- Deployed on Databricks — a SaaS analytics platform with its own security model

**What this solution is NOT:**
- A web application with user input flowing into queries
- A service with external endpoints, APIs, or untrusted callers
- An application that stores or manages credentials in source code

---

## 1. Architecture Overview

Understanding the data flow is critical to evaluating any finding against this codebase.

```
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA ENGINEER WORKFLOW                            │
│                                                                     │
│  1. Engineer writes SQL metadata  ──►  Git repository               │
│  2. Pull request created          ──►  Peer review + approval       │
│  3. CI/CD validation runs         ──►  Automated metadata validator │
│  4. Merge to main                 ──►  Deployment pipeline          │
│  5. Metadata stored in warehouse  ──►  Databricks SQL Warehouse (Entra ID)  │
│  6. Pipelines read metadata       ──►  Dynamic SQL from metadata    │
│  7. Notebooks process data        ──►  Spark execution in the workspace    │
└─────────────────────────────────────────────────────────────────────┘
```

**Key security properties:**
- **No external attack surface**: No web endpoints, no user forms, no API accepting untrusted input
- **All SQL is internal**: Dynamic SQL values come exclusively from the metadata warehouse, populated by reviewed SQL scripts deployed through CI/CD
- **Authentication is Entra ID + workspace RBAC**: No passwords, no connection strings with credentials, no API keys stored in code
- **CI/CD replaces environment values**: Development-time identifiers in Git are overwritten during deployment via `Deploy-WorkspaceArtifacts-And-ReplaceIds.ps1`

---

## 2. Patterns You Will Encounter (and Why They Are Expected)

The sections below document patterns that static analysis tools and manual reviews commonly flag. For each pattern we explain what it is, why it exists, and what CWEs do and do not apply.

---

### 2.1 GUIDs in Source Files (Workspace IDs, catalog IDs, Notebook IDs)

**Where you'll see this:**
- `workspace_config.json` — Workspace configuration variables
- `*.sqlnotebook-content.py` — notebook metadata headers (e.g., `default_catalog`, `default_catalog_workspace_id`)
- `workspace_cicd/workspace_cicd.py` — deployment script with `INSERT_WORKSPACEID` placeholders
- CI/CD YAML files referencing `$(DEV_WORKSPACE_ID)`, `$(WORKSPACE_ID)`, etc.

**What these are:**

Databricks workspace IDs, catalog IDs, warehouse IDs, and notebook IDs are **structural identifiers**, not secrets. They are the Fabric equivalent of Azure Resource IDs. They:

- Are returned openly by the [workspace REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces) in every call
- Are stored in artifact definitions by [workspace Git integration](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/intro-to-git-integration) by design — this is Microsoft's serialization format
- Grant **zero access** on their own — an attacker who knows a workspace ID still needs Entra ID authentication and Databricks workspace role membership to do anything with it
- Are **replaced per environment** by the CI/CD pipeline (`Deploy-WorkspaceArtifacts-And-ReplaceIds.ps1`), so values in Git are development workspace references only

**Why CWE-200 (Information Disclosure) does not apply:** These IDs are not sensitive. Microsoft does not classify Fabric resource GUIDs as secrets, just as Azure Subscription IDs and Resource Group names are not secrets.

**What about the `INSERT_WORKSPACEID` placeholders?** The file `workspace_cicd/workspace_cicd.py` contains `INSERT_WORKSPACEID`, `INSERT_ENVIRONMENT`, etc. These are deployment-time placeholders — not default credentials. The CI/CD process performs string replacement using values from secured pipeline variable groups. CWE-798 (Hard-coded Credentials) does not apply because these are neither credentials nor hard-coded values — they are template tokens.

> **Recommendation:** Exclude `workspace_config.json`, notebook `# META` headers, and `workspace_cicd.py` from GUID/secret scanning rules. These are platform artifacts with expected content.

---

### 2.2 Dynamic SQL via f-string Interpolation in Spark Notebooks

**Where you'll see this:**
- `helper_functions_1.py.sqlnotebook-content.py` — surrogate key functions, watermark management, Delta table operations
- Patterns like `spark.sql(f"SELECT * FROM {table_name}")` or `spark.sql(f"SELECT {columns} FROM {table} GROUP BY {columns}")`

**What this is:**

The accelerator constructs Spark SQL statements dynamically using f-strings. The interpolated values are **table names, column names, and join conditions** — all sourced from the internal metadata warehouse.

**Why this is not SQL injection (CWE-89):**

1. **No untrusted input.** Every interpolated value originates from the metadata warehouse — a Databricks SQL Warehouse accessible only to data engineers with Entra ID authentication and workspace contributor/admin roles.

2. **Metadata is governed.** Metadata SQL is authored by engineers, goes through pull request review, passes automated validation (via `validate_metadata_sql.py`), and is deployed via CI/CD. There is no path for untrusted input to reach these code paths.

3. **Spark SQL has no parameterized identifier support.** `spark.sql()` does not offer prepared statements for table names, column names, or schema objects — only for literal values. F-string interpolation is the standard PySpark pattern for dynamic identifier references. This is how the [Apache Spark documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html) demonstrates it.

4. **Some flagged lines aren't SQL at all.** Scanners may flag PySpark DataFrame operations (`.select()`, `.withColumn()` using `f.lit()`) or even `return` statements. These are not SQL construction.

> **Recommendation:** When evaluating dynamic SQL findings, trace the data flow to the interpolated variable's origin. If it comes from the metadata warehouse or pipeline parameters (not from HTTP requests, user forms, or external APIs), CWE-89 does not apply.

---

### 2.3 Use of `ast.literal_eval`

**Where you'll see this:**
- `helper_functions_3.py.sqlnotebook-content.py` — schema drift detection

**What this is:**

`ast.literal_eval` is Python's [built-in safe parser](https://docs.python.org/3/library/ast.html#ast.literal_eval) for string representations of Python literals. It is used to parse stored schema metadata like `"[('id', 'int'), ('name', 'string')]"` back into Python objects.

**Why CWE-94 (Code Injection) does not apply:**

`ast.literal_eval` is **not** `eval()`. It can only parse strings, numbers, tuples, lists, dicts, sets, booleans, None, and frozensets. It **cannot** execute function calls, imports, attribute access, operators, or arbitrary code. The Python documentation explicitly recommends it for safely parsing strings. Furthermore, the input comes from the internal metadata warehouse, not from untrusted sources.

> **Recommendation:** Do not flag `ast.literal_eval` as equivalent to `eval()`. They have fundamentally different capabilities. If your scanner conflates them, add an exception for `ast.literal_eval`.

---

### 2.4 Ephemeral OAuth Tokens in CI/CD Pipelines

**Where you'll see this:**
- `.github/workflows/*.yml` and `ado_pipelines/*/release.yml` — patterns like:
  ```powershell
  $access_token = ConvertFrom-SecureString -SecureString (Get-AzAccessToken -AsSecureString -ResourceURL 'https://api.fabric.microsoft.com/').Token -AsPlainText
  ```

**What this is:**

CI/CD pipelines obtain short-lived OAuth2 bearer tokens at runtime via Azure CLI / `Get-AzAccessToken`. The `-AsPlainText` conversion is required because the workspace REST API expects a Bearer token string in the Authorization header.

**Why CWE-522 (Insufficiently Protected Credentials) does not apply:**

- Tokens are **ephemeral** — they expire within minutes/hours
- They are **never committed** to source code — they exist only in pipeline agent memory during execution
- They are **obtained from managed identities or service principals**, not hard-coded
- This is the [Microsoft-recommended approach](https://learn.microsoft.com/en-us/cli/azure/account?view=azure-cli-latest#az-account-get-access-token) for CI/CD automation
- The pipeline agent's execution context is isolated and ephemeral

> **Recommendation:** Do not flag runtime token acquisition via `Get-AzAccessToken` as credential exposure. The token exists only in the pipeline agent's session scope. If credential storage is a concern, verify that no tokens are written to logs or artifacts — in this repo, they are not.

---

### 2.5 TrustServerCertificate in SQL Connection Strings

**Where you'll see this:**
- `Deploy-WorkspaceArtifacts-And-ReplaceIds.ps1` — DACPAC deployment
- `Extract-DacpacFiles.ps1` — DACPAC extraction
- `integration_tests/*/conftest.py` — test fixtures

**What this repo actually uses:**

Every SQL connection string in this repository uses `TrustServerCertificate=False` (the **secure** setting) along with `Encrypt=True`:

```
Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;
```

Authentication uses Entra ID tokens (`/at:$sqlToken`), not SQL passwords. Connections target Databricks SQL Warehouse endpoints, which are TLS-secured Microsoft-managed services.

> **Recommendation:** If your scan flags `TrustServerCertificate`, verify the actual value. `TrustServerCertificate=False` means the client **validates** the server's certificate — this is the secure configuration. Only `TrustServerCertificate=True` would be a concern.

---

### 2.6 Input Validation in Shortcut Processing

**Where you'll see this:**
- `ado_pipelines/Shortcuts/release.yml` — CSV field validation

**What this is:**

The shortcut deployment script validates every required field from CSV configuration files:

```powershell
if ([string]::IsNullOrWhiteSpace($shortcut.SourceWorkspaceId)) {
    throw "Error: SourceWorkspaceId is empty in file $path..."
}
```

This **is** the input validation. The script validates 8+ required fields and throws descriptive errors for missing values. The data source is a CSV file committed via pull request, not external user input.

> **Recommendation:** Review the actual code before filing "Missing Input Validation" findings. If the flagged code contains validation logic (`IsNullOrWhiteSpace` checks, `throw` on invalid data), the finding is a false positive.

---

### 2.7 Deployment Logging in CI/CD Pipelines

**Where you'll see this:**
- `ado_pipelines/Workspace_CICD/release.yml` — deployment progress messages
- Various `Write-Host` and `Write-Output` statements in automation scripts

**What this is:**

CI/CD scripts log operational information: workspace names, artifact names, deployment phases, and success/failure status. This is standard DevOps practice necessary for debugging deployment failures.

**What is NOT logged:** Tokens, passwords, connection strings, or any credentials.

The logs are protected by Azure DevOps / GitHub Actions access controls — only users with pipeline access can view them.

> **Recommendation:** Verify that no credentials or tokens appear in log output. In this repo, only infrastructure metadata (workspace names, artifact names, deployment status) is logged. This is standard operational logging and is an accepted practice.

---

## 3. How Authentication and Access Control Actually Works

Since many findings stem from confusion about where secrets live and how access is controlled, here is a summary:

| Concern | How It's Handled |
|---------|-----------------|
| **User authentication** | Microsoft Entra ID (Azure AD). No passwords in code. |
| **Service-to-service auth** | Managed identities or service principals via Azure CLI. Tokens are ephemeral. |
| **Databricks workspace access** | workspace RBAC roles (Admin, Member, Contributor, Viewer). Knowing a GUID is not enough. |
| **CI/CD secrets** | Stored in Azure DevOps Variable Groups or GitHub Actions Secrets — never in source code. |
| **SQL connections** | Entra ID token auth (`/at:$sqlToken`), `Encrypt=True`, `TrustServerCertificate=False`. |
| **Metadata governance** | SQL scripts go through PR review → automated validation → CI/CD deployment. |
| **Environment isolation** | Dev workspace GUIDs in Git are replaced at deploy time for QA/Prod environments. |

---

## 4. Quick Reference — Common Scanner Findings and Expected Disposition

| Scanner Finding | Typical CWE | Expected Disposition | Reason |
|----------------|-------------|---------------------|--------|
| Hardcoded GUIDs / Workspace IDs | CWE-200 | False Positive | Fabric resource IDs are not secrets. CI/CD replaces per environment. |
| Dynamic SQL via f-strings in Spark | CWE-89 | False Positive | No untrusted input. Metadata-sourced values only. Standard Spark pattern. |
| `ast.literal_eval` usage | CWE-94 | False Positive | Safe parser — not `eval()`. Python docs recommend it. |
| Access tokens in CI/CD scripts | CWE-522 | False Positive | Ephemeral runtime tokens via Azure CLI, not stored credentials. |
| `TrustServerCertificate` keyword | CWE-295 | Verify Value | This repo uses `=False` (secure). Only flag if value is `True`. |
| `INSERT_WORKSPACEID` placeholders | CWE-798 | False Positive | Deployment-time template tokens, not credentials. |
| Missing input validation | CWE-20 | Verify Code | Check if flagged code already contains validation (it does in this repo). |
| Sensitive data in logs | CWE-532 | Accepted Risk | Operational metadata only (workspace names, artifact names). No credentials logged. |

---

## 5. Files and Folders — What to Focus On (and What to Skip)

If you have limited time, here is where real security-relevant code lives versus platform boilerplate:

| Path | What It Contains | Security Relevance |
|------|-----------------|-------------------|
| `automation_scripts/` | CI/CD PowerShell scripts | **Review** — handles token acquisition, deployment, DACPAC publishing |
| `ado_pipelines/` and `.github/workflows/` | Pipeline YAML definitions | **Review** — references service connections, variable groups, deployment logic |
| `workspace_cicd/workspace_cicd.py` | Deployment script with placeholders | **Skip** — template tokens replaced at deploy time |
| `<git_folder>/*.sqlnotebook-content.py` | Spark notebooks (data processing) | **Context-dependent** — dynamic SQL is metadata-driven, not user-driven |
| `<git_folder>/workspace_config.json` | Workspace config | **Skip** — environment-specific IDs, not secrets |
| `<engine_folder>/metadata/` | SQL metadata definitions | **Skip** — data engineer-authored configuration, not executable attack surface |

---

## 6. Questions?

If you have questions about the architecture or need clarification on a specific pattern before filing a finding, please reach out to the Accelerator IP team. We would rather answer questions upfront than dispute false positives after the fact.
