<div align="center">

# Analytics Transformation with Fabric

### Your End-to-End Data Engineering Copilot

<br/>

[![Watch the Demo Video](https://img.shields.io/badge/%E2%96%B6%20Watch%20the%20Demo%20Video-FFB000?style=for-the-badge&logo=microsoftstream&logoColor=white)](https://microsoft.sharepoint.com/:v:/s/UnifyyourIntelligentDatawithFabricCatalogExtension/cQoFHtg9xEhOQbU1cfrIEiJcEgUCz4vsMcytzOQrQazoitbasQ?nav=eyJyZWZlcnJhbEluZm8iOnsicmVmZXJyYWxBcHAiOiJTdHJlYW1XZWJBcHAiLCJyZWZlcnJhbFZpZXciOiJTaGFyZURpYWxvZy1MaW5rIiwicmVmZXJyYWxBcHBQbGF0Zm9ybSI6IldlYiIsInJlZmVycmFsTW9kZSI6InZpZXcifX0=&)

**See the accelerator in action from authoring to execution.**

<br/>

[👋 New User Onboarding](docs/New_User_Onboarding.md) · [🚀 Deployment Guide](docs/Deployment_Guide.md) · [📊 Slide Deck](docs/presentation/)

</div>

---

Data engineering is overdue for a better interface.

This repository reimagines the engineering workflow with AI coding agents so teams can author, modify, troubleshoot, investigate, and deploy Fabric data solutions by expressing intent in natural language.

What makes that viable is not AI alone. It is the governed framework underneath the agent experience: the metadata model, implementation patterns, validation rules, deployment automation, and operational guardrails that make the workflow repeatable at enterprise scale.

Under the hood, the platform turns those requests into governed metadata, pipelines, notebooks, tests, and deployment automation that can scale across enterprise teams.

<a id="quick-start"></a>

## ⚡ Quick Start with GitHub Copilot

Open this repository in VS Code and use the pre-built `/commands` below as your starting point. Each one launches a guided Copilot workflow — just type the command and describe what you need.

When your goal spans multiple stages, start with `/fdp-00-execute`. It uses the machine-readable workflow, query, and metadata contracts under `.github/contracts/` plus the deterministic planner in `automation_scripts/resolve_agent_plan.py` to chain the right downstream workflows with approvals only where needed.

### `/commands`

| Command | What it does |
|:--------|:-------------|
| `/fdp-00-execute` | Plan and execute an end-to-end workflow from one intent across authoring, validation, deployment, execution, and investigation |
| `/fdp-01-onboarding` | Get started with the accelerator |
| `/fdp-02-deploy` | Deploy the accelerator IP into a Databricks workspace for initial setup |
| `/fdp-02-feature` | Set up an isolated feature branch and workspace |
| `/fdp-03-author` | Create or modify metadata, entities, DQ rules, transformations, and custom Python/PySpark functions |
| `/fdp-03-convert` | Convert existing SQL/Python/ETL code into accelerator metadata |
| `/fdp-04-commit` | Commit, push, sync workspace, and deploy metadata changes |
| `/fdp-05-run` | Execute a pipeline or notebook run |
| `/fdp-06-investigate` | Check table health, run history, DQ, schema, lineage, live data |
| `/fdp-06-troubleshoot` | Diagnose pipeline or notebook failures |
| `/fdp-99-maintain` | Maintain core accelerator internals (notebooks, pipelines, SPs) |
| `/fdp-99-audit` | Review documentation for gaps or outdated content |
| `/fdp-99-changelog` | Generate or update the changelog from recent commits |
| `/fdp-99-prompt` | Maintain prompt and customization files |

<details>
<summary><b>✨ Zero Setup Required</b> — click to expand</summary>

<br/>

After you clone or open the repository in VS Code, you can immediately start asking questions about the accelerator and its workflows. There is no build step, package installation, environment variable setup, API key exchange, or custom bootstrap required to begin exploring the repository through Copilot.

The assistant can work purely from the local markdown, scripts, and notebooks at first. You'll only need Fabric access when you move from design and investigation into deployment or execution.

> 💡 **Tip:** Use **GPT 5.4** for the best experience.

</details>

---

## 📋 Table of Contents

<table>
<tr>
<td>

- [⚡ Quick Start with Copilot](#quick-start)
- [🏗️ Architecture](#architecture)
- [🤖 How Copilot Becomes the Engineering Interface](#how-copilot-becomes-the-engineering-interface)

</td>
<td>

- [🏭 Production Readiness](#production-readiness)

</td>
</tr>
</table>

---

<a id="architecture"></a>

## 🏗️ Architecture

The accelerator combines a Copilot-first authoring experience with a Fabric-native execution model.

The platform architecture defines how intent, metadata, pipelines, and workspaces fit together. The authoring foundation shows how governed implementation patterns are created and maintained.

---

<a id="how-copilot-becomes-the-engineering-interface"></a>

## 🤖 How Copilot Becomes the Engineering Interface

Copilot is not an add-on to this repository. It is the primary way engineers interact with it.

The framework is what makes that safe to scale. Without it, agent-driven development collapses into one-off code generation and inconsistent operational behavior.

In practice, that means you can move from intent to implementation through guided workflows that:

- translate requests into reusable metadata and transformation patterns
- connect authoring with testing, deployment, and execution paths
- preserve governance, repeatability, and CI/CD discipline
- let engineers investigate and troubleshoot the live system without losing architectural context



---

<a id="production-readiness"></a>

## 🏭 Production Readiness

This is not a proof of concept. The accelerator ships with the depth required to run governed data platforms in production.

<table>
<tr>
<td width="33%" align="center">
<h3>🧪 875+ Tests</h3>
<sub>Unit, integration, and stored-procedure tests across notebooks, pipelines, and metadata validation</sub>
</td>
<td width="33%" align="center">
<h3>🚀 CI/CD Pipelines</h3>
<sub>Multi-environment deployment automation with workspace provisioning, artifact sync, and rollback</sub>
</td>
<td width="33%" align="center">
<h3>✅ Data Quality Engine</h3>
<sub>Pattern validation, FK checks, custom rules, quarantine tables, and DQ dashboards</sub>
</td>
</tr>
<tr>
<td width="33%" align="center">
<h3>📊 Operational Monitoring</h3>
<sub>Real-time run history, schema drift detection, lineage tracking, alerts, and audit trails</sub>
</td>
<td width="33%" align="center">
<h3>🔄 Custom Code Integration</h3>
<sub>Extend any pipeline stage with custom Python, PySpark, or SQL — governed by the same metadata and testing framework</sub>
</td>
<td width="33%" align="center">
<h3>🔌 Broad Connectivity</h3>
<sub>Oracle, SQL Server, PostgreSQL, MySQL, DB2, APIs, SFTP, CSV, JSON, Parquet, Excel, XML</sub>
</td>
</tr>
<tr>
<td width="33%" align="center">
<h3>🏗️ Medallion Architecture</h3>
<sub>Bronze → Silver → Gold with automatic schema management and watermark-based incremental loads</sub>
</td>
<td width="33%" align="center">
<h3>🔐 Governance</h3>
<sub>Metadata-driven controls, access policies, managed private endpoints, data access rules</sub>
</td>
<td width="33%" align="center">
<h3>👥 Multi-Role</h3>
<sub>Built for data engineers, DevOps, architects, and analytics teams to collaborate through shared patterns</sub>
</td>
</tr>
</table>