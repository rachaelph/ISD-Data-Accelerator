# 🚀 New User Onboarding Guide

> Last updated: 2026-03-29

Welcome to Analytics Transformation with Databricks! Follow these simple steps to get started.

**Repository URL:** https://github.com/mcaps-microsoft/ISD-Data-AI-Platform-Accelerator

---

## Step 1: Clone the Repository

```bash
git clone https://github.com/mcaps-microsoft/ISD-Data-AI-Platform-Accelerator
```

**Or use VS Code's built-in Git:**
1. Press `Ctrl+Shift+P` → type **"Git: Clone"**
2. Paste the repository URL
3. Choose a local folder

---

## Step 2: Open in VS Code

```bash
cd ISD-Data-AI-Platform-Accelerator
code .
```

**Or:**
1. Open VS Code
2. **File** → **Open Folder** → Select the cloned repo folder

> [!CAUTION]
> ### 🚨 Critical: Open as Top-Level Folder
> The repository folder **must be the root/top-level folder** in VS Code — **not nested inside another folder**.
> 
> ✅ **Correct:** `File → Open Folder → ISD-Data-AI-Platform-Accelerator`  
> ❌ **Wrong:** Opening a parent folder that contains the repo as a subfolder
> 
> **Why?** GitHub Copilot only loads the custom instructions (`.github/copilot-instructions.md`) when the repo is the workspace root. Without this, Copilot won't have full context of the accelerator.

> [!IMPORTANT]
> **Local repo clone is not enough for AI execution.** If you want Copilot to help **commit, sync, and execute** accelerator changes, your **Databricks workspace must also be connected to the same Git repository and the correct workspace folder** in **Databricks workspace settings → Git integration**.

---

## Step 3: Start Asking Questions and Run Workflows with GitHub Copilot

> ✨ **Zero setup required!** No build steps, package installs, or API keys needed.

1. Open **GitHub Copilot Chat** (`Ctrl+Shift+I` or click the Copilot icon in the sidebar)
2. Use the repo's **`/commands` first** instead of free-form chat. They load the right workflow, context, and validation steps automatically.

💡 **Tip:** Use **Claude Opus 4.6** for the best experience (select it from the model dropdown in Copilot Chat).

> [!IMPORTANT]
> **Do not start with free-form prompts when a `/command` exists.** The command-based workflows are more efficient, use the repo's built-in instructions and skills, and generally produce better outcomes.

### What the AI can do for you

GitHub Copilot is not just for explanations. In this repo, it can help you **operate the accelerator end-to-end**:

- **Generate metadata** for Bronze, Silver, and Gold using natural language
- **Validate metadata** before deployment so configuration issues are caught early
- **Execute the accelerator** by committing changes, syncing the Databricks workspace, deploying metadata SQL, and running the right notebook or pipeline
- **Troubleshoot failures** by reading logs, checking metadata, and suggesting fixes
- **Investigate outcomes** by checking table health, DQ, lineage, profiling, and recent runs

For AI-driven execution workflows such as `/fdp-04-commit`, `/fdp-05-run`, `/fdp-06-investigate`, and `/fdp-06-troubleshoot`, you need **all** of the following:

1. A **local clone** of this repository open in VS Code
2. A **Databricks workspace connected to Git** and pointed at the correct repo folder
3. **Az PowerShell available locally and an active sign-in** via `Connect-AzAccount` — Copilot checks this automatically before script-backed workflows and can install the required module when it is missing

> [!IMPORTANT]
> **Execute in DEV or a feature workspace first.** Use AI to test safely, but keep metadata in Git and avoid making direct production changes.

### 💬 Recommended Starter Commands

| Category | Example Prompts |
|----------|-----------------|
| **End-to-End Planner** | *"/fdp-00-execute — Add a freshness rule to the customer table, deploy it, run it, and show me whether it worked."* |
| **Orientation** | *"/fdp-01-onboarding — Help me understand the accelerator, how AI works in this repo, and what I should do first."* |
| **Deployment** | *"/fdp-02-deploy — Help me deploy the accelerator to my Databricks workspace and verify that it worked."* |
| **Building Pipelines** | *"/fdp-03-author — Create metadata for my customer table from SQL Server into Bronze with incremental loading on modified_date."* |
| | *"/fdp-03-author — Add a Silver transformation for customer data: snake_case rename, deduplicate by primary key, remove nulls."* |
| | *"/fdp-03-author — Add a Gold fact table with surrogate keys."* |
| | *"/fdp-03-author — Create the metadata and custom PySpark notebook needed for this enrichment logic that built-in transformations cannot handle."* |
| **AI Execution** | *"/fdp-03-author — Create metadata for my customer table from SQL Server into Bronze with incremental loading on modified_date"* |
| | *"/fdp-04-commit — Commit my current changes, sync my dev workspace, and deploy them"* |
| | *"/fdp-05-run — Run Table_ID 101 in my dev workspace"* |
| | *"/fdp-05-run — Run trigger SalesDataProduct starting from step 2 in my dev workspace"* |
| | *"/fdp-06-troubleshoot — My pipeline failed with this error: [paste error]"* |
| **Investigating Data** | *"/fdp-06-investigate — Run a health check and data summary for my customer table"* |
| | *"/fdp-06-investigate — run a full health check on Table_ID 101"* |
| **Troubleshooting** | *"/fdp-06-troubleshoot — Investigate why my pipeline failed"* |
| | *"/fdp-06-troubleshoot — Help me debug this Spark error: [paste error]"* |

> 💡 **Best practice:** Start with the closest `/command`, then keep iterating in the same chat thread.

---

## Step 4: Use AI to Execute the Accelerator

Once your workspace is deployed, the fastest path is to let GitHub Copilot guide the whole flow:

If your goal spans multiple stages, start with `/fdp-00-execute`. It resolves a deterministic stage plan from your request and chains authoring, validation, commit, run, and investigation automatically when that is the right governed path.

> [!IMPORTANT]
> Before using `/fdp-04-commit`, `/fdp-05-run`, `/fdp-06-investigate`, or `/fdp-06-troubleshoot`, confirm your Databricks workspace is connected to the repository and that Az PowerShell is installed with an active sign-in via `Connect-AzAccount`. The workflow depends on workspace Git integration plus local Azure auth before Copilot can run the relevant scripts.

1. **Describe what you want to build**
	- Start with `/fdp-03-author` or `/fdp-03-convert` instead of plain chat

2. **Let Copilot generate or edit the metadata**
	- Use `/fdp-03-author` for new pipelines or editing existing tables

3. **Ask Copilot to execute it**
	- Use `/fdp-04-commit` to:
	  - commit and push your local changes
	  - sync the Databricks workspace with Git
	  - deploy changed metadata SQL directly to the metadata warehouse
	- Use `/fdp-05-run` to:
	  - run either a single table or a full trigger
	  - no Git operations needed — just execution

4. **If anything fails, stay in chat and iterate**
	- Use `/fdp-06-troubleshoot` to diagnose the failure
	- Use `/fdp-06-investigate` to inspect the resulting table, DQ, lineage, profiling, and logs

### Recommended first execution flow

For a brand-new user, this is the simplest end-to-end pattern:

1. Deploy the accelerator to a **dev** workspace
2. Connect that **dev workspace** to the **same Git repo and correct workspace folder**
3. Ask Copilot to generate metadata for **one small table**
4. Ask Copilot to validate and execute that table in the dev workspace
5. Confirm the run succeeded in logs and that data landed in Bronze/Silver/Gold as expected
6. Expand to more tables only after the first table works cleanly

### Example end-to-end prompts

```text
/fdp-00-execute
Add a freshness rule for the Customers table, deploy it to my dev workspace, run it, and show me whether it worked.
```

```text
/fdp-03-author
Create metadata to ingest the dbo.Customers table from SQL Server into Bronze.
Primary key: customer_id
Use incremental loading on modified_date.
Trigger name: SalesDataProduct
```

```text
/fdp-04-commit
Commit my current changes, sync my dev workspace, and deploy them.
```

```text
/fdp-05-run
Run the Customers table in my dev workspace.
```

```text
/fdp-06-troubleshoot
The execution failed. Investigate the latest error and tell me exactly what to fix.
```

---

## 📚 Key Documentation

| Document | Description |
|----------|-------------|
| README.md | Project overview and quick start |
| docs/FAQ.md | Frequently asked questions |
| docs/How_AI_Is_Embedded.md | How GitHub Copilot is built into the accelerator workflow |
| docs/METADATA_REFERENCE.md | Metadata contract, validation, and overview |
| docs/Deployment_Guide.md | Step-by-step deployment (~5 mins) |
| docs/Video_Training.md | Role-based video tutorials |
| docs/presentation/ | Visual slide deck (open HTML in browser) |

---

## ✅ Checklist for New Users

- [ ] Clone the repository
- [ ] Open in VS Code
- [ ] Open GitHub Copilot Chat
- [ ] Run `/fdp-01-onboarding`
- [ ] Use `/fdp-02-deploy` to walk through deployment prerequisites
- [ ] Connect your Databricks dev workspace to the same Git repo and correct folder
- [ ] Use `/fdp-03-author` for one small test table
- [ ] Use `/fdp-04-commit` to deploy changes to a dev workspace
- [ ] Use `/fdp-05-run` to execute in a dev workspace
- [ ] If the run fails, use `/fdp-06-troubleshoot`
- [ ] Review the Slide Deck for visual overview
- [ ] Watch relevant Video Training for your role

---

## 🗓️ Your First Week with Copilot

Once you're set up, use Copilot as your guide to ramp up. This isn't a rigid schedule — move at your own pace, skip what you already know, and spend more time where it matters for your role.

### Day 1: Orientation — Understand the Big Picture

Start by getting the mental model. Use `/fdp-01-onboarding` in Copilot Chat:

```text
/fdp-01-onboarding
Give me a high-level overview of the accelerator, the medallion architecture,
and what happens when a pipeline runs end-to-end.
```

**By end of day:** You should be able to explain Bronze → Silver → Gold in your own words, and know that everything is driven by metadata (not hardcoded pipelines).

---

### Day 2: Deployment — Get It Running

Deploy to a Databricks workspace so you have something real to interact with:

```text
/fdp-02-deploy
Walk me through deployment prerequisites, how to deploy to my Databricks workspace,
how to connect the workspace to Git, and how to verify the deployment succeeded.
```

**By end of day:** You have a working deployment with the metadata warehouse, catalogs, notebooks, and pipelines visible in your Databricks workspace.

---

### Day 3: Your First Pipeline — Learn by Doing

Build something simple end-to-end. Pick a source you have access to and use:

```text
/fdp-03-author
Create metadata to ingest [your table] from [your database] into Bronze.
Primary key: [your key]
Connection ID: [your connection]
```

Then execute it with AI and watch what happens. Follow up with:

```text
/fdp-04-commit — Commit my current changes, sync my dev workspace, and deploy them.
```

Then run it:

```text
/fdp-05-run — Run this table in my dev workspace.
```

Then refine it with:

```text
/fdp-03-author
Add a Silver transformation that renames columns to snake_case,
removes nulls, and deduplicates by primary key.
```

Then inspect the outcome with:

```text
/fdp-06-investigate
Show me whether this table succeeded, where the data landed,
and a summary of the latest run.
```

**By end of day:** You've ingested real data into Bronze and transformed it into Silver. You understand the feedback loop: write metadata → run pipeline → check results.

---

### Day 4: Go Deeper — Transformations & Data Quality

Now layer on more capability using structured commands:

```text
/fdp-03-author
Summarize the built-in transformation options available for this table,
then add the right metadata changes.
```

```text
/fdp-03-author
Add data quality checks to my Silver table — validate email format,
check that department_id exists in the reference table, quarantine bad records.
```

```text
/fdp-03-author
Create a Gold aggregation table that summarizes [your metric] by [your dimensions].
```

```text
/fdp-03-author
Recommend whether I should use merge, overwrite, or merge_and_delete for this table,
and then apply that choice in metadata.
```

**By end of day:** You can build multi-step pipelines with quality gates, and you know when to use built-in transformations vs when something needs custom code.

---

### Day 5: Real-World Scenarios

Apply what you've learned to your actual project needs:

```text
/fdp-03-convert
I have an existing [stored procedure / Python script / ADF pipeline] that does X.
Convert it to accelerator metadata:
[paste your code]
```

```text
/fdp-99-maintain
I need to extend, upgrade, or fix the accelerator itself for [describe the unusual requirement].
Recommend the right internal changes and implement them safely.
```

```text
/fdp-03-author
Set up incremental loading so I'm not reprocessing everything every run.
```

```text
/fdp-03-author
Handle [your specific challenge — slowly changing dimensions,
multi-source dedup, file ingestion from SFTP, etc.].
```

**By end of day:** You have a plan (or working prototype) for your real project, built on top of the accelerator.

---

### Ongoing: Commands You'll Use Along the Way

These come up naturally as you work. Start with the closest command:

| When You're... | Use This Command |
|----------------|-------------------|
| Debugging a failure | *"/fdp-06-troubleshoot — My pipeline failed with this error: [paste error]. Investigate it."* |
| Ready to deploy your changes | *"/fdp-04-commit — Commit my latest changes, sync dev, and deploy them"* |
| Ready to run your changes | *"/fdp-05-run — Run this trigger in my dev workspace"* |
| Unsure about a feature | *"/fdp-01-onboarding — Explain how [feature] fits into the accelerator and what I should use."* |
| Optimizing performance | *"/fdp-03-author — Add liquid clustering if appropriate and explain why."* |
| Setting up CI/CD | *"/fdp-02-feature — Help me set up an isolated branch and workspace for development."* |
| Working with a team | *"/fdp-02-feature — Show me how to work in this repo without conflicting with other developers."* |
| Extending the solution | *"/fdp-99-maintain — I need to do [something unusual]. Implement the right accelerator changes."* |

---

### 💡 Tips for Learning Faster

1. **Use the closest `/command` first** — The structured workflows are faster, load the right context automatically, and produce better results than free-form prompts.

2. **Keep iterating in the same chat thread** — Build on previous steps. For example: *"/fdp-03-author — Now add SCD Type 2 to that dimension table we just created."*

3. **Paste errors directly into the troubleshooting command** — When something fails, include the full error message in `/fdp-06-troubleshoot`.

4. **Start small, iterate** — Don't try to build a 10-table pipeline on day one. Get one table working end-to-end, then expand. See the [Metadata Generation Guide - Writing Effective Prompts](METADATA_GENERATION_GUIDE.md#writing-effective-prompts-for-metadata-generation) for how to iterate.

5. **Always test in a DEV/QA workspace first** — Metadata controls real pipelines that write to real tables. A misconfigured `overwrite` or `merge_and_delete` can wipe target data, and running against the wrong workspace can affect production. Use negative or test `Table_ID`s, verify your workspace IDs, and start with small tables before scaling up. If something goes wrong, you can delete the metadata and try again — but the data changes it triggered may not be reversible.
