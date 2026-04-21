## Deploy to Databricks

> Last updated: 2026-03-29

Use this file when the question is about initial deployment steps, pre-deployment checks, or how to import the accelerator into a Databricks workspace.

### Pre-Deployment Checklist
1. Decide whether your workspace should use case-sensitive or case-insensitive collation for the Warehouse SQL endpoint, and configure it before running the deployment. [Learn more about Databricks SQL Warehouse collation](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-collation).
2. Select the Databricks deployment pattern that best matches your scale, governance, and security requirements so that the deployment aligns with your operating model. [Review Databricks workspace topology guidance](https://learn.microsoft.com/azure/databricks/admin/workspace/manage-workspaces).

### Steps:
1.  **Import the following notebooks into your Databricks workspace**:
    *   [CreatingArtifacts_Orchestrator.py](../deployment/ImportOnePlatform/CreatingArtifacts_Orchestrator.py)
    *   [CreatingArtifacts.py](../deployment/ImportOnePlatform/CreatingArtifacts.py)
    *   `ImportArtifacts_DONT_OPEN_IN_NOTEBOOK_UI.py` (from the deployment IP package)

    > **Re-deployment tip:** The deployment is idempotent — safe to re-run on existing workspaces. See [FAQ: How do I re-deploy or upgrade?](FAQ.md#how-do-i-re-deploy-or-upgrade-the-accelerator-in-an-existing-workspace) for the full re-deployment process and artifact behavior table.

2.  **Configure Deployment Parameters** (in `CreatingArtifacts_Orchestrator.py`):
    
    Before running the deployment, you can customize the following parameters:

    #### Optional Parameters:
    - **`folder_name`**: Specify a folder name to organize all created resources within each workspace. Leave empty to deploy to the root of the workspace.

    #### catalog and Warehouse Configuration:
    - **`bronze_catalog_name`**: Name for the Bronze catalog (default: `"bronze"`)
    - **`bronze_catalog_workspace_id`**: Workspace ID for Bronze catalog (default: current workspace)
    - **`silver_catalog_name`**: Name for the Silver catalog (default: `"silver"`)
    - **`silver_catalog_workspace_id`**: Workspace ID for Silver catalog (default: current workspace)
    - **`gold_catalog_name`**: Name for the Gold catalog (default: `"gold"`)
    - **`gold_catalog_workspace_id`**: Workspace ID for Gold catalog (default: current workspace)
    - **`metadata_catalog_name`**: Name for the metadata catalog (default: `"metadata_catalog"`)
    - **`metadata_workspace_id`**: Workspace ID for Metadata artifacts (default: current workspace)

    #### Compute and Artifact Workspace Configuration:
    - **`spark_compute_workspace_id`**: Workspace where notebooks will be deployed (default: current workspace)
    - **`pipeline_workspace_id`**: Workspace where data pipelines will be deployed (default: current workspace)
    - **`report_semantic_model_workspace_id`**: Workspace where semantic models and reports will be deployed (default: current workspace)

    **Note:** All workspace IDs are optional. If left empty, they default to the current workspace. This allows for flexible deployment across multiple workspaces or a simplified single-workspace deployment.

    > ⚠️ **CI/CD consideration:** The built-in CI/CD pipeline works best when compute and orchestration artifacts (notebooks, jobs, SQL warehouses, reports) stay in a **single workspace**. Unity Catalog catalogs can live in separate workspaces because they are resolved via `Datastore_Configuration` at runtime — fed from `databricks_batch_engine/datastores/datastore_<ENV>.json` on every `/fdp-04-commit`. See [FAQ: Should I deploy to a single workspace or multiple workspaces?](FAQ.md#should-i-deploy-to-a-single-workspace-or-multiple-workspaces) and [Workspace CI/CD Guide](Workspace_CICD_Guide.md) for details.

3.  **Run IP Deployment Notebook**:
    *   Open and execute all cells in the `CreatingArtifacts_Orchestrator.py` notebook. This will deploy all the necessary catalogs, Warehouses, Pipelines, Notebooks, and other IP artifacts into your workspace(s).
    *   **Execution Time:** ~5 minutes

    The deployment will create all required catalogs, warehouses, notebooks, pipelines, reports, and semantic models. See [FAQ: What artifacts are deployed?](FAQ.md#what-artifacts-are-deployed-to-my-databricks-workspace) for the full inventory.

4. **Set up Git and your target repository**:
   
    After the IP is deployed, connect the deployed workspace(s) to the repository where you will manage metadata and future changes. See [FAQ: How do I set up Git repositories?](FAQ.md#how-do-i-set-up-git-repositories-if-i-deployed-across-multiple-workspaces) and [DevOps Guide - Initial Setup](DevOps.md#1-initial-setup--prerequisites) for detailed instructions.

    **Quick-start:**
    1. Create or choose the Git repository that will hold your deployed workspace assets.
    2. Clone it locally in VS Code. Copy accelerator assets (`.github/copilot-instructions.md`, `docs/`, and optionally `automation_scripts/`, `workspace_cicd/`, CI/CD definitions, `integration_tests/`) into the target repo.
    3. Use one top-level folder per Databricks workspace. Connect each workspace via **Workspace settings → Git integration**.
    4. Metadata SQL notebooks should live under `<workspace-folder>/metadata/metadata_<TriggerName>.sql`.

5. [**Configure Metadata**](METADATA_REFERENCE.md#metadata-overview-and-importance)

## Review Source Code

If you'd like to review the source code for each workspace artifact without deploying the IP, visit the [src directory](../src/).
