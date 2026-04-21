# Frequently Asked Questions (FAQ)

> Last updated: 2026-03-29

## Table of Contents

- [General Questions](#general-questions)
  - [What is this repository and how do I get started?](#what-is-this-repository-and-how-do-i-get-started)
  - [What data sources does the solution support?](#what-data-sources-does-the-solution-support)
  - [How do I deploy the IP (Intellectual Property)?](#how-do-i-deploy-the-ip-intellectual-property)
  - [How do I re-deploy or upgrade the accelerator in an existing workspace?](#how-do-i-re-deploy-or-upgrade-the-accelerator-in-an-existing-workspace)
  - [What is the medallion architecture?](#what-is-the-medallion-architecture)
  - [What are the rules for each medallion layer?](#what-are-the-rules-for-each-medallion-layer)
- [Understanding the Deployment](#understanding-the-deployment)
  - [What artifacts are deployed to my Databricks workspace?](#what-artifacts-are-deployed-to-my-databricks-workspace)
  - [What pipelines are deployed and what do they do?](#what-pipelines-are-deployed-and-what-do-they-do)
  - [How does data ingestion work in this solution?](#how-does-data-ingestion-work-in-this-solution)
  - [What notebooks are deployed and what are they used for?](#what-notebooks-are-deployed-and-what-are-they-used-for)
  - [What stored procedures exist and what does each one do?](#what-stored-procedures-exist-and-what-does-each-one-do)
  - [Why does the solution use notebooks instead of wheel files?](#why-does-the-solution-use-notebooks-instead-of-wheel-files)
  - [How do I run a data pipeline after deployment?](#how-do-i-run-a-data-pipeline-after-deployment)
  - [How do I trace data lineage and monitor pipeline execution?](#how-do-i-trace-data-lineage-and-monitor-pipeline-execution)
- [Metadata Configuration](#metadata-configuration)
  - [How do I create metadata configurations?](#how-do-i-create-metadata-configurations)
  - [How does the accelerator help with migration to Databricks?](#how-does-the-accelerator-help-with-migration-to-databricks)
  - [How do I convert existing SQL/ETL code to accelerator metadata?](#how-do-i-convert-existing-sqletl-code-to-accelerator-metadata)
  - [How do generated metadata SQL notebooks integrate with Databricks and Git?](#how-do-generated-metadata-sql-files-integrate-with-git)
  - [Where can I find examples of complex transformation patterns?](#where-can-i-find-examples-of-complex-transformation-patterns)
  - [What configuration categories are available?](#what-configuration-categories-are-available)
  - [What default configuration values does the accelerator apply and why?](#what-default-configuration-values-does-the-accelerator-apply-and-why)
  - [How do I validate my metadata before execution?](#how-do-i-validate-my-metadata-before-execution)
  - [How should I organize my metadata SQL notebooks?](#how-should-i-organize-my-metadata-sql-notebooks)
  - [Can I use subfolders to organize metadata SQL notebooks?](#can-i-use-subfolders-to-organize-metadata-sql-notebooks)
  - [Can I use custom Python or PySpark functions for data processing?](#can-i-use-custom-python-or-pyspark-functions-for-data-processing)
  - [Where do I store custom function notebooks?](#where-do-i-store-custom-function-notebooks)
  - [How do I share reusable code across multiple custom notebooks?](#how-do-i-share-reusable-code-across-multiple-custom-notebooks)
  - [Why must custom notebooks place imports inside functions?](#why-must-custom-notebooks-place-imports-inside-functions)
  - [When should I use built-in transformations vs custom functions?](#when-should-i-use-built-in-transformations-vs-custom-functions)
  - [What parameters are passed to custom functions, and how do I debug failures?](#what-parameters-are-passed-to-custom-functions-and-how-do-i-debug-failures)
  - [How do I find my connection ID?](#how-do-i-find-my-connection-id)
  - [How do I customize Spark cluster size or add external libraries?](#how-do-i-customize-spark-cluster-size-or-add-external-libraries)
  - [How do I add a custom Python library that only certain tables need?](#how-do-i-add-a-custom-python-library-that-only-certain-tables-need)
   - [What's the difference between process, pipeline_stage_and_batch, and pipeline_stage_only?](#whats-the-difference-between-process-pipeline_stage_and_batch-and-pipeline_stage_only)
  - [How do I change between Bronze, Silver, and Gold catalogs for my target?](#how-do-i-change-between-bronze-silver-and-gold-catalogs-for-my-target)
  - [Can I have multiple triggers running at the same time?](#can-i-have-multiple-triggers-running-at-the-same-time)
  - [What if my source data doesn't have a primary key?](#what-if-my-source-data-doesnt-have-a-primary-key)
  - [What's the difference between merge types for target tables?](#whats-the-difference-between-merge-types-for-target-tables)
  - [When should I use liquid clustering?](#when-should-i-use-liquid-clustering)
  - [What are the best practices for creating custom transformation functions?](#what-are-the-best-practices-for-creating-custom-transformation-functions)
  - [What are best practices for metadata validation before deployment?](#what-are-best-practices-for-metadata-validation-before-deployment)
  - [What are best practices for managing metadata in Git?](#what-are-best-practices-for-managing-metadata-in-git)
  - [How do I add a new catalog or warehouse as a target datastore?](#how-do-i-add-a-new-catalog-or-warehouse-as-a-target-datastore)
  - [How do I ingest a Delta table or mirrored database table from another workspace with Change Data Feed?](#how-do-i-ingest-a-delta-table-or-mirrored-database-table-from-another-workspace-with-change-data-feed)
  - [What logging best practices should I follow?](#what-logging-best-practices-should-i-follow)
- [DevOps and CI/CD](#devops-and-cicd)
  - [How do I set up CI/CD pipelines?](#how-do-i-set-up-cicd-pipelines)
  - [What IDs does the Workspace CI/CD pipeline replace automatically?](#what-ids-does-the-workspace-cicd-pipeline-replace-automatically)
  - [What documentation do I update when modifying the accelerator?](#what-documentation-do-i-update-when-modifying-the-accelerator)
  - [What variables do I need to configure for CI/CD pipelines?](#what-variables-do-i-need-to-configure-for-cicd-pipelines)
  - [What's the difference between the DEV, QA/UAT, and PROD workspace CI/CD pipelines?](#whats-the-difference-between-the-dev-qauat-and-prod-workspace-cicd-pipelines)
  - [What integration tests are run during CI/CD?](#what-integration-tests-are-run-during-cicd)
  - [Should I run integration tests in production?](#should-i-run-integration-tests-in-production)
  - [Why are my integration tests timing out in the release pipeline?](#why-are-my-integration-tests-timing-out-in-the-release-pipeline)
  - [How do I enable a multi-developer environment?](#how-do-i-enable-a-multi-developer-environment)
  - [Why isn't feature workspace deployment fully automated?](#why-isnt-feature-workspace-deployment-fully-automated)
  - [Should I deploy to a single workspace or multiple workspaces?](#should-i-deploy-to-a-single-workspace-or-multiple-workspaces)
  - [How do I set up Git repositories if I deployed across multiple workspaces?](#how-do-i-set-up-git-repositories-if-i-deployed-across-multiple-workspaces)
  - [How do I manage metadata table records in Git?](#how-do-i-manage-metadata-table-records-in-git)
  - [How do I trigger full data reloads?](#how-do-i-trigger-full-data-reloads)
  - [How do I test metadata changes in my feature workspace before promoting to dev?](#how-do-i-test-metadata-changes-in-my-feature-workspace-before-promoting-to-dev)
  - [What happens if two developers modify the same Table_ID?](#what-happens-if-two-developers-modify-the-same-table_id)
  - [How do I revert a metadata change that was deployed to production?](#how-do-i-revert-a-metadata-change-that-was-deployed-to-production)
  - [How do I deploy volume shortcuts via CI/CD?](#how-do-i-deploy-volume-shortcuts-via-cicd)
  - [How does file change detection work in CI/CD pipelines?](#how-does-file-change-detection-work-in-cicd-pipelines)
  - [What naming conventions are used in CI/CD pipelines?](#what-naming-conventions-are-used-in-cicd-pipelines)
  - [How do I generalize the CI/CD pipeline to deploy multiple workspaces?](#how-do-i-generalize-the-cicd-pipeline-to-deploy-multiple-workspaces)
- [Data Engineering](#data-engineering)
  - [How do I ingest data from an Oracle database?](#how-do-i-ingest-data-from-an-oracle-database)
   - [How do I fix Oracle ParquetInvalidDecimalPrecisionScale errors (Precision 38 Scale 127)?](#how-do-i-fix-oracle-parquetinvaliddecimalprecisionscale-errors-precision-38-scale-127)
  - [How does incremental loading work?](#how-does-incremental-loading-work)
  - [How do I handle file ingestion from the volume?](#how-do-i-handle-file-ingestion-from-the-volume)
  - [How does incremental copy work when ingesting files?](#how-does-incremental-copy-work-when-ingesting-files)
  - [What file formats are supported and what options are available?](#what-file-formats-are-supported-and-what-options-are-available)
  - [How does batch vs. sequential file reading work?](#how-does-batch-vs-sequential-file-reading-work)
  - [What is Change Data Feed and when should I use it?](#what-is-change-data-feed-and-when-should-i-use-it)
  - [How do I enable Change Data Feed on my Delta tables?](#how-do-i-enable-change-data-feed-on-my-delta-tables)
  - [How do I write files to date-partitioned folders (YYYY/MM/DD)?](#how-do-i-write-files-to-date-partitioned-folders-yyyymmdd)
  - [What system columns does the accelerator create automatically?](#what-system-columns-does-the-accelerator-create-automatically)
  - [What built-in transformations are available?](#what-built-in-transformations-are-available)
  - [How does column name cleansing work?](#how-does-column-name-cleansing-work)
  - [How do I create a dimension table with SCD Type 2?](#how-do-i-create-a-dimension-table-with-scd-type-2)
  - [How does SCD Type 2 change detection work?](#how-does-scd-type-2-change-detection-work)
  - [How do I build a fact table with surrogate keys?](#how-do-i-build-a-fact-table-with-surrogate-keys)
  - [What data quality checks are available?](#what-data-quality-checks-are-available)
  - [How do I configure data quality check actions (warn, quarantine, fail)?](#how-do-i-configure-data-quality-check-actions-warn-quarantine-fail)
  - [How are data quality checks executed?](#how-are-data-quality-checks-executed)
  - [How do I reload data after an error?](#how-do-i-reload-data-after-an-error)
  - [Can I write to multiple target tables from one source?](#can-i-write-to-multiple-target-tables-from-one-source)
  - [How do I handle late-arriving data with watermarks?](#how-do-i-handle-late-arriving-data-with-watermarks)
  - [Can I schedule pipelines to run at specific times?](#can-i-schedule-pipelines-to-run-at-specific-times)
  - [I have multiple triggers that need to run in a specific order - what's the best way to do that?](#i-have-multiple-triggers-that-need-to-run-in-a-specific-order---whats-the-best-way-to-do-that)
  - [Two of my triggers need the same source table - is that going to cause problems?](#two-of-my-triggers-need-the-same-source-table---is-that-going-to-cause-problems)
  - [How should I design triggers to avoid unnecessary waiting between silver and gold?](#how-should-i-design-triggers-to-avoid-unnecessary-waiting-between-silver-and-gold)
  - [We're thinking about using a Warehouse for Gold instead of Catalog - anything we should know?](#were-thinking-about-using-a-warehouse-for-gold-instead-of-catalog---anything-we-should-know)
  - [Can I write to multiple Warehouses? Different teams want their own.](#can-i-write-to-multiple-warehouses-different-teams-want-their-own)
  - [How do I optimize pipeline performance for ingesting large tables from external sources?](#how-do-i-optimize-pipeline-performance-for-ingesting-large-tables-from-external-sources)
  - [How do I enable source partitioning for external database ingestion?](#how-do-i-enable-source-partitioning-for-external-database-ingestion)
  - [How do I add a new ingestion source type?](#how-do-i-add-a-new-ingestion-source-type)
  - [What Spark optimizations does the accelerator apply automatically?](#what-spark-optimizations-does-the-accelerator-apply-automatically)
  - [What is the Metadata_Files tracking table?](#what-is-the-metadata_files-tracking-table)
  - [Should Bronze layer use raw files or Delta tables?](#should-bronze-layer-use-raw-files-or-delta-tables)
  - [How does the accelerator work with Databricks high-concurrency mode?](#how-does-the-accelerator-work-with-high-concurrency-mode)
- [Troubleshooting](#troubleshooting)
  - [How do I restart a pipeline after a mid-run failure?](#how-do-i-restart-a-pipeline-after-a-mid-run-failure)
  - [Where can I see why my pipeline failed?](#where-can-i-see-why-my-pipeline-failed)
  - [Troubleshooting pipeline and notebook failures](#troubleshooting-pipeline-and-notebook-failures)
  - [What does "quarantine" mean and how do I review quarantined records?](#what-does-quarantine-mean-and-how-do-i-review-quarantined-records)
  - [How do I fix schema mismatch errors?](#how-do-i-fix-schema-mismatch-errors)
  - [Why are my values becoming NULL during file ingestion?](#why-are-my-values-becoming-null-during-file-ingestion)
  - [My custom file ingestion function can't read files (FileNotFoundError or path errors)](#my-custom-file-ingestion-function-cant-read-files-filenotfounderror-or-path-errors)
  - [What does "FIFO_Status" mean in the logs?](#what-does-fifo_status-mean-in-the-logs)
  - [How do I know if my data quality checks are working?](#how-do-i-know-if-my-data-quality-checks-are-working)
  - [I'm getting "SQL pool is warming up" errors. What should I do?](#im-getting-sql-pool-is-warming-up-errors-what-should-i-do)
  - [Where can I find logs and monitoring?](#where-can-i-find-logs-and-monitoring)
  - [How do I write data to a different workspace?](#how-do-i-write-data-to-a-different-workspace)
  - [Why is delta__created_datetime or delta__modified_datetime missing from my target table?](#why-is-delta__created_datetime-or-delta__modified_datetime-missing-from-my-target-table)
  - [Why is my Databricks compute cluster not working? (custom libraries not loading, compute settings ignored)](#why-is-my-databricks-compute-cluster-not-working-custom-libraries-not-loading-compute-settings-ignored)
- [Pre-Built Reports](#pre-built-reports)
  - [What reports are included in the solution?](#what-reports-are-included-in-the-solution)
  - [How do I access the pre-built reports?](#how-do-i-access-the-pre-built-reports)
  - [What data sources do the reports use?](#what-data-sources-do-the-reports-use)
  - [How do I troubleshoot pipeline failures using the reports?](#how-do-i-troubleshoot-pipeline-failures-using-the-reports)
  - [How do I use the Exploratory Data Analysis report?](#how-do-i-use-the-exploratory-data-analysis-report)
  - [How is exploratory data analysis controlled?](#how-is-exploratory-data-analysis-controlled)
  - [How do I use the Data Lineage report?](#how-do-i-use-the-data-lineage-report)
  - [Can I customize the pre-built reports?](#can-i-customize-the-pre-built-reports)
  - [How often is the report data refreshed?](#how-often-is-the-report-data-refreshed)
  - [What if I don't see any data in the reports?](#what-if-i-dont-see-any-data-in-the-reports)
  - [How do I monitor data quality across all pipelines?](#how-do-i-monitor-data-quality-across-all-pipelines)
  - [Where can I find the report source files?](#where-can-i-find-the-report-source-files)
- [Quick Reference Links](#quick-reference-links)

---

## General Questions

### What is this repository and how do I get started?
**Keywords:** introduction, overview, getting started, setup, deployment, what is this, accelerator, framework, solution, beginning, first steps, onboarding

**Also answers:** What is this solution? How do I start using the accelerator? What can I do with this? Where do I begin? What is the Databricks Data Engineering Accelerator?

This repository contains the **Databricks Data Engineering Accelerator** - a metadata-driven, enterprise-grade data ingestion and transformation solution built on Databricks. It provides:

- **Medallion Architecture**: Automated Bronze → Silver → Gold data processing pipeline
- **Metadata-Driven Configuration**: Configure data ingestion and transformations using SQL metadata tables instead of coding individual pipelines
- **Multi-Source Support**: Ingest from databases (Oracle, SQL Server, PostgreSQL, etc.), files (CSV, JSON, Excel, XML, Parquet), external volume shortcuts, and Databricks Lakehouse Federation / mirrored sources
- **Built-in DevOps**: CI/CD pipelines, multi-developer environments, Git integration, and automated testing
- **Enterprise Features**: Incremental loading with watermarks, SCD Type 2 dimensions, data quality checks, quarantine handling, and real-time monitoring reports

**Getting Started Steps:**
1. **Deploy the solution**: Ask GitHub Copilot *"How do I deploy the Databricks Data Engineering Accelerator?"* for step-by-step deployment instructions (~15 minutes)
2. **Configure your first data source**: Ask Copilot to generate metadata for your specific scenario, like:
   - *"Generate metadata to ingest all tables from my Oracle database"*
   - *"Create metadata to load CSV files from a external volume shortcut"*
   - *"Build a Silver layer transformation with data quality checks"*
3. **Validate metadata**: Ask Copilot *"How do I validate my metadata?"* to check for configuration errors
4. **Execute pipelines**: Ask Copilot *"How do I run my data pipeline?"* to start data ingestion
5. **Monitor results**: Ask Copilot *"How do I monitor my pipeline execution?"* to track status and troubleshoot issues

**New to the solution?** Ask GitHub Copilot questions like:
- *"What can I build with this accelerator?"*
- *"Show me examples of metadata configurations"*
- *"How do I set up incremental loading?"*
- *"What's the medallion architecture?"*

Copilot has full context of all documentation and can generate complete, validated metadata configurations for your specific use cases.

### What data sources does the solution support?
**Keywords:** sources, connectors, databases, files, ingestion, input, extract, read from, load from, connections, supported systems, data types, formats

**Also answers:** Can I connect to Oracle? Does it support SQL Server? What file formats can I ingest? Can I use shortcuts? Does it work with mirrored databases? What databases are supported?

See [INGESTION_PATTERNS_REFERENCE.md](INGESTION_PATTERNS_REFERENCE.md) for the canonical pattern map and [Data Ingestion Guide](DATA_INGESTION_GUIDE.md) for workflow context:
- **Databases**: Oracle, SQL Server, PostgreSQL, MySQL, DB2, Azure SQL
- **Files**: CSV, JSON, Excel, XML, Parquet (from UC Volumes, including shortcuts)
- **Shortcuts**: external volume shortcuts (cross-workspace, cross-tenant) and external shortcuts (ADLS Gen2, S3, Google Cloud Storage, Dataverse) are fully supported for file ingestion
- **Database mirroring**: The solution can read from mirrored data out of the box — mirrored data lands as Delta tables, so you configure it the same way as any other catalog table. Databricks Lakehouse Federation and mirroring support sources such as Azure SQL Database, Azure Cosmos DB, Snowflake, Google BigQuery, Oracle, and PostgreSQL. See [How do I ingest a Delta table or mirrored database table from another workspace with CDF?](#how-do-i-ingest-a-delta-table-or-mirrored-database-table-from-another-workspace-with-change-data-feed) for setup steps

### How do I deploy the IP (Intellectual Property)?
**Keywords:** deployment, install, setup, installation, provisioning, artifacts, workspace setup, initial setup, deploy artifacts, create workspace

See [Deployment Guide](Deployment_Guide.md) for step-by-step instructions. Summary:
1. Deploy the solution to your Databricks workspace
2. Run `deployment script` to deploy all artifacts
3. Configure metadata following [Metadata Reference - Metadata Deep Dive](METADATA_REFERENCE.md#metadata-overview-and-importance)

### How do I re-deploy or upgrade the accelerator in an existing workspace?
**Keywords:** re-deploy, redeploy, upgrade, update, re-run deployment, existing workspace, newer version, update notebooks, refresh accelerator, idempotent

**Also answers:** Can I re-run CreatingArtifacts_Orchestrator? Do I need to re-import all notebooks? Will re-deployment overwrite my data? Is the deployment idempotent? How do I update to a newer version?

The deployment is **idempotent** — it is safe to re-run on an existing workspace. You do **not** need to re-import `deployment script`. Reuse the one already in your workspace to keep your existing parameter selections.

**Re-deployment steps:**
1. Import only the updated `deployment script` and `import artifacts script` from the latest version of the repo
2. Open your existing `CreatingArtifacts_Orchestrator` notebook in the workspace (your parameters are already configured)
3. Run all cells

**What happens on re-deployment:**

| Artifact | Behavior |
|----------|----------|
| Catalogs | Skipped if they already exist (case-insensitive name match) |
| Warehouses | Skipped if they already exist |
| Notebooks | Updated in place (existing content is replaced with the new version) |
| Pipelines | Updated in place |
| Semantic Models & Reports | Updated in place |
| Environments | Reused if they already exist |
| Datastore data | Never deleted — all data files are preserved |
| Warehouse tables | Preserved — only schema and stored procedures are updated |

> ℹ️ **Datastore_Configuration is now JSON-driven.** It is upserted (MERGE, not truncate) from `databricks_batch_engine/datastores/datastore_<ENV>.json` during `/fdp-04-commit` via `automation_scripts/agents/sync_datastore_config.py`. Custom datastore entries belong in that JSON file (and optionally a per-branch override in `datastores/overrides/`), not in a manual INSERT.

> 💡 **Tip:** For major version upgrades, review `CHANGELOG.md` for breaking changes and test in a non-production workspace first.

### What is the medallion architecture?
**Keywords:** bronze, silver, gold, layers, architecture, data layers, catalog layers, medallion, zones, landing, curated, refined, raw, processed

See [Medallion Architecture Best Practices](Medallion_Architecture_Best_Practices.md) for the canonical layer guidance. The solution uses three layers:
- **Bronze**: Raw data storage
- **Silver**: Cleaned and enriched data
- **Gold**: Business-ready analytics data

### What are the rules for each medallion layer?
**Keywords:** layer rules, bronze rules, silver rules, gold rules, what goes where, medallion guidelines, layer boundaries, data placement, transformation rules, DQ placement, business logic placement

**Also answers:** What belongs in Bronze? What belongs in Silver? What belongs in Gold? Where do I put data quality checks? Where does business logic go? Can I join tables in Bronze? Should I write directly to Silver from external sources?

Use [Medallion Architecture Best Practices](Medallion_Architecture_Best_Practices.md) as the canonical source. The short version is:

- **Bronze** keeps raw source fidelity with minimal shaping.
- **Silver** handles validation, cleansing, deduplication, and conformance.
- **Gold** contains business-ready models, aggregations, facts, and dimensions.

Common failure modes are skipping Bronze for external sources, putting DQ in Bronze, and pushing business aggregates into Silver. Keep the full rules, anti-patterns, and examples in [Medallion Architecture Best Practices](Medallion_Architecture_Best_Practices.md).

---

## Understanding the Deployment

### What artifacts are deployed to my Databricks workspace?
**Keywords:** artifacts, resources, items, objects, deployment output, what gets created, catalogs, warehouses, pipelines, notebooks, reports, deployed items

**Also answers:** What gets created when I deploy? What catalogs are created? How many pipelines are deployed? What notebooks are included? What reports come with the solution?

The deployment process creates a comprehensive set of artifacts in your Databricks workspace. See [Runtime and Workflows Guide - Post-Deployment Artifact Review](DevOps.md#post-initial-deployment-artifact-review) for complete details:

**UC Catalogs (4 total)**:
- **Bronze**: Stores raw data in its original format or as Delta tables
- **Silver**: Contains cleaned, conformed, and enriched data
- **Gold**: Provides business-ready data for analytics (star schemas, fact tables, dimensions)
- **Metadata Volume**: Dedicated volume for metadata management and CI/CD operations

**Metadata Warehouse (1)**:
- Central repository for pipeline configuration metadata
- Contains logging tables for monitoring and troubleshooting
- Stores stored procedures for data pipeline orchestration
- Source for pre-built Power BI reports

**Data Pipelines (9+ pipelines)**:
- Orchestration pipelines for executing data movements
- Specialized pipelines for different ingestion patterns
- Utility pipelines for alerts, data profiling, and validation

**Notebooks (10+ notebooks)**:
- Core processing notebooks (batch ingestion, transformations)
- Helper function modules (3 notebooks with reusable utilities)
- Utility notebooks (lineage generation, validation, logging, exploratory analysis)

**Power BI Reports (3)**:
- Data Pipeline Monitoring Report (real-time operational dashboards)
- Exploratory Data Analysis Report (automated data profiling)
- Data Lineage Report (data flow visualization across medallion layers)

**Semantic Models (3)**:
- Direct Lake models connecting to Metadata Warehouse for real-time reporting

**Other Artifacts:**
- **Environment (1)**: `ENV_Default` — Databricks compute cluster for Spark compute configuration
- **Workspace Config (1)**: `workspace_config.json` — Workspace-level configuration variables for CI/CD

### What pipelines are deployed and what do they do?
**Keywords:** pipelines, orchestration, data pipelines, workflow, automation, ETL, ELT, data movement, processing, job names, metadata parameters, notebook invocation, warehouse endpoints, lightweight parameters, direct notebook invocation, pipeline to notebook, pass metadata, batch_processing.py parameters, non-framework executor_Execute_Non_Framework_Method, execute_warehouse_sp, execute_databricks_notebook, execute_databricks_job, write to delta, delta table write, create_delta_table, write data, save to catalog, delta lake, write to bronze, write to silver, write to gold, merge to delta, append to delta, overwrite delta

**Also answers:** How does metadata get passed to notebooks? How do pipelines invoke notebooks? What parameters are passed to batch_processing.py? How does the pipeline call the Spark notebook? What parameters does the notebook receive? How is data written to delta tables? How does the accelerator write to Delta Lake? What function writes data to catalog tables? How does create_delta_table work? How are Delta tables created?

The solution deploys modular pipelines that orchestrate metadata-driven data movement. See [Core Components Reference - Pipelines](CORE_COMPONENTS_REFERENCE.md#data-pipelines-for-data-movement) for the full breakdown.

**Summary:** `Trigger Step Orchestrator` loops through `Order_Of_Operations` steps for a trigger. `Entity Router` routes each Table_ID by `Processing_Method`: `batch` goes directly to `batch_processing.py`; `pipeline_stage_and_batch` goes to `staging task` (which invokes `batch_processing.py` after staging); other methods go to `non-framework executor`. `staging task` handles external database extraction with watermark tracking. `non-framework executor` orchestrates warehouse SPs, custom notebooks, and dataflows. Utility pipelines handle profiling (`Exploratory_Data_Analysis`) and DDL extraction (`External Data Staging_Metadata`). For alerting, use [Databricks SQL alerts](https://learn.microsoft.com/azure/databricks/sql/user/alerts) on warehouse queries.

### How does data ingestion work in this solution?
**Keywords:** ingestion, data flow, how it works, process, execution, workflow, metadata-driven, configuration, orchestration, data movement, ETL process, end-to-end, data processing, transformation flow

**Also answers:** How does the data transformation process work? What is the end-to-end data flow? How are transformations applied? What order do transformations run in? How does the accelerator process data?

The solution uses a **metadata-driven approach** — all ingestion logic is configured through metadata tables (Orchestration, Primary Config, Advanced Config) rather than hardcoded in pipelines. See [Core Components Reference](CORE_COMPONENTS_REFERENCE.md#data-pipelines-for-data-movement) for execution details and [Data Ingestion Guide](DATA_INGESTION_GUIDE.md) for scenario walkthroughs.

**Flow:** You define data movements in metadata SQL → run `Trigger Step Orchestrator_For_Each_Trigger_Step` with a `Trigger_Name` → the pipeline reads metadata, loops through `Order_Of_Operations` steps sequentially, processes Table_IDs in parallel within each step, extracts/transforms/writes data based on configuration, applies DQ checks, and logs everything to `Data_Pipeline_Logs`. Merge strategies include append, merge, overwrite, SCD Type 2, and file output.

### What notebooks are deployed and what are they used for?
**Keywords:** notebooks, PySpark, Spark, helper functions, modules, processing notebooks, transformation logic, code, scripts, notebook names, helper functions, batch_processing.py, helper functions, %run magic

**Also answers:** What helper functions are available? How do notebooks load shared code? What is helper functions used for? How does batch_processing.py work? Where is the transformation logic?

The solution deploys modular PySpark notebooks. See [Core Components Reference - Notebooks](CORE_COMPONENTS_REFERENCE.md#notebooks-for-data-movement) for the full list and details.

**Key notebooks:** `batch_processing.py` is the main data processing engine (handles batch ingestion across all medallion layers, fetches its own metadata via pyodbc, handles logging internally). Three helper notebooks (`helper_functions_1.py/2/3`) provide reusable transformation, file ingestion, and orchestration utilities — loaded via `%run` magic. Utility notebooks handle profiling (`run_exploratory.py_Data_Analysis`), lineage generation (`generate_lineage.py`), and integration testing (`integration_tests.py`). Custom notebooks (`custom_*`) are loaded dynamically via `load_workspace_module_into_globals()` when metadata references them.

### What stored procedures exist and what does each one do?
**Keywords:** stored procedures, SP, FIFO_Status, Get_Pipeline_Tables, Pivot_Primary_Config, Get_Advanced_Metadata, Get_Watermark_Value, Get_Schema_Details, Get_Datastore_Details, Log_Data_Movement, Log_New_Schema, Get_Exploratory_Analysis_Input, Get_Last_Run_Status, Create_Date_Dimension, metadata warehouse

**Also answers:** What SPs are in the metadata warehouse? How does FIFO locking work? What does Get_Pipeline_Tables return? How does the watermark get retrieved? Where is logging handled? What creates the date dimension?

The metadata warehouse contains 12 stored procedures. See [Core Components Reference - Stored Procedures](CORE_COMPONENTS_REFERENCE.md#stored-procedures) for the full catalog with calling contexts.

**Categories:** Orchestration & Config (`Get_Pipeline_Tables`, `Pivot_Primary_Config`, `Get_Advanced_Metadata`, `Get_Datastore_Details`), Data Movement & Watermarks (`FIFO_Status`, `Get_Watermark_Value`, `Get_Last_Run_Status`), Logging & Schema (`Log_Data_Movement`, `Log_New_Schema`, `Get_Schema_Details`), Utilities (`Get_Exploratory_Analysis_Input`, `Create_Date_Dimension`). For batch processing, `batch_processing.py` calls these via pyodbc; for non-batch, `non-framework executor` calls them via pipeline Script activities.

### Why does the solution use notebooks instead of wheel files?
**Keywords:** notebooks, wheel files, .whl, Python packages, custom functions, architecture decision, design rationale, load_workspace_module_into_globals, dynamic loading, unified developer experience, getDefinition

**Also answers:** Why not use Python packages? How does load_workspace_module_into_globals work? Why are helper functions in notebooks? How does the accelerator load custom code? Why develop in the workspace instead of locally?

This accelerator uses a **notebook-based approach** for custom functions instead of Python wheel (.whl) files, optimizing for the platform's compute model, CI/CD simplicity, and unified developer experience.

**Key benefits:** No cluster startup delay (no `.whl` install required), standard Git-based CI/CD (notebooks sync like other workspace artifacts), interactive debugging, and dynamic loading via `load_workspace_module_into_globals()` (custom notebooks loaded only when metadata references them). Core helpers use `%run` magic; custom functions use a dual-path loader (production: `.py` file from catalog `Files/custom_functions/`; development fallback: `dbutils.notebook.getDefinition()` API).

**Unified experience:** All development, testing, and debugging happens in the same Spark environment where code runs in production — no local Python setup, no mocked APIs, no "works on my machine" issues. Feedback loops are seconds (run cell) instead of minutes (build → deploy → test).

**Trade-offs:** Global namespace (mitigated by prefixed naming), no semantic versioning (use Git tags), code in notebook cells (notebooks are `.py` files under the hood — fully diff-able). Consider wheel files only for shared utilities across multiple independent workspaces or formal package versioning compliance requirements.

For detailed guidance on creating custom functions, see [Extending the Accelerator](Extending_the_Accelerator.md).

### Why does the accelerator use Spark notebooks instead of Python with Polars/DuckDB?
**Keywords:** Spark, PySpark, Polars, DuckDB, Python, DataFrame, performance, Delta Lake, architecture decision, single node, compute, CU, technology choice, data processing, ETL, Python notebook, Spark notebook, notebook type, notebook runtime, synapse_pyspark, which notebook, pandas

**Also answers:** Why not use Polars? Should I use DuckDB in the workspace? Can I mix Polars and Spark? Why PySpark over Python? What about single node compute? Is Polars faster than Spark? Should I use Python notebook instead of Spark? Which notebook type should I use? Can I use pandas instead of PySpark? What runtime should my notebook use?

This accelerator **standardizes on Spark notebooks** rather than Python notebooks with libraries like Polars or DuckDB. This is a deliberate architectural decision based on Delta Lake compatibility, Databricks platform alignment, and long-term maintainability.

**Delta Lake Feature Compatibility:**

The most critical reason is **Delta Lake feature support**. Non-Spark engines have significant gaps:

| Feature | Spark | Polars/DuckDB |
|---------|-------|---------------|
| **Deletion vectors** | ✅ Full read/write support | ⚠️ Limited/no write support |
| **Auto compaction** | ✅ Enabled automatically | ❌ Not available |
| **Optimized file sizing** | ✅ Spark handles this | ❌ Manual management required |
| **Delta 4.0 features** (Variant type, type widening) | ✅ Full support | ❌ Not supported |
| **Z-order / Liquid clustering** | ✅ Native integration | ❌ Not available |

**Practical Consequences of Mixing Engines:**

If you use Polars/DuckDB for Bronze/Silver and Spark for Gold:

1. **Write amplification** — Without deletion vector support, your Bronze/Silver tables will have inefficient merge operations and higher storage transaction costs
2. **Maintenance burden** — You can't use the platform's auto-compaction; table maintenance becomes your responsibility
3. **Feature inconsistency** — Your Gold layer will support Delta 4.0 features that Bronze/Silver cannot use
4. **Hidden costs** — While Polars may use fewer CUs for small atomic jobs, the lack of first-class Delta features introduces costs not visible in individual job metrics

**Platform Strategy Alignment:**

Databricks is built around Spark as the primary compute engine:

- **Likely roadmap direction**: Based on public signals, Microsoft appears to be investing in Single Node Spark compute options — if/when this ships, existing Polars code may become tech debt requiring a rewrite
- **Migration dynamics**: Databricks' Spark compatibility helps Databricks customers migrate with minimal code changes. There's no equivalent "Polars migration play" because enterprise Polars adoption remains limited
- **Industry standard**: Snowflake invested years building Snowpark (Spark-compatible) — not "SnowPolars" — because Spark is the enterprise standard
- **Competitive dynamics**: Healthy competition in the cloud data platform market drives Spark pricing down. As a Spark user, you benefit from this without code rewrites

**Why Not Rewrite Later?**

Some argue "use Polars now, rewrite with AI later." Reality check:

- **Code translation is easy** — GPT can convert syntax
- **Data validation is hard** — Ensuring correctness after migration requires extensive testing
- [Uber's Hive-to-Spark migration](https://www.uber.com/blog/how-uber-migrated-from-hive-to-spark-sql-for-etl-workloads/) demonstrates the complexity even between similar engines

**Recommendation:**

Standardize on Spark and push Microsoft for faster/cheaper Single Node Spark compute (Databricks feature requests). This approach:

- ✅ Avoids tech debt and future rewrites
- ✅ Ensures full Delta Lake compatibility across all layers
- ✅ Aligns with Microsoft's platform investment direction
- ✅ Benefits from vendor competition driving down Spark costs
- ✅ Leverages the mature Spark ecosystem and community

> 💡 **Note:** This guidance applies equally to DuckDB, Ibis, and other alternative DataFrame libraries. While these tools have valid use cases for ad-hoc analysis, they lack the Delta Lake integration maturity required for production medallion architectures.

### How do I run a data pipeline after deployment?
**Keywords:** run, execute, trigger, start, launch, initiate, run pipeline, execute pipeline, trigger pipeline, manual run, start pipeline, kick off

**Also answers:** How do I start the data movement? How do I kick off a pipeline? What pipeline do I run? How do I execute my metadata? How do I test my configuration?

After deploying the solution and configuring metadata, you execute data pipelines through the main orchestration pipeline. See [FAQ: How do I run a data pipeline after deployment?](#how-do-i-run-a-data-pipeline-after-deployment) for complete instructions:

**Prerequisites**:
1. ✅ Solution deployed to Databricks workspace
2. ✅ Metadata configured in Metadata Warehouse (Orchestration, Primary Config, Advanced Config tables)
3. ✅ Metadata validated using `validate_metadata_sql.py` script (recommended)

**Steps to Run a Pipeline**:

1. **Open the orchestration pipeline** in your Databricks workspace:
   - Navigate to your workspace
   - Find and open pipeline: `Trigger Step Orchestrator_For_Each_Trigger_Step`

2. **Click "Run"** in the toolbar

3. **Provide the parameters**:

   | Parameter | Required | Default | Description |
   |-----------|----------|---------|-------------|
   | **`Trigger_Name`** | ✅ Yes | — | The trigger name you configured in your metadata (e.g., `'SalesDataProduct'`) |
   | **`Full_Reload`** | No | `''` (empty) | Set to `'Yes'` to force a full reload of all tables in the trigger |
   | **`start_with_step`** | No | `1` | Resume from a specific processing step (useful for reruns after partial failures). See [Restarting Pipelines From Failure](Restarting_Pipelines_From_Failure.md) |
   | **`table_ids`** | No | `''` (all) | Comma-separated Table_IDs to process a subset (e.g., `'101,102'`). See [Restarting Pipelines From Failure](Restarting_Pipelines_From_Failure.md) |
   | **`run_exploratory_analysis`** | No | `True` | Set to `False` to skip EDA profiling after data movement |
   | **`folder_path_from_trigger`** | No | `''` | Used by event-driven triggers to pass the file path that triggered the pipeline |

4. **Monitor execution**:
   - View real-time progress in pipeline run history
   - Check `Data_Pipeline_Logs` table for detailed logging
   - Use the Data Pipeline Monitoring Power BI report for comprehensive monitoring

**Example Execution**:
```
Pipeline: Trigger Step Orchestrator_For_Each_Trigger_Step
Parameters:
  - Trigger_Name: 'SalesDataProduct'
  - Full_Reload: '' (leave empty for normal incremental run)
```

**What Happens During Execution**:
1. Pipeline reads all metadata records for the specified `Trigger_Name`
2. Processes each `Order_Of_Operations` sequentially (1, 2, 3...)
3. Within each order, processes Table_IDs in parallel
4. For each Table_ID:
   - Extracts data from source (database, files, or Delta tables)
   - Applies transformations and data quality checks
   - Writes to target datastore with configured merge strategy
   - Logs execution details and any errors

**Scheduling Pipelines** (Optional):
To run pipelines automatically on a schedule:
1. Open `Trigger Step Orchestrator_For_Each_Trigger_Step` pipeline
2. Click **"Schedule"** in the toolbar
3. Configure recurrence (hourly, daily, weekly, custom)
4. Set the `Trigger_Name` parameter value
5. Save the schedule

The pipeline will now run automatically at the configured intervals.

For pipeline parameters and execution options, see [Core Components Reference - Setting Up Triggers](CORE_COMPONENTS_REFERENCE.md#setting-up-triggers-for-data-movement).

### How do I trace data lineage and monitor pipeline execution?
**Keywords:** monitoring, logging, lineage, tracing, tracking, audit trail, observability, logs, execution history, troubleshooting, debugging, error tracking, Data_Pipeline_Logs, Activity_Run_Logs, Data_Quality_Notifications, Schema_Logs

**Also answers:** Where are the logs stored? How do I find execution history? How do I track data from source to target? What tables contain pipeline logs? How do I audit data movements?

The solution provides comprehensive traceability through multiple logging mechanisms. See [Monitoring and Logging](MONITORING_AND_LOGGING.md#application-logging-framework) for complete details.

**Logging tables:** `Data_Pipeline_Logs` (pipeline execution with timestamps, row counts, watermarks, Spark Monitor URLs), `Activity_Run_Logs` (step-by-step activity messages linked via `Log_ID`, with `Source_Type` to distinguish notebook vs pipeline origin), `Data_Quality_Notifications` (DQ check results and quarantine counts), `Schema_Logs` / `Schema_Changes` (schema evolution tracking). For batch processing, `batch_processing.py` handles all logging internally via pyodbc; for non-batch, `non-framework executor` logs via pipeline Script activities.

**Additional capabilities:** Metadata validation via `validate_metadata_sql.py` (pre-commit), integration tests in `integration_tests/notebooks/`, EDA profiling via `Exploratory_Data_Analysis`, end-to-end lineage via `generate_lineage.py`, and real-time monitoring via Power BI reports.

### How do I set up alerting for pipeline failures or data quality issues?
**Keywords:** alerts, alerting, notifications, email, Teams, failure notifications, data quality alerts, pipeline failure alerts, Data Activator, scheduled job notifications, PL_Send_Alerts, send alerts, email alerts, outlook

**Also answers:** How do I get notified when a pipeline fails? How do I send email alerts for data quality issues? Is there a PL_Send_Alerts pipeline? How do I set up failure notifications? How do I get Teams notifications for pipeline errors?

The accelerator does not include a custom alerting pipeline. Instead, use two native Databricks capabilities:

1. **Built-in scheduled job failure notifications** (recommended) — the simplest way to get alerted when a pipeline run fails. Configure job failure notifications in your Databricks workspace. Works for jobs, notebooks, and any other schedulable task. Emails include error details and a link to the failed job run. See [Configure email and system notifications for jobs](https://learn.microsoft.com/azure/databricks/jobs/notifications) for details.

2. **SQL-based alert rules on warehouse queries** — for content-aware alerts driven by your data (e.g., DQ failures, specific error patterns). In the metadata warehouse SQL editor, write a `SELECT` query against `Data_Pipeline_Logs` (for failures) or `Data_Quality_Notifications` (for DQ issues), then create a Databricks SQL alert. Configure the check frequency, condition, and action (email, webhook, or destination integration). See [Create a Databricks SQL alert](https://learn.microsoft.com/azure/databricks/sql/user/alerts) for full instructions.

   **Example — alert on pipeline failures today:**
   ```sql
   SELECT Trigger_Name, Table_ID, Ingestion_End_Time, Spark_Monitor_URL
   FROM   Data_Pipeline_Logs
   WHERE  Ingestion_Status = 'Failed'
   AND    CAST(Ingestion_End_Time AS DATE) = CAST(GETDATE() AS DATE)
   ```

   **Example — alert on data quality failures today:**
   ```sql
   SELECT Datastore_Name, Table_Name, Data_Quality_Category, Data_Quality_Message
   FROM   Data_Quality_Notifications
   WHERE  Data_Quality_Result != 'Pass'
   AND    CAST(Ingestion_End_Time AS DATE) = CAST(GETDATE() AS DATE)
   ```

### How do I create metadata configurations?
**Keywords:** metadata, configuration, setup, create metadata, configure, define, metadata records, metadata tables, SQL notebooks, metadata generation

Use GitHub Copilot with guidance from [Metadata Generation Guide - Writing Effective Prompts](METADATA_GENERATION_GUIDE.md#writing-effective-prompts-for-metadata-generation), or refer to [Data Ingestion Guide](DATA_INGESTION_GUIDE.md) for specific use cases.

### How does the accelerator help with migration to Databricks?
**Keywords:** migration, Databricks adoption, legacy platform, move to Databricks, migration challenges, why accelerator, migration value, data platform modernization, Synapse migration, Databricks migration, on-premises migration

**Also answers:** Why should I use the accelerator for migration? What are the challenges of migrating to Databricks? How does this reduce migration risk? Why not just rewrite everything in the workspace?

Migrating to Databricks presents significant challenges that the accelerator is specifically designed to address:

**The Migration Challenge**

| Challenge | Without the Accelerator | With the Accelerator |
|-----------|------------------------|----------------------|
| **Rewriting hundreds of ETL jobs** | Each job requires manual conversion to PySpark/SQL, creating inconsistent implementations | Convert logic to metadata configuration once; the framework handles all execution consistently |
| **Inconsistent code quality** | Every developer writes different patterns, creating maintenance nightmares | Pre-tested, standardized helper functions replace bespoke scripts; uniform patterns across all pipelines |
| **Steep platform/Spark learning curve** | Teams must learn PySpark, Delta Lake, Spark SQL, and platform-specific APIs | Configure metadata tables, not code; the framework abstracts Spark complexity |
| **Risk of logic loss during conversion** | Manual rewrites miss edge cases, leading to data discrepancies | Structured decomposition workflow with verification tables ensures 100% coverage |
| **Ongoing maintenance burden** | Scattered scripts across notebooks make updates error-prone | Centralized patterns and metadata-driven execution; change once, apply everywhere |
| **Testing and validation overhead** | Each custom script needs individual testing | Core framework is pre-tested; validate configuration, not code |

**Key Benefits for Migration Projects**

1. **Faster Time-to-Value**: Instead of months rewriting ETL logic, focus on converting requirements to metadata configurations
2. **Reduced Risk**: Pre-tested transformation functions eliminate bugs from hand-written code
3. **Built-in Best Practices**: Medallion architecture, incremental loading, data quality checks, and monitoring are ready to use
4. **AI-Assisted Conversion**: GitHub Copilot can analyze your existing SQL/ETL code and generate equivalent metadata configurations
5. **Standardized DevOps**: CI/CD pipelines, Git integration, and multi-environment deployments work out of the box

**Migration Approach**

The recommended migration path:
1. **Deploy the accelerator** to your Databricks workspace (~15 minutes)
2. **Analyze existing ETL** to identify sources, transformations, and targets
3. **Generate metadata** using GitHub Copilot or manually following the conversion workflow
4. **Validate and test** using built-in validation tools and monitoring reports
5. **Iterate** — migrate in phases, validating each domain before proceeding

For the technical step-by-step conversion process, see [How do I convert existing SQL/ETL code to accelerator metadata?](#how-do-i-convert-existing-sqletl-code-to-accelerator-metadata)

### How do I convert existing SQL/ETL code to accelerator metadata?
**Keywords:** migration, conversion, legacy code, existing ETL, SQL conversion, SSIS, Informatica, DataStage, ADF, convert code, migrate ETL, existing pipelines, legacy migration, transform SQL, convert stored procedures

**Also answers:** How do I migrate from SSIS? How do I convert Informatica mappings? How do I migrate ADF pipelines? How do I convert existing SQL to metadata? Can I reuse my existing ETL logic?

Converting legacy ETL code (SQL stored procedures, SSIS packages, Informatica mappings, ADF pipelines, etc.) to accelerator metadata requires a **decomposition-first approach**: list every operation in the legacy code, assign each to a medallion layer, map operations to built-in transformations (or custom functions), generate metadata SQL, and verify 100% coverage.

See [Legacy Code Conversion Guide](LEGACY_CODE_CONVERSION_GUIDE.md) for the complete step-by-step workflow, decomposition checklist, operation-to-transformation mapping table, and verification patterns.

### Where can I find examples of complex transformation patterns?
**Keywords:** complex patterns, join patterns, aggregation patterns, window functions, pivot examples, advanced transformations, SCD2 examples, fact table patterns, multi-step transformations

**Also answers:** Where are transformation examples? How do I see sample configurations? Are there template patterns? Where can I find advanced examples?

For complex scenarios beyond basic configurations, refer to these resources:

| Pattern Type | Documentation |
|--------------|---------------|
| Multi-step joins and aggregations | [Complex Transformation Patterns](Complex_Transformation_Patterns.md) |
| SCD Type 2 dimensions | [Runtime and Workflows Guide - Dimension Tables](DATA_MODELING_GUIDE.md#loading-data-into-a-dimension-table) |
| Fact tables with surrogate keys | [Runtime and Workflows Guide - Fact Tables](DATA_MODELING_GUIDE.md#loading-data-into-a-fact-table) |
| Window functions | [Transformation Patterns Reference - add_window_function](TRANSFORMATION_PATTERNS_REFERENCE.md) |
| Custom functions (Tier 2) | [Runtime and Workflows Guide - Custom Functions](DATA_TRANSFORMATION_GUIDE.md#using-a-custom-function-as-a-data-transformation-step) |
| End-to-end examples | [Metadata Generation Guide - Complete Examples](METADATA_GENERATION_GUIDE.md#complete-end-to-end-examples) |

**Quick Pattern Reference:**

| Need | Look Up |
|------|---------|
| Join 2-3 tables | `join_data` transformation |
| Join 4+ tables | Chain multiple `join_data` steps OR `custom_transformation_function` |
| Aggregate with GROUP BY | `aggregate_data` transformation |
| Running totals / rankings | `add_window_function` transformation |
| Pivot columns to rows | `unpivot_data` transformation |
| Rows to columns | `pivot_data` transformation |
| Recursive hierarchy | `custom_transformation_function` or `custom_table_ingestion_function` (Tier 2 required) |
| Multi-pass algorithm | `custom_transformation_function` or `custom_table_ingestion_function` (Tier 2 required) |

> 💡 **Tip:** When asked to generate metadata, GitHub Copilot will automatically consult these documentation sources. You can also ask Copilot directly: *"Show me an example of [pattern] configuration"*

### How do generated metadata SQL notebooks integrate with Databricks and Git?
**Keywords:** Git integration, version control, SQL notebooks, metadata files, Git workflow, commit, push, sync, source control, repository, Git sync, metadata deployment, SQL deployment

**Also answers:** How do I deploy metadata SQL notebooks? Where do I put metadata files? How does metadata sync to Databricks? How do I execute metadata in the warehouse? What is the Git workflow for metadata?

When GitHub Copilot generates metadata configuration files, they seamlessly integrate with your Databricks workspace through Git version control. Here's how the workflow operates:

**Prerequisites:**
1. **Az PowerShell must be available and signed in** on your local machine
   - Copilot checks this automatically before commit, run, investigate, and troubleshoot workflows
   - If the module is missing, the preflight can install the required Az PowerShell components automatically and then validate the session

2. **Set up Git integration** for your Databricks workspace that contains the Metadata Warehouse
   - Navigate to your workspace in the workspace
   - Enable Git integration and connect to your repository
   - See [DevOps Guide - Initial Setup](DevOps.md#1-initial-setup--prerequisites) for detailed instructions

3. **Clone the repository** to your local machine using VS Code
   - Open VS Code
   - Press `Ctrl+Shift+P` (Windows) or `Cmd+Shift+P` (Mac) to open the Command Palette
   - Type "Git: Clone" and select it
   - Paste your repository URL when prompted
   - Choose a local folder where you want to clone the repository
   - Click "Open" when prompted to open the cloned repository

3. **Copy required files** from this accelerator repository to your cloned repository:
   - Copy the accelerator's `.github/copilot-instructions.md` into your repo at `.github/copilot-instructions.md`
   - Copy the entire `docs/` folder to the root of your repo
   - These files provide GitHub Copilot with the solution context and documentation it needs to generate accurate metadata configurations

**How Generated Metadata Files Are Integrated:**

1. **File Generation**: When you ask Copilot to generate metadata configurations, it creates SQL Notebook files in your local repository under the metadata folder structure:
   ```
   your-repo/
   ├── .github/
   │   └── copilot-instructions.md
   ├── docs/
   │   └── (documentation files)
   └── <workspace-folder>/
       └── metadata/                              ← Metadata folder (one notebook per trigger)
           ├── metadata_SalesDataProduct.sql           ← ✅ Your metadata SQL lives here
           ├── metadata_CustomerETL.sql
           └── metadata_YourTriggerName.sql
               └── .sql
   ```
   **Note**: Each trigger gets its own `.sql` folder containing `.sql`.

   **Note**: Replace `<workspace-folder>` with your actual workspace folder name (e.g., `src`, `databricks`, or `MyWorkspace`)

2. **Git Commit & Push**: Use VS Code's Source Control panel to commit and push the generated files:
   - Open the Source Control view (Ctrl+Shift+G)
   - Stage the metadata notebook folders
   - Enter a commit message (e.g., "Add metadata configuration for YourTriggerName")
   - Click "Commit" then "Sync Changes" to push to your repository

3. **Git Sync**: In your Databricks workspace, click the **"Source control"** button and then **"Update all"** to sync the changes from Git to your workspace

4. **Warehouse Integration**: The metadata SQL notebooks appear in your Databricks workspace as SQL notebooks that you can open and execute directly

5. **Metadata Table Population**: Execute the SQL notebooks in the Metadata Warehouse to populate the metadata tables:
   - Option A: Open the notebook and click "Run all"
   - Option B: Use CI/CD pipelines for automated deployment (see [DevOps Guide - CI/CD Pipeline Configuration](DevOps.md#4-cicd-pipeline-configuration))

**Benefits of This Approach:**
- ✅ **Version Control**: All metadata changes are tracked in Git with full history
- ✅ **Collaboration**: Multiple developers can work on metadata configurations simultaneously
- ✅ **Review Process**: Use pull requests to review metadata changes before deployment
- ✅ **Automated Deployment**: CI/CD pipelines automatically deploy metadata to dev/test/prod environments
- ✅ **Rollback Capability**: Easily revert problematic metadata changes using Git history
- ✅ **Workspace Portability**: Metadata configurations can be deployed across multiple Databricks workspaces
- ✅ **Native Execution**: SQL notebooks integrate seamlessly with the Databricks SQL execution engine

**Example Workflow:**
```
Developer asks Copilot → Copilot generates metadata_SalesETL.sql →
Developer reviews file → Git commit & push →
Git Syncs changes → Notebook appears in workspace →
Execute SQL notebook in the workspace → Pipelines use new metadata
```

For detailed Git workflow and best practices, see [DevOps Guide - Metadata Table Records](DevOps.md#metadata-table-records).

### What configuration categories are available?
**Keywords:** configuration options, settings, parameters, config categories, metadata options, available configurations, configuration types, config names, source_details, target_details, watermark_details, data_cleansing, column_cleansing

**Also answers:** What metadata options exist? What can I configure? What settings are available? What are the valid configuration values? Where is the data dictionary?

See [METADATA_REFERENCE.md](METADATA_REFERENCE.md) for the complete reference of all valid configuration categories, names, and attributes.

### What default configuration values does the accelerator apply and why?
**Keywords:** defaults, baseline settings, preconfigured values, recommended settings, auto configuration, standard options, out-of-the-box values

The accelerator applies opinionated defaults so that each medallion layer gets sensible behavior without verbose metadata. The table below highlights the defaults that matter most in day-to-day authoring. All values come directly from [METADATA_REFERENCE.md](METADATA_REFERENCE.md).

| Area | Default | Where It Applies | Why It Matters |
| :--- | :------- | :---------------- | :------------- |
| `target_details.merge_type` | Bronze → `append`<br>Silver/Gold → `merge`<br>Gold with delete markers → `merge_and_delete` | All pipelines that do not explicitly set a merge type | Keeps Bronze loads fast (no dedupe), guarantees idempotent upserts in curated layers, and switches to delete-aware logic automatically when `delete_rows_with_value` is populated. |
| `column_cleansing.apply_case`, `column_cleansing.replace_non_alphanumeric_with_underscore` | Empty string and `false` (no automatic changes) | Column standardization | User must explicitly configure column name transformations. Set `apply_case` to `lower`/`upper`/`title` and `replace_non_alphanumeric_with_underscore` to `true` if needed. |
| `data_cleansing.replace_blank_with_null_in_string_columns`, `trim_data_in_string_columns` | Empty string (no automatic changes) | Primary configuration | User must explicitly configure data cleansing. Set to `*` for all string columns or specify column names if needed for your use case. |
| `watermark_details.use_watermark_column` | `true` | Incremental extracts | Ensures we filter by the latest watermark automatically so repeat runs do not reread the entire source. For Delta-to-Delta full-refresh / `target_details.merge_type = 'overwrite'` patterns, set this explicitly to `false` rather than just omitting watermark rows. |
| `target_details.quarantine_table_name` | `{target_table_name}_quarantined` | Any configuration that enables quarantine | Provides a deterministic landing zone for non-compliant rows without additional metadata. |
| `data_quality` actions (`if_not_compliant`) | `warn` | Pattern validation, referential integrity, data filter, minimum records, etc. | Logs warnings by default; set to `mark` for row-level detail markers, `quarantine` to isolate bad records, or `fail` to halt the pipeline for critical validations. |
| `validate_pattern.allow_null` | `true` | Pattern checks in Advanced configuration | Permits optional fields by default; set to `false` when the field must exist. |
| `other_settings.data_profiling_frequency` | `weekly` | Optional profiling runs | Balances insight and cost by running the profiling pipeline once a week unless you change it. |

> **Tip:** Use defaults whenever they align with your scenario—they reduce metadata volume and are already covered by the validation script. Override them only when you have a concrete requirement and document the change in your SQL notebook header.

### How do I validate my metadata before execution?
**Keywords:** validation, validate, check, verify, test metadata, validation script, metadata errors, configuration errors, validate configuration

Run the `validate_metadata_sql.py` script before delivery. Use [METADATA_AUTHORING_WORKFLOW.md](METADATA_AUTHORING_WORKFLOW.md) for the canonical authoring workflow and [Metadata Reference - Metadata Validation](METADATA_REFERENCE.md#metadata-validation) for the detailed validation behavior.

```bash
python .github/skills/metadata-validation/validate_metadata_sql.py <path_to_sql_file>
```

### How should I organize my metadata SQL notebooks?
**Keywords:** file structure, organization, SQL notebook format, file layout, metadata file structure, sections, DELETE statements, INSERT statements, file organization

Metadata SQL notebooks follow a strict 5-section structure to ensure consistency, maintainability, and CI/CD compatibility. See [Metadata SQL File Format](METADATA_SQL_FILE_FORMAT.md#sql-file-format-and-structure) for complete details.

**Required File Structure (in this exact order):**

1. **Header Comment Block** - File metadata and documentation
   - Purpose description
   - Trigger name and Table_IDs covered
   - Source and target information
   - Prerequisites and special notes

2. **DELETE Statements** - Remove existing metadata (reverse dependency order)
   - `DELETE FROM Advanced_Configuration WHERE Table_ID IN (SELECT...)`
   - `DELETE FROM Primary_Configuration WHERE Table_ID IN (SELECT...)`
   - `DELETE FROM Orchestration WHERE Trigger_Name = '...'`
   - **⚠️ CRITICAL**: Always use `WHERE Trigger_Name = '...'` with subquery, NEVER hardcode Table_IDs

3. **Orchestration INSERT** - Define what, where, and when
   - One INSERT statement with all Table_IDs for the trigger
   - Each row on a single line (required for CI/CD diff tracking)

4. **Primary Configuration INSERT** - Core settings for each Table_ID
   - One INSERT statement containing ALL configuration categories (source_details, target_details, watermark_details, etc.)
   - Group all configs for each Table_ID together

5. **Advanced Configuration INSERT** - Transformations and data quality
   - One INSERT statement containing ALL transformations and DQ checks
   - Group all configs for each Table_ID together

**Example Structure:**
```sql
-- =====================================================================
-- Header: Metadata Configuration for SalesDataProduct
-- =====================================================================

-- STEP 1: DELETE existing metadata
DELETE FROM Data_Pipeline_Metadata_Advanced_Configuration
WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'SalesDataProduct');
DELETE FROM Data_Pipeline_Metadata_Primary_Configuration
WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'SalesDataProduct');
DELETE FROM Data_Pipeline_Metadata_Orchestration
WHERE Trigger_Name = 'SalesDataProduct';

-- STEP 2: Orchestration Metadata
INSERT INTO Data_Pipeline_Metadata_Orchestration (...)
VALUES
('SalesDataProduct', 1, 101, 'bronze', 'dbo.sales', 'sale_id', 'pipeline_stage_and_batch', 1),
('SalesDataProduct', 2, 102, 'silver', 'dbo.sales_clean', 'sale_id', 'batch', 1);

-- STEP 3: Primary Configuration
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (...)
VALUES
(101, 'source_details', 'source', 'oracle'),
(101, 'source_details', 'datastore_name', 'oracle_sales'),
(101, 'watermark_details', 'column_name', 'ModifiedDate'),
(102, 'source_details', 'table_name', 'bronze.dbo.sales'),
(102, 'target_details', 'merge_type', 'merge');

-- STEP 4: Advanced Configuration (if needed)
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (...)
VALUES
(102, 'data_transformation_steps', 'derived_column', 1, 'column_name', 'total_amount'),
(102, 'data_quality', 'validate_condition', 1, 'condition', 'amount > 0'),
(102, 'data_quality', 'validate_condition', 1, 'if_not_compliant', 'quarantine'),
(102, 'data_quality', 'validate_condition', 1, 'message', 'Amount must be positive')
```

**Critical Rules:**
- ✅ **Use trigger-based DELETE with subqueries** - Never hardcode Table_IDs
- ✅ **One INSERT per metadata table** - Group all Table_IDs together (max 1000 rows)
- ✅ **Single-line VALUES rows** - Essential for Git diff tracking
- ✅ **Use only `--` comments** - Never use `/* */` multi-line comments
- ✅ **Semicolon on last VALUES row** - Each INSERT statement must end with `;` on the last VALUES row only; all other rows end with commas
- ✅ **Consistent ordering** - Always: Header → DELETEs → Orchestration → Primary → Advanced

**Why This Structure Matters:**
- **Idempotency**: DELETE statements allow safe re-execution without duplicates
- **CI/CD Compatibility**: Single-line rows produce clean Git diffs
- **Environment Portability**: Trigger-based DELETEs work across dev/test/prod
- **Maintainability**: Predictable structure makes files easy to review and update
- **Automated Processing**: CI/CD pipelines rely on this consistent format

**Common Mistakes to Avoid:**
- ❌ Hardcoding Table_IDs in DELETE statements
- ❌ Creating separate INSERT statements for each configuration category
- ❌ Using multi-line formatting for VALUES rows
- ❌ Placing Advanced Configuration before Primary Configuration
- ❌ Omitting the semicolon from the last VALUES row of each INSERT statement

### Can I use subfolders to organize metadata SQL notebooks?
**Keywords:** subfolders, folders, directory structure, organize files, file organization, nested folders, folder hierarchy

**Also answers:** Can I organize metadata by domain? How do I organize hundreds of metadata notebooks?

You can organize metadata notebooks by domain using naming conventions:

**New notebook-based structure:**
```
metadata/
├── metadata_00_Shared_CommonSources.sql
├── metadata_01_Sales_Bronze_Ingestion.sql
├── metadata_01_Finance_Bronze_Ingestion.sql
├── metadata_02_Sales_Silver_Cleansing.sql
└── metadata_03_Sales_Gold_Facts.sql
    └── .sql
```

**Pattern:** `metadata_{Order}_{Domain}_{Layer}_{Purpose}.sql`

Notebooks sort alphabetically by domain and layer, making them easy to find. This approach works natively with Databricks's workspace browser.

**Scaling guidance:**

| Number of Tables | Recommended Approach |
|------------------|---------------------|
| < 50 tables | Single notebook per trigger |
| 50-200 tables | Hierarchical naming convention |
| 200-500 tables | Domain-consolidated notebooks (one notebook per data product with section headers) |
| 500+ tables | Domain-consolidated notebooks + team ownership boundaries |

### Why use SQL notebooks instead of Python notebooks for metadata?
**Keywords:** notebooks, metadata notebooks, Python metadata, notebook configuration, SQL notebooks

**Also answers:** Can I store metadata in Python notebooks? Why use SQL notebooks?

The accelerator uses **SQL notebooks** (`.sql`) for metadata because:

**Advantages of SQL notebooks:**
- **Native warehouse execution**: SQL notebooks execute directly against the Metadata Warehouse in the workspace
- **Git-friendly diffs**: SQL INSERT statements are line-by-line, making PR reviews easy
- **Validation support**: The accelerator's `validate_metadata_sql.py` parses SQL syntax with 60+ validation rules
- **Cross-file validation**: The validator checks Table_ID uniqueness across all metadata notebooks
- **No execution context switching**: SQL runs In Warehouse context without needing Spark/JDBC

**When Python notebooks might make sense:**
- You need to generate metadata programmatically from source system APIs
- You have complex conditional logic for metadata generation
- Your team wants to use Python libraries for transformation

If you need Python-based metadata generation, you'd need to adapt the validation tooling accordingly.

**When notebooks might make sense:**
- Your team is significantly more comfortable with Python than SQL
- You want to generate metadata programmatically from source system APIs
- You have complex conditional logic for metadata generation

If notebooks align better with your team's workflow, the migration is possible but requires investment in tooling updates.

### How do I get consistent metadata generation results from Copilot?
**Keywords:** consistent, inconsistent, different results, LLM variation, naming conventions, Table_ID, reproducible, deterministic, metadata generation consistency

**Problem:** Each time you ask Copilot to generate metadata for the same source, you may get different Table_IDs, naming conventions, or transformation approaches.

**Solution - For individual files:** Before generating new metadata, ask Copilot to **read your existing metadata notebooks first**:

> "Read the existing metadata notebooks in the metadata folder, then generate metadata for [your source] following the same conventions"

Copilot will extract patterns from your existing files (naming, Table_ID ranges, transformations used, dimension tables referenced) and generate new metadata that matches.

**Solution - For large-scale migrations:** If you need to convert many files from a legacy ETL system (ADAM, Informatica, SSIS, etc.), build a **deterministic converter script**:

1. **When to use a script:**
   - Converting 10+ similar source files
   - Source files have consistent structure (same ETL tool export format)
   - You need guaranteed identical output for same input

2. **What the script does:**
   - Parses the source format (XML, JSON, etc.)
   - Applies fixed mapping rules (naming, Table_ID allocation, transformation selection)
   - Outputs valid metadata SQL following accelerator conventions

3. **How to create one:**
   - Ask Copilot: "Create a Python script that converts [source format] to accelerator metadata SQL"
   - Provide 2-3 example source files and your desired output format
   - Iterate until the script produces consistent results

**Key principle:** Your existing metadata IS your configuration. New metadata should look like what you already have.

### Can I use custom Python or PySpark functions for data processing?
**Keywords:** custom functions, Python, PySpark, custom code, custom logic, custom transformations, user-defined functions, UDF, custom processing

Yes! There are four types of custom functions:
- **Custom ingestion from files**: See [Runtime and Workflows Guide - Custom File Ingestion](DATA_INGESTION_GUIDE.md#using-a-custom-function-to-ingest-files)
- **Custom ingestion from tables**: See [Runtime and Workflows Guide - Custom Table Ingestion](DATA_TRANSFORMATION_GUIDE.md#transform-delta-tables-with-custom-ingestion-and-data-cleansing)
- **Custom staging**: See [Runtime and Workflows Guide - Custom Staging Function](DATA_INGESTION_GUIDE.md#using-a-custom-staging-function)
- **Custom transformations**: See [Runtime and Workflows Guide - Custom Transformation Functions](DATA_TRANSFORMATION_GUIDE.md#using-a-custom-function-as-a-data-transformation-step)

### Where do I store custom function notebooks?
**Keywords:** custom notebook location, notebook folder, notebook path, where to put notebooks, custom_, custom function notebook, notebook placement, workspace folder, notebook storage

**Also answers:** What folder should custom notebooks be in? Do I need to specify the folder path in metadata? Can I organize notebooks into folders? Where does the accelerator look for custom notebooks?

Custom function notebooks must be stored in the **same Databricks workspace** as the core accelerator notebooks (`batch_processing.py`, `helper functions_*`). Within that workspace, you can place them in **any folder**—the accelerator will find them by name.

**Key Rules:**

| Rule | Details |
|------|---------|
| **Same workspace** | Custom notebooks MUST be in the same workspace as `batch_processing.py` |
| **Any folder** | You can organize notebooks into folders (e.g., `Custom/`, `Transformations/`, `Ingestion/`) |
| **Metadata = notebook name only** | Do NOT include folder path in metadata—only the notebook name |

**Correct Metadata Configuration:**
```sql
-- ✅ CORRECT: Just the notebook name (no folder path)
(101, 'source_details', 'custom_table_ingestion_function_notebook', 'custom_Product_Ingestion'),

-- ✅ CORRECT: For transformation functions
(102, 'data_transformation_steps', 'custom_transformation_function', 1, 'notebook_name', 'custom_Hierarchy_Builder'),
```

**Incorrect Metadata Configuration:**
```sql
-- ❌ WRONG: Don't include folder paths
(101, 'source_details', 'custom_table_ingestion_function_notebook', 'Custom/custom_Product_Ingestion'),
(101, 'source_details', 'custom_table_ingestion_function_notebook', 'Transformations/custom_Product_Ingestion'),
```

**Example Workspace Structure:**
```
My Databricks workspace/
├── batch_processing.py          ← Core accelerator notebook
├── helper_functions_1.py        ← Core accelerator notebook
├── helper_functions_2.py        ← Core accelerator notebook
├── helper_functions_3.py        ← Core accelerator notebook
├── Custom/                      ← Your folder (optional)
│   ├── custom_Product_Ingestion    ← Your custom notebook
│   └── custom_Hierarchy_Builder    ← Your custom notebook
└── Ingestion/                   ← Another folder (optional)
    └── custom_API_Extract          ← Your custom notebook
```

> 💡 **Tip:** Use a consistent naming convention like `custom_*` to easily identify your custom notebooks in the workspace.

### How do I share reusable code across multiple custom notebooks?
**Keywords:** reusable code, code duplication, share code, shared utilities, helper functions, %run, helper_functions_4.py, custom helper, common code, authentication, API client, utility functions, modular, namespace, load_workspace_module_into_globals, globals

**Also answers:** Can custom notebooks use %run? Why can't I import from another custom notebook? How do I avoid copy-pasting code between custom notebooks? Where should I put shared authentication logic? How do custom functions access helper functions? Do I need to create helper_functions_4.py?

**Short answer:** Put reusable code in a **new helper functions notebook** (e.g., `helper_functions_4.py`), not inside custom function notebooks. Custom notebooks should contain only entity-specific logic.

**Why this works — the shared namespace:**

Custom function notebooks are loaded via `load_workspace_module_into_globals()`, which executes code into the **same IPython session namespace** as the core notebooks. This means your custom functions already have access to all helper functions from `helper_functions_1.py/2/3`:

```python
# Inside your custom notebook — these work WITHOUT any imports:
def my_custom_ingestion(metadata, spark):
    log_and_print("Starting custom ingestion")  # ✅ From helper_functions_1.py
    
    config = metadata.get('source_datastore_config', {})
    ws_id = _get_datastore_config(config, 'bronze', 'Workspace_ID')  # ✅ From helper_functions_3.py
    
    return df
```

**Common misconception: "%run doesn't work in custom notebooks"**

This is a red herring. The accelerator never uses `%run` to load custom notebooks — it uses `load_workspace_module_into_globals()` + `get_ipython().run_cell()`. The custom function's code executes in the shared namespace where all helper functions already exist. You don't need `%run` inside custom notebooks.

**The right pattern — helper_functions_4.py:**

If you have reusable code that multiple custom notebooks need (e.g., auth logic, API clients, storage utilities), create `helper_functions_4.py` in your workspace:

```python
# helper_functions_4.py — YOUR reusable helpers (not shipped with the accelerator)

def authenticate_osdu(keyvault_name, secret_name):
    """Shared OSDU authentication used by multiple custom notebooks."""
    token = dbutils.secrets.get(keyvault_name, secret_name)
    return {"Authorization": f"Bearer {token}"}

def call_paginated_api(base_url, headers, params=None):
    """Generic paginated API client reused across custom ingestion notebooks."""
    import requests
    all_records = []
    url = base_url
    while url:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        all_records.extend(data.get('results', []))
        url = data.get('next_page_url')
    return all_records
```

Then add `import helper_functions_4.py` to `batch_processing.py` **after** the existing `import helper_functions_3.py` line. Your reusable helpers become available to all custom notebooks automatically.

**Why helper_functions_4.py instead of modifying 1/2/3:**

| Approach | Upgrade Safe? | Why |
|----------|--------------|-----|
| Add code to `helper_functions_1.py/2/3` | ❌ | Core notebooks are overwritten when you pull accelerator updates |
| Create `helper_functions_4.py` | ✅ | Your file — never shipped with the accelerator, never overwritten |
| Duplicate code in each custom notebook | ❌ | Maintenance burden — fixes don't propagate |

**Setup steps:**

1. Create `helper_functions_4.py` in your Databricks workspace with your reusable functions
2. Add `import helper_functions_4.py` to `batch_processing.py` after `import helper_functions_3.py`
3. Your custom notebooks now call these shared helpers directly — no imports needed

> ⚠️ **Important:** `helper_functions_4.py` is NOT included in the accelerator repo. You create it in your own workspace. This ensures accelerator upgrades never overwrite your custom helpers.

See [Extending the Accelerator - Adding Reusable Helper Functions](Extending_the_Accelerator.md#adding-reusable-helper-functions) for detailed setup instructions.

### Why must custom notebooks place imports inside functions?
**Keywords:** import inside function, top-level import, namespace pollution, shared namespace, import scope, globals, shadowing, import conflict, custom notebook import, validate_custom_notebook_import_scope, LEGB, f alias, pyspark.sql.functions

**Also answers:** Why does the validator flag my top-level imports? Can top-level imports in custom notebooks cause problems? Why can't I import at the top of my custom notebook? Do imports inside functions still work? Can my function still call log_and_print if I move imports inside?

Custom notebooks execute via `get_ipython().run_cell()` in the **same shared IPython namespace** as all helper functions (`helper_functions_1.py/2/3`). Every top-level `import` in your custom notebook goes straight into `globals()`, where it can **shadow** critical symbols that the helper functions depend on.

**Why this is dangerous at scale:**

| Risk | Example |
|------|---------|
| **Shadowing** | Your notebook does `from some_lib import f` → silently overwrites `pyspark.sql.functions` alias `f` that all helpers use |
| **Order-dependent bugs** | Function A works alone but breaks when loaded after Function B (which overwrote an import) |
| **Wildcard pollution** | `from X import *` dumps dozens of names into `globals()` |
| **Debugging nightmare** | Load order of custom notebooks changes → "it worked yesterday" |

**The fix — import inside your function:**

```python
# ✅ CORRECT — imports scoped to the function, cannot pollute shared namespace
def my_custom_transform(new_data, metadata, spark):
    from pyspark.sql import functions as f
    from pyspark.sql.types import StructType, StructField, StringType
    import requests

    log_and_print("Starting transform")  # ✅ Still works — resolved from globals via LEGB
    result = new_data.withColumn("new_col", f.lit("value"))
    return result
```

```python
# ❌ WRONG — top-level imports pollute the shared namespace
import requests
from pyspark.sql import functions as f

def my_custom_transform(new_data, metadata, spark):
    ...
```

**Can my function still call helpers like `log_and_print`?** Yes. Python's LEGB scoping rule (Local → Enclosing → Global → Built-in) means your function first checks its local scope, then the global scope where `log_and_print`, `_get_datastore_config`, and all other helper functions live. Moving imports inside the function only affects where *your* imports live — it has zero impact on accessing helpers.

**Existing precedent:** The accelerator's own `_apply_datetime_substitutions()` in `helper_functions_1.py` already uses a local `from datetime import datetime as dt_module` with the comment: *"Imports datetime locally to avoid conflicts with custom functions that may have modified the datetime module in the global namespace."*

> ⚠️ **CI/CD enforcement:** The validation script (`validate_metadata_sql.py`) includes a `validate_custom_notebook_import_scope` rule that flags top-level imports in custom notebooks as errors, blocking deployment.

### When should I use built-in transformations vs custom functions?
**Keywords:** built-in transformations, custom function, custom ingestion, when to use custom, transformation hierarchy, tier 1, tier 2, multiple tables, complex logic, recursive CTE, API, ML, machine learning, combine, mix, both

**Also answers:** How do I decide between out-of-box transformations and custom code? Should I use custom_transformation_function or built-in transformations? When is custom code necessary? How do I build a dimension from multiple source tables? Can I use both built-in and custom transformations together?

Use [TRANSFORMATION_DECISION_TREE.md](TRANSFORMATION_DECISION_TREE.md) as the canonical escalation guide and [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) when Tier 2 is necessary.

Short version:

- Start with built-in transformations first.
- Escalate to custom functions only when built-ins cannot express the requirement cleanly.
- Reserve standalone notebook execution as the last resort because it weakens lineage and framework reuse.

Typical Tier 2 triggers are recursive logic, multi-pass algorithms, external API or SDK calls, ML inference, or large multi-table assembly where the metadata becomes harder to maintain than the code.

You can also combine both approaches for the same Table_ID: keep standard renames, casts, filters, and projections in built-ins, and isolate only the non-expressible logic in a custom function.

### How does watermarking work with custom_table_ingestion_function?
**Keywords:** custom ingestion watermark, watermark custom function, custom function incremental, watermark_value, watermark_filter, custom ingestion incremental, multi-table watermark, watermark table name, watermark column name

**Also answers:** How does the accelerator know which table to get watermark from? What watermark values are passed to my custom function? How do I implement incremental loading in a custom function? How does watermark_details work with custom ingestion?

When you use `custom_table_ingestion_function`, the accelerator **passes watermark info TO your function** but **your function is responsible for using it** (or not). Here's the complete flow:

**How the Accelerator Determines the Watermark Table/Column:**

The accelerator reads from your metadata configuration:
```sql
-- These settings tell the accelerator WHERE to get the new watermark value
(100, 'watermark_details', 'table_name', 'bronze.customers'),     -- Table to query for MAX watermark
(100, 'watermark_details', 'column_name', 'ModifiedDate'),        -- Column to get MAX value from
(100, 'watermark_details', 'column_data_type', 'datetime'),       -- Data type for proper comparison
```

**What Gets Passed to Your Custom Function:**

Your function receives a `metadata` dictionary with these watermark-related keys (added automatically by the accelerator before calling your function):

```python
def my_custom_ingestion(metadata: dict, spark) -> DataFrame:
    # Accelerator adds these before calling your function:
    watermark_column_name = metadata.get('watermark_column_name')  # e.g., "ModifiedDate"
    watermark_value = metadata.get('watermark_value')              # e.g., "2025-01-15 00:00:00" (from last run)
    watermark_filter = metadata.get('watermark_filter')            # e.g., "ModifiedDate > '2025-01-15 00:00:00'"
```

**Who Does What:**

| Responsibility | Accelerator Does This | Your Custom Function Does This |
|----------------|----------------------|-------------------------------|
| Store last watermark value | ✅ (in logging table) | - |
| Pass watermark info to function | ✅ | - |
| Build `watermark_filter` string | ✅ | - |
| **Apply filter to source data** | - | ✅ (your choice to use it or not) |
| Return filtered DataFrame | - | ✅ |
| Calculate NEW watermark | ✅ `MAX(column_name)` from `table_name` | - |
| Save new watermark for next run | ✅ | - |

> ⚠️ **Critical:** The accelerator calculates the **new watermark** by querying `MAX(watermark_column_name)` from `watermark_details.table_name`—NOT from your function's returned DataFrame!

**Example: Using the Pre-Built Filter**

```python
def my_dimension_from_multiple_tables(metadata: dict, spark) -> DataFrame:
    # Get pre-built watermark filter from accelerator
    watermark_filter = metadata.get('watermark_filter', '')
    
    # Apply filter to your DRIVING table (the one configured in watermark_details.table_name)
    customers = spark.sql(f"""
        SELECT * FROM bronze.customers 
        WHERE {watermark_filter if watermark_filter else '1=1'}
    """)
    
    # Read lookup tables (no watermark - full read each time)
    countries = spark.sql("SELECT * FROM bronze.countries")
    regions = spark.sql("SELECT * FROM bronze.regions")
    
    # Join them all
    result = customers \
        .join(countries, "country_id", "left") \
        .join(regions, "region_id", "left")
    
    return result
```

**Metadata Configuration Example:**

```sql
-- Orchestration
(101, 'SalesDataProduct', 1, 'silver', 'dbo.dim_customer', 'customer_id', 'batch', 1),

-- Primary Config: Custom ingestion function
(101, 'source_details', 'custom_table_ingestion_function', 'my_dimension_from_multiple_tables'),
(101, 'source_details', 'custom_table_ingestion_function_notebook', 'custom_Dim_Customer'),

-- Watermark config: Points to the DRIVING table (determines new_watermark_value)
(101, 'watermark_details', 'table_name', 'bronze.customers'),
(101, 'watermark_details', 'column_name', 'ModifiedDate'),
(101, 'watermark_details', 'column_data_type', 'datetime'),
```

**Common Patterns:**

| Pattern | Watermark Configuration | What Happens |
|---------|------------------------|--------------|
| **One driving table + lookups** | Set `table_name` to the driving table | Filter driving table by watermark; full-read lookups |
| **Full refresh (no incremental)** | Set `merge_type = 'overwrite'`; if the source is a Delta table read in batch, also set `use_watermark_column = 'false'` | Reload everything without watermark filtering |
| **Custom incremental logic** | Configure watermark; implement your own filtering | Use `watermark_value` to build custom filters across multiple tables |

### What parameters are passed to custom functions, and how do I debug failures?
**Keywords:** custom function parameters, function signature, custom function debugging, custom function fails, NameError, KeyError, metadata dict, function_config, custom function not found, custom function troubleshooting, globals, load_workspace_module_into_globals

**Also answers:** What is the function signature for custom transformations? What does the metadata dict contain? Why does my custom function work locally but fail in the pipeline? How do I access custom attributes in my function? What causes NameError for custom functions?

Use [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) for the canonical function-type comparison and the template notebooks in [resources](resources/) for the exact signatures.

At runtime, the `metadata` payload typically includes `orchestration_metadata`, `primary_config`, `advanced_config`, `datastore_config`, and `event_payload`. `custom_transformation_function` calls also receive `function_config`, which is the correct place to read step-specific custom attributes.

The most common failure modes are still:

- `NameError`: notebook or function name mismatch.
- Wrong signature: missing the expected parameters for the selected function type.
- Returning `None` or a pandas DataFrame instead of a Spark DataFrame.
- Importing libraries only in the development notebook instead of inside the custom notebook function.
- Deploying metadata without deploying the corresponding custom notebook artifact.

For full examples and debugging details, use the runtime sections for each function type and the templates in [resources](resources/).

### How do I find my connection ID?
**Keywords:** connection ID, GUID, connection GUID, database connection, connection string, data source connection, find connection, connection identifier, datastore_name, datastore configuration

1. Create a Databricks secret scope (or reuse one) that holds credentials for the external system.
2. Register the external datastore in `databricks_batch_engine/datastores/datastore_<ENV>.json` under `external_datastores`:

```jsonc
{
  "external_datastores": {
    "oracle_sales": {
      "kind": "oracle",
      "connection_details": {
        "host": "oracle.example.com",
        "service_name": "ORCL",
        "secret_scope": "kv-oracle"
      }
    }
  }
}
```

3. Run `/fdp-04-commit` — it automatically invokes `sync_datastore_config.py`, which MERGEs the entry into `{metadata_catalog}.{metadata_schema}.Datastore_Configuration` (serializing `connection_details` into the `Connection_Details` JSON column).

4. In your metadata SQL, reference the logical name — **not** raw credentials:

```sql
(@table_id, 'source_details', 'datastore_name', 'oracle_sales'),
```

> ⚠️ **Do NOT embed credentials in metadata SQL.** The `datastore_name` pattern keeps metadata environment-agnostic — the same name resolves to different `connection_details` per environment via each environment's `datastore_<ENV>.json` file. Feature branches can further override those details through `datastores/overrides/<sanitized-branch>.json`.

### How do I customize Spark cluster size or add external libraries?
**Keywords:** Spark cluster, cluster size, custom pool, executor, driver, cores, memory, external libraries, Python packages, pip, Databricks compute cluster, spark_environment_id, compute, Spark configuration, workspace pool

**Also answers:** How do I install custom Python packages? How do I increase Spark executor memory? How do I use a larger Spark cluster?

The accelerator supports attaching a Databricks compute cluster to control Spark compute configuration and external libraries on a per-entity basis.

**What is a Databricks compute cluster?**

A Databricks compute cluster is a configurable artifact that bundles Spark compute settings (cluster size, driver/executor cores, memory) and library management (public PyPI packages, custom `.whl`/`.jar` files) into a reusable configuration. See the [Microsoft documentation](https://learn.microsoft.com/azure/databricks/compute/) for full details.

**How to configure it:**

1. **Create a Databricks compute cluster** in your workspace via the Databricks UI
2. Configure the compute settings (pool selection, executor cores/memory) and install any required libraries
3. **Copy the Environment ID** (GUID) from the Environment's URL or settings page
4. **Add it to your entity's metadata** in the `other_settings` category:

```sql
(@table_id, 'other_settings', 'spark_environment_id', '<your-environment-guid>'),
```

**When to use this:**

| Scenario | Example |
|----------|---------|
| Large tables needing more memory | Increase executor memory from 28g to 56g or 112g |
| Complex transformations needing more cores | Scale up from 4 to 8 or 16 Spark vCores |
| Custom Python/Java libraries | Install packages like `geopy`, `phonenumbers`, or custom `.whl` files |
| Isolation between workloads | Separate compute configs for Bronze ingestion vs Gold aggregations |

**Default behavior:** If `spark_environment_id` is not specified, the entity uses the initially deployed Databricks compute cluster (Databricks workspace defaults).

> **CI/CD note:** You only need the Environment GUID from your **dev** workspace. The Workspace CI/CD pipeline (`Deploy-WorkspaceArtifacts-And-ReplaceIds.ps1`) automatically replaces environment GUIDs by matching the environment **display name** between source and target workspaces. As long as the environment has the same name in DEV, QA, and PROD (e.g., `ENV_Geocoding`), the GUID is swapped automatically during deployment — no `parameter.yml` tokens are needed for this.

> **Tip:** You can reuse the same Databricks compute cluster across multiple entities. Only create separate environments when workloads have genuinely different compute or library requirements.

### How do I add a custom Python library that only certain tables need?
**Keywords:** custom Python library, new environment, dedicated environment, per-table environment, pip package, third-party library, ENV_Default, spark_environment_id, isolated environment, selective library, specific tables

**Also answers:** Should I modify ENV_Default to add a library? How do I avoid adding libraries to the default environment? How do I create a separate environment for a few tables? How do I install a Python package for specific entities only?

> ⚠️ **Best Practice: Do NOT add every library to `ENV_Default`.** Instead, create a **dedicated Databricks compute cluster** with only the libraries your specific tables need, and assign it via metadata.

Adding libraries to `ENV_Default` affects **every entity in the workspace** — it increases environment publish time, can introduce version conflicts, and may break existing pipelines if a new library conflicts with an existing one. The accelerator is designed to let you assign environments per entity, so use that.

**Step-by-step:**

**1. Create a new Databricks compute cluster**

- In the Databricks UI, go to your workspace
- Click **+ New** → **Environment**
- Name it descriptively, e.g., `ENV_Geocoding` or `ENV_Sales_API`
- Add only the Python libraries this workload needs (e.g., `geopy`, `phonenumbers`)
- Click **Save**, then **Publish** (⚠️ Save alone does NOT install the libraries — you must Publish)
- Wait for the publish to complete

**2. Copy the Environment ID**

- Open the environment in the Databricks UI
- The Environment ID (GUID) is in the browser URL:
  ```
  the Databricks workspace cluster policy URL
  ```
- Copy the `<environment-id>` GUID

**3. Assign it to only the tables that need it via metadata**

Add a single row to `Data_Pipeline_Metadata_Primary_Configuration` for each table that needs this environment:

```sql
-- Only Table_ID 100 and 101 will use the geocoding environment
-- All other tables continue using the workspace default (ENV_Default)
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
(100, 'other_settings', 'spark_environment_id', '<your-environment-guid>'),
(101, 'other_settings', 'spark_environment_id', '<your-environment-guid>');
```

**How it works at runtime:**

When `batch_processing.py` starts for a given entity, it checks if `spark_environment_id` is set in that entity's metadata. If set, the notebook attaches to that specific environment (with its libraries and compute settings). If not set, it uses the workspace default.

```
┌────────────────────────────────────────────────────────────────────┐
│  Entity Processing                                                 │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  Table_ID 100 (spark_environment_id = ENV_Geocoding)               │
│  ├─ Spark session uses ENV_Geocoding                               │
│  ├─ geopy, phonenumbers available via import                       │
│  └─ Custom notebook uses: import geopy                             │
│                                                                    │
│  Table_ID 200 (no spark_environment_id set)                        │
│  ├─ Spark session uses ENV_Default (workspace default)             │
│  └─ Standard processing, no extra libraries needed                 │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

**Why create a dedicated environment instead of modifying `ENV_Default`?**

| Concern | Modify `ENV_Default` | Dedicated Environment |
|---------|---------------------|-----------------------|
| **Blast radius** | ⚠️ All entities affected | ✅ Only assigned entities |
| **Publish time** | Slower (more libraries) | Fast (minimal libraries) |
| **Version conflicts** | ⚠️ Risk increases with each library | ✅ Isolated dependencies |
| **Debugging** | Harder to trace which library caused issues | ✅ Clear scope |
| **Rollback** | Must roll back for all entities | ✅ Unassign the environment ID from metadata |

**Multiple environments example:**

```sql
-- Geocoding tables use ENV_Geocoding
(100, 'other_settings', 'spark_environment_id', 'aaa-bbb-ccc-ddd'),
(101, 'other_settings', 'spark_environment_id', 'aaa-bbb-ccc-ddd'),

-- High-memory tables use ENV_LargeCompute
(200, 'other_settings', 'spark_environment_id', 'eee-fff-ggg-hhh'),

-- All other tables: no row needed — they use ENV_Default automatically
```

> 💡 **Tip:** Keep `ENV_Default` lean — it should contain only what the core accelerator needs. Create purpose-specific environments for additional libraries, and share them across related entities.

> **CI/CD note:** Use the Environment GUID from your **dev** workspace in metadata SQL. The Workspace CI/CD pipeline automatically replaces environment GUIDs by matching the environment **display name** across workspaces. Just ensure the environment has the **same name** in DEV, QA, and PROD (e.g., `ENV_Geocoding`) — no manual GUID swapping or `parameter.yml` tokens are needed. See `Deploy-WorkspaceArtifacts-And-ReplaceIds.ps1` for details.

📖 **Related:** [How do I customize Spark cluster size or add external libraries?](#how-do-i-customize-spark-cluster-size-or-add-external-libraries) | [Why is my Databricks compute cluster not working?](#why-is-my-databricks-compute-cluster-not-working-custom-libraries-not-loading-compute-settings-ignored)

### What's the difference between process, pipeline_stage_and_batch, and pipeline_stage_only?
**Keywords:** processing methods, process, pipeline_stage_and_batch, pipeline_stage_only, stage only, processing mode, ingestion mode, data loading methods, execute_warehouse_sp, execute_databricks_notebook, execute_databricks_job

**Also answers:** What processing methods are available? When should I use execute_databricks_notebook? How do I stage data without loading to a table? Can I just land files without writing to Delta?

- **`batch`**: Routes directly to `batch_processing.py`. Use for:
  - **Delta tables** (Bronze/Silver/Gold layers within the workspace)
  - **Files** (CSV, JSON, Excel, XML, Parquet) already in the UC Volumes section
  - **Custom staging function** (`custom_staging_function`) — **preferred for API sources**: Your Python function stages raw files (JSON, CSV, etc.) to UC Volumes; the notebook then reads, parses, and processes them into a Bronze Delta table. This follows medallion architecture best practice — land data as-is first, then parse into the Bronze table. Especially important for JSON API responses where preserving the raw payload enables reprocessing without re-calling the API. Set `source_details.exit_after_staging='true'` to stop after landing files. See [Data Ingestion Guide - Custom Staging Function](DATA_INGESTION_GUIDE.md#using-a-custom-staging-function).
  - **Custom source function** (`custom_source_function`): Your Python function returns a DataFrame from any external source (SDKs, external databases); the framework handles DQ, transforms, merge, and logging. Use when the source returns small, structured, tabular data and raw file preservation is not needed. See [Data Ingestion Guide - Custom Source Function](DATA_INGESTION_GUIDE.md#using-a-custom-source-function).
- **`pipeline_stage_and_batch`**: Required when ingesting from **external sources** that need an orchestration job to first extract data into UC Volumes (staging), before the notebook loads it into Delta tables. Use this when `source_details.source` is configured (e.g., databases like Oracle, SQL Server, PostgreSQL, REST APIs via the built-in `rest_api` source type, or any future external source types). The staging extraction is handled by `External Data Staging_Copy`, which uses a Switch activity with Copy activities for each source type. You can also **add a dynamic external job activity** to `External Data Staging_Copy` as an alternative extraction mechanism — see [Adding a external job to staging task for External Staging](Extending_the_Accelerator.md#option-a2-add-a-dataflow-gen-2-to-staging task-for-external-staging).
- **`pipeline_stage_only`**: Same external source extraction as `pipeline_stage_and_batch` (data is copied to UC Volumes via `External Data Staging`), but **stops after staging** — does **not** write to a Delta table. Use this when you want to land raw files in the UC Volumes section for downstream consumption (e.g., another team's notebook, manual inspection, or a separate processing pipeline) without the accelerator automatically loading them into a table. The same `source_details` configuration applies (`source`, `datastore_name`, `query`, `staging_volume_name`, `staging_folder_path`, etc.).
- **`execute_databricks_notebook`**: Execute an external Databricks notebook as a job task. Use when orchestrating existing notebooks, legacy migration, or cross-workspace processing.
- **`execute_warehouse_sp`**: Execute a warehouse stored procedure as a standalone pipeline activity. Author metadata using `source_details.datastore_name` for the warehouse host and `source_details.stored_procedure_name` for the procedure name.
- **`execute_databricks_job`**: Execute a Databricks job as a **standalone** task for data already within the workspace (e.g., workspace-internal transformations or existing notebook orchestrations). Use when you need to orchestrate existing jobs or workflows. **Note:** This is different from using an external job inside `External Data Staging_Copy` for external data staging — for that scenario, see `pipeline_stage_and_batch` above.

> ⚠️ **CI/CD note for `execute_*` methods:** Register the executable artifact host in `Datastore_Configuration` and reference it by `source_details.datastore_name` in metadata. For notebooks/jobs, that entry is the Databricks workspace and item itself. For warehouse stored procedures, that entry is the warehouse host and the procedure name lives in `source_details.stored_procedure_name`. See [What IDs does the Workspace CI/CD pipeline replace automatically?](#what-ids-does-the-workspace-cicd-pipeline-replace-automatically) for details.

### How do I ingest from external sources without using a pipeline Copy Activity?
**Keywords:** custom source function, custom staging function, REST API, SDK, OAuth, external database, notebook ingestion, batch external, custom connector, no Copy Activity, custom_source_function, custom_staging_function, API ingestion

**Also answers:** How do I pull data from a REST API? How do I use a Python SDK to ingest data? Can I ingest external data without pipeline_stage_and_batch? How do I connect to sources not supported by built-in Databricks connectors?

Use `Processing_Method = 'batch'` with one of two notebook-native custom functions. Both run inside `batch_processing.py` — no pipeline Copy Activity or `staging task` staging pipeline is involved.

| Approach | Your function returns | Framework handles | Use when |
|----------|----------------------|-------------------|----------|
| `custom_staging_function` **(preferred for APIs)** | `{rows_copied, next_watermark_value}` | File reading, DQ, transforms, merge, logging | API/SDK source — land raw response files (JSON, CSV, etc.) then parse into Bronze table |
| `custom_source_function` | DataFrame | DQ, transforms, merge, schema, logging | SDK/service returns small structured tabular data and raw file preservation is not needed |

> **Medallion best practice:** For API sources — especially those returning JSON — prefer `custom_staging_function` to land the raw response as-is in UC Volumes, then let the framework parse and load it into the Bronze Delta table. This preserves source fidelity, enables reprocessing without re-calling the API, and follows the medallion principle of landing data before transforming it.

**Custom staging function (recommended for APIs)** — Configure `Processing_Method = 'batch'`, plus `source_details.custom_staging_function`, `source_details.custom_staging_function_notebook`, `source_details.staging_volume_name`, and `source_details.staging_folder_path`. Your function receives `(metadata, spark)` including `target_folderpath_w_ingestion_type` for the staging path. Write raw API responses as direct child files into that timestamped folder (e.g., JSON, Parquet, CSV); zip files are allowed, subfolders are not. The timestamp is derived from the orchestration run's `trigger_time`, and that value is required so retries within the same run reuse the same folder while preserving the `YYYYMMDDHHMMSS` naming pattern. Before each custom staging attempt, the framework clears and recreates that folder so stale files from a failed attempt cannot be mistaken for current output. If `trigger_time` is missing or invalid, custom staging fails rather than generating a non-idempotent folder name. Set `exit_after_staging = 'true'` to stop after landing files, or omit it to continue into table processing. If the folder is empty after staging, the run exits gracefully as no new data. Staging-phase lineage logs the source as `External` / `Custom Staging Function`; if the run continues into table ingestion, the later batch phase logs the staged files as the source.

**Custom source function** — Configure `source_details.custom_source_function` and `source_details.custom_source_function_notebook`. Your function receives `(metadata, spark)` and returns a DataFrame (or `(DataFrame, next_watermark_value)` for incremental loads). No staging folder, no file I/O — the DataFrame flows directly into the standard batch pipeline. Best suited for SDKs or services that return small, already-structured tabular data where raw file archival is not a concern. `source_details.datastore_name` is optional here unless your function needs to resolve a registered datastore entry for connection or workspace metadata.

Both approaches support incremental loads via watermarks and accept custom config values through `source_details` key-value pairs (e.g., `custom_staging_function_api_endpoint`).

> **When to use `pipeline_stage_and_batch` instead:** Use the pipeline approach when a built-in the workspace Copy Activity connector already supports your source — this includes databases (Oracle, SQL Server, PostgreSQL, etc.) **and REST APIs** via the built-in `rest_api` source type in `External Data Staging_Copy`. The pipeline approach is more performant for large extractions and doesn't consume Spark compute during the extract phase.

> **Pipeline vs notebook trade-offs for API ingestion:**
>
> | | Pipeline (`rest_api` source) | Notebook (`custom_staging_function`) |
> |---|---|---|
> | **Credentials** | Managed via Databricks secrets — no secrets in code | Typically requires a managed private endpoint to Key Vault for credentials |
> | **Spark overhead** | No Spark compute during extraction | Managed private endpoint to Key Vault can slow Spark startup and development speed |
> | **Flexibility** | Limited to what the Copy Activity REST connector supports (pagination, auth patterns) | Full Python control over authentication, pagination, response parsing, retries, and complex multi-step API flows |
> | **Best for** | Standard REST APIs with straightforward auth and pagination | Complex APIs requiring custom logic, OAuth flows, GraphQL, or multi-call assembly |

See [Data Ingestion Guide - Custom Source Function](DATA_INGESTION_GUIDE.md#using-a-custom-source-function) and [Custom Staging Function](DATA_INGESTION_GUIDE.md#using-a-custom-staging-function) for full configuration examples, function signatures, and template notebooks.

### How does incremental copy with staging area work for external databases?
**Keywords:** incremental copy, staging area, pipeline_stage_and_batch, external database ingestion, watermark, staging task, Copy Activity, folder-based watermark, Get_Watermark_Value, Log_Data_Movement, staging folder, timestamp folders, duplicate handling, merge_in_batches_with_columns, drop_duplicates

**Also answers:** How does staging task extract data? How does batch_processing.py pick up staged data? What is the staging folder structure? How are watermarks tracked for external sources? How do I handle duplicates from reprocessed batches? What happens if a watermark gets reset?

When using `Processing_Method = 'pipeline_stage_and_batch'`, the accelerator uses a **two-phase ingestion pattern** with separate watermark tracking per phase. See [Data Ingestion Guide](DATA_INGESTION_GUIDE.md) for the full walkthrough.

**Phase 1 (staging task Extract):** Calls `Get_Watermark_Value(@processing_phase='Staging')`, builds a filtered query against the source database, extracts data via Copy Activity to a timestamped staging folder (e.g., `staging_path/20250127143022/data.parquet`), and logs with `@processing_phase='Staging'`.

**Phase 2 (batch_processing.py):** Calls `Get_Watermark_Value(@processing_phase='Batch')`, discovers new staging folders by comparing folder timestamps to the batch watermark, reads and unions all new files, applies transformations and DQ checks, writes to the Delta table, and logs with `@processing_phase='Batch'`.

**Why two watermarks:** Extraction and loading are decoupled — failed batch processing doesn't re-extract, multiple extractions can accumulate before a single batch load, and recovery restarts from the last successful phase.

**Duplicate handling:** Use `drop_duplicates` transformation (recommended — deduplicates by primary key using `delta__modified_datetime` ordering) or `merge_in_batches_with_columns` (processes each staging folder sequentially for audit trail preservation).

### How do I change between Bronze, Silver, and Gold catalogs for my target?
**Keywords:** target datastore, destination, target datastore, output location, where to write, target warehouse, destination datahouse, change target

Set the `Target_Datastore` column in the Orchestration metadata table to the actual catalog name you configured during deployment:
- Bronze catalog: `Target_Datastore = 'bronze'` (or whatever you named your Bronze catalog)
- Silver catalog: `Target_Datastore = 'silver'` (or whatever you named your Silver catalog)
- Gold catalog: `Target_Datastore = 'gold'` (or whatever you named your Gold catalog)
- Custom catalog: Use any catalog name in your workspace (e.g., `'my_custom_catalog'`)

**Note**: The default catalog names are `bronze`, `silver`, and `gold`, but these can be customized during deployment in the `deployment script` notebook parameters.

### Can I have multiple triggers running at the same time?
**Keywords:** parallel execution, concurrent runs, multiple triggers, simultaneous execution, parallel processing, concurrent pipelines, parallel runs

Yes! Triggers are independent and can run in parallel. Each trigger executes its own set of Table_IDs based on `Order_Of_Operations`. However, be mindful of:
- cluster capacity limits
- Concurrent connections to source systems
- Resource contention on shared dimensions/fact tables

> 📖 For a deeper dive on trigger granularity, design patterns, and managing cross-trigger dependencies, see [Trigger Design and Dependency Management](Trigger_Design_and_Dependency_Management.md).

### What if my source data doesn't have a primary key?
**Keywords:** no primary key, missing primary key, no business key, no natural key, append, overwrite, merge, upsert, external data, files, source without key, silver, gold, drop duplicates, deduplicate, add_row_hash, composite key

**Also answers:** How do I handle data without keys? How do I deduplicate data? How do I create a synthetic key? Can I use merge without primary keys? How do I handle duplicates?

If you're ingesting data that does **not** have a reliable primary key / business key, what you do depends on the target layer:

- **Bronze (raw landing):**
   - Leave `Primary_Keys` blank (NULL/empty) in the Orchestration metadata.
   - Use `target_details.merge_type = 'append'` (accumulate raw history) or `target_details.merge_type = 'overwrite'` (full refresh).
   - Avoid `merge`-based modes because there is no key to match on.

- **Silver/Gold (curated tables):** you typically need a deterministic way to identify “the same record” across runs.
   - **Option A (recommended): deduplicate on a composite key**
      - Identify a set of columns that together uniquely identify a record (your "composite business key").
      - Use `data_transformation_steps` → `drop_duplicates` with `column_name = 'col1,col2,col3'` (the columns that form your key).
      - Optionally add `order_by = '<modified_date_column>'` + `order_direction = 'desc'` to keep the most recent row when duplicates exist.
      - Then set `Primary_Keys` in the Orchestration metadata to those same columns (e.g., `'col1,col2,col3'`) and use `merge_type = 'merge'`.
   - **Option B (wide/awkward keys): hash first, then deduplicate**
      - If your composite key is many columns or unwieldy, first use `data_transformation_steps` → `add_row_hash` with `column_name = 'col1,col2,col3'` and `output_column_name = 'business_key_hash'`.
      - Then `drop_duplicates` on `column_name = 'business_key_hash'`.
      - Use `business_key_hash` as `Primary_Keys` for merge.
   - **Option C (fallback): full refresh**
      - If you truly cannot define any stable key, keep `Primary_Keys` blank and use `target_details.merge_type = 'overwrite'`.

> **Rule of thumb:** If you cannot define a stable business key (even a composite one), prefer `overwrite` for Silver/Gold. Using `merge` without a real key can create incorrect updates or unpredictable results.

### What's the difference between merge types for target tables?
**Keywords:** merge types, write modes, upsert, append, overwrite, merge strategies, load types, insert modes, update modes, merge options, merge_and_delete, merge_mark_unmatched_deleted, merge_mark_all_deleted, replace_where, scd2, soft delete, output_file, file output

**Also answers:** How do I upsert data? How do I handle deletes? What write mode should I use? How do I do a full refresh? How do I update existing records?

See [Runtime and Workflows Guide - target_details Configuration Reference](METADATA_REFERENCE.md) for complete details:
- **`append`**: Add all new records (no deduplication) - fastest option
- **`merge`**: Upsert based on primary keys (insert new, update existing)
- **`overwrite`**: Replace all data in target table
- **`merge_and_delete`**: Merge + physically delete records marked for deletion
- **`merge_mark_unmatched_deleted`**: Merge + soft-delete records not in source
- **`merge_mark_all_deleted`**: Mark all matched records as deleted before merge (versioning scenarios)
- **`replace_where`**: Selective partition overwrite based on column values (efficient for partition-level updates)
- **`warehouse_spark_connector`**: Use official Spark connector for Databricks SQL Warehouse writes
- **`scd2`**: SCD Type 2 historical tracking for dimension tables (requires `source_timestamp_column_name`)
- **`output_file`**: Write to file formats (CSV, Parquet, JSON, Excel) — auto-detected when `Target_Entity` contains `/`, or set explicitly

> If your source does not have primary keys, see: [What if my source data doesn't have a primary key?](#what-if-my-source-data-doesnt-have-a-primary-key)

### When should I use liquid clustering?
**Keywords:** liquid clustering, clustering, partitioning, performance, optimization, query performance, Z-order, clustering columns

Use liquid clustering on columns that are:
- Frequently used in WHERE clauses for filtering
- Used in JOIN conditions
- Used in GROUP BY operations
- High-cardinality columns (many distinct values)

Example: `liquid_clustering_columns = 'customer_id, order_date, region'`

See [Runtime and Workflows Guide - Advanced Configurations](DATA_INGESTION_GUIDE.md#advanced-configurations) for setup.

### What are the best practices for creating custom transformation functions?
**Keywords:** best practices, custom functions, transformation best practices, coding standards, function development, PySpark best practices, error handling

Keep the detailed implementation guidance in [Runtime and Workflows Guide - Custom Transformation Functions](DATA_TRANSFORMATION_GUIDE.md#using-a-custom-function-as-a-data-transformation-step). The short version is:

- keep the function focused on the logic built-ins cannot express
- import dependencies inside the function body
- return a Spark DataFrame with predictable schema
- log enough context to debug pipeline failures
- test the notebook independently before wiring it into metadata

If the function is generic enough to be reused, move repeated helper logic into shared helpers instead of copying it across notebooks.

### What are best practices for metadata validation before deployment?
**Keywords:** validation best practices, testing, pre-deployment, validation workflow, metadata testing, configuration validation, deployment checklist

> 🔴 **CRITICAL:** EVERY metadata SQL notebook MUST be validated before delivery. NO EXCEPTIONS. This prevents configuration errors that cause pipeline failures in production.

Use [METADATA_AUTHORING_WORKFLOW.md](METADATA_AUTHORING_WORKFLOW.md) as the canonical procedural checklist and [Metadata Reference - Metadata Validation](METADATA_REFERENCE.md#metadata-validation) for the detailed validator behavior.

The non-negotiable rules are:

1. Run `validate_metadata_sql.py` before commit or delivery.
2. Fix every reported issue.
3. Re-run until validation passes cleanly.
4. Update [METADATA_REFERENCE.md](METADATA_REFERENCE.md) and validator rules together when the contract changes.

Typical failures are undocumented values, missing required attributes, cross-record inconsistencies, and SQL formatting mistakes such as incorrect `VALUES` layout or semicolon placement.

### What are best practices for managing metadata in Git?
**Keywords:** Git best practices, version control, source control, Git workflow, branching, pull requests, PR, feature branches, release branches, hotfix branches, metadata management

Use [DevOps Guide - Metadata Table Records](DevOps.md#metadata-table-records) as the canonical workflow.

Short answer:
- author metadata in trigger-scoped notebooks and keep the SQL idempotent
- validate before creating a pull request, then promote through `feature/*` → `dev` → `release/*` → approved `main` (preferably tagged)
- do not make manual metadata edits in higher environments; let CI/CD apply the change and verify it in the pipeline logs

Use the DevOps guide when you need the exact authoring rules, batch-size limits, or the full promotion procedure.

### How do I add a new catalog or warehouse as a target datastore?
**Keywords:** add datastore, new catalog, new warehouse, cross-workspace, different workspace, multi-workspace, workspace configuration, datastore configuration, Datastore_Name, Datastore_Configuration, catalog name, globally unique, naming, datastore naming, catalog naming, register datastore, register catalog

**Also answers:** What is Datastore_Name? Does the datastore name have to match the volume name? Do catalog names need to be unique? How do I register a catalog? What goes in Datastore_Configuration? Why can't I find my catalog? Why is my datastore not working?

To add a new catalog, warehouse, or external source as a target/source datastore, edit `databricks_batch_engine/datastores/datastore_<ENV>.json`. The `Datastore_Configuration` Delta table is the runtime-side mirror of that file and is refreshed automatically on every `/fdp-04-commit` via `automation_scripts/agents/sync_datastore_config.py`. See [databricks_batch_engine/datastores/README.md](../databricks_batch_engine/datastores/README.md) for the full schema.

> 🔴 **CRITICAL NAMING RULE:** `Datastore_Name` is a logical identifier referenced from metadata rows. Pick a name and keep it stable across environments.

> 🔴 **ACCELERATOR UNIQUENESS REQUIREMENT:** Datastores are resolved by name, so those names must be unique within a given environment's JSON file.

Short answer:
- For a medallion layer (`bronze`/`silver`/`gold`): add or edit an entry under `layers.<name>` with its Unity Catalog `catalog`. Per-layer schemas do **not** live here — they come from each metadata row's `Target_Entity` (`schema.table`).
- For an external system (SAP, SQL Server, Snowflake, REST API, ADLS, ...): add an entry under `external_datastores.<name>` with a `kind` discriminator and a `connection_details` object.
- Register every environment (DEV, QA, PROD, ...), not just one, so promotion picks up the right catalog and connection details.
- Commit both the JSON change and any metadata SQL that references the new name; the commit agent runs the sync automatically.

### How do I query live data in a table using Copilot?
**Keywords:** investigate table, live data, query table, sample data, column stats, value distribution, data freshness, custom query, table summary, /fdp-06-investigate, SQL analytics endpoint, table-observability

**Also answers:** How does Copilot connect to my catalog? Can Copilot run SQL against my tables? How do I get stats on my table? How do I preview data? How do I check data freshness?

Use the `/fdp-06-investigate` prompt command in Copilot Chat. This activates the **table-observability** skill which can:

1. **Check cached EDA profiling first** — if the `Exploratory_Data_Analysis_Results` table has data profiled within the last 24 hours, Copilot uses that instead of running a live query
2. **Run live SQL queries** against your actual table data via the SQL Analytics endpoint registered in `Datastore_Configuration`

> **🔴 CRITICAL:** Do not hardcode database names such as `silver`, `gold`, `bronze`, or `Metadata`. Copilot must resolve the live database name dynamically from `Datastore_Configuration.Datastore_Name` for the selected environment. The metadata warehouse is only for resolving config/logs and discovering the target endpoint/database.

**Available investigation modes:**

| Mode | What It Does |
|------|--------------|
| **Data Summary** | Row counts, column counts, null analysis, cardinality |
| **Sample Data** | Preview 10–50 rows from the table |
| **Column Stats** | Min, max, mean, std dev, distinct counts per column |
| **Value Distribution** | Top-N value frequencies for a specific column |
| **Custom Query** | Run a read-only ad-hoc query (SELECT/WITH only) |
| **Data Freshness** | Compare latest data timestamps against last pipeline run |
| **Stats Comparison** | Compare cached EDA profiling vs live computed stats |

**Prerequisites:**
- Az PowerShell must be installed locally and you must be signed in with `Connect-AzAccount`
- The `Endpoint` column in `Datastore_Configuration` must be populated with the SQL Analytics endpoint (or Warehouse SQL endpoint)
- Datastore endpoints are registered per-environment in `datastores/datastore_{ENV}.sql` (parent folder varies by environment — search recursively)
- Copilot asks for the environment only when it cannot be inferred or when multiple datastore environments are possible

**Safe live-query sequence:**
1. Resolve the table and investigation mode first (run history, metadata config, live data, current schema, etc.)
2. For metadata-warehouse questions, use the metadata query helper to resolve the warehouse from datastore notebooks and run the canonical query
3. For live data or current schema, resolve the target endpoint and datastore from the appropriate `datastores/datastore_{ENV}.sql` entry for the selected environment
4. Connect to the resolved target endpoint using the resolved target `Datastore_Name`
5. Query `INFORMATION_SCHEMA.COLUMNS` first to discover the live schema, then run summary/sample/statistics queries against the target datastore

> 💡 **Tip:** Type `/fdp-06-investigate` in Copilot Chat for a full automated investigation workflow.

**Common Mistakes:**

| Mistake | What Happens | Fix |
|---------|--------------|-----|
| `Datastore_Name` doesn't match workspace artifact name | Pipeline fails — cannot find datastore | Use the **exact** display name from Databricks |
| catalog name conflicts with another in the tenant | the platform renames it (e.g., appends a number) | Choose a globally unique name upfront |
| Forgot to add datastore in QA/PROD notebooks | CI/CD deployment to QA/PROD fails | Add to all `datastore_*.sql` files |
| Used different GUIDs across notebooks for same env | Points to wrong artifact | Copy GUIDs carefully per environment |

### How do I ingest a Delta table or mirrored database table from another workspace with Change Data Feed?
**Keywords:** cross-workspace catalog, ingest catalog another workspace, CDF cross workspace, change data feed different workspace, external catalog/volumouse ingestion, remote catalog, mirrored database, Database mirroring, mirrored DB, mirrored catalog, Cosmos DB, Snowflake, BigQuery, Databricks mirror, Azure SQL mirror

**Also answers:** How do I read a Delta table from a different Databricks workspace? How do I set up CDF for a table in another workspace? How do I ingest from a mirrored database in the workspace? How do I use Change Data Feed with Database mirroring? How do I read mirrored Snowflake/Cosmos DB/BigQuery data?

This pattern works for **any Delta table in another workspace**, including:
- Catalogs owned by other teams
- **Mirrored databases** (Azure SQL, Cosmos DB, Snowflake, Databricks, BigQuery, Oracle, PostgreSQL) — mirrored data lands as Delta tables, so the setup is identical

**Steps:**

1. **Register the external catalog** as a datastore in `databricks_batch_engine/datastores/datastore_<ENV>.json`. For a sibling Unity Catalog catalog, add it under `external_datastores` with `kind: "databricks_catalog"`:
   ```jsonc
   "finance_catalog": {
     "kind": "databricks_catalog",
     "connection_details": {
       "workspace_url": "https://adb-xxx.azuredatabricks.net",
       "catalog_name": "finance_catalog"
     }
   }
   ```
   > 🔴 `Datastore_Name` (`'finance_catalog'` above) is the logical name referenced from metadata. It does not need to match the Unity Catalog catalog name — that comes from `connection_details.catalog_name`. See [How do I add a new catalog or warehouse as a target datastore?](#how-do-i-add-a-new-catalog-or-warehouse-as-a-target-datastore) for full rules.
   >
   > 💡 If you shortcut the table into an **existing** registered catalog, you don't need a new datastore — just use the existing catalog's 3-part name.

2. **Configure metadata** with `use_change_data_feed = 'true'` and reference the datastore in `table_name`:
   ```sql
   (101, 'source_details', 'table_name', 'finance_catalog.dbo.sales'),
   (101, 'watermark_details', 'use_change_data_feed', 'true'),
   (101, 'target_details', 'merge_type', 'merge')
   ```

3. **Run** `Trigger Step Orchestrator_For_Each_Trigger_Step` with your trigger name.

### What logging best practices should I follow?
**Keywords:** logging best practices, log levels, monitoring, application logging, debugging, log messages, info, warn, error, logging standards

For comprehensive logging and monitoring, follow these best practices from [Runtime and Workflows Guide - Application Logging](MONITORING_AND_LOGGING.md#best-practices-for-application-logging):

**Application Logging Best Practices:**
1. **Consistent Usage**: Always use `log_and_print()` instead of standalone `print()` statements for proper log level management
2. **Appropriate Levels**: Use correct log levels based on message severity:
   - `info`: Normal processing information
   - `warn`: Warning messages for non-critical issues
   - `error`: Error messages for critical failures
3. **Meaningful Messages**: Include relevant context like Table_ID, processing step, and data values
4. **Structured Information**: Include key identifiers for log correlation (Table_ID, Trigger_Name, etc.)
5. **Error Context**: When logging errors, include enough context for troubleshooting (stack traces, input values)

**Monitoring Approach:**
- **Application Logs**: Detailed step-by-step processing information for debugging (notebook outputs)
- **Pipeline Logs**: High-level processing metrics and status for monitoring dashboards (`Data_Pipeline_Logs`)
- **Data Quality Logs**: Specific data quality violations and remediation actions (`Data_Quality_Notifications`)

**Example Logging Pattern:**
```python
from helper_functions import log_and_print

# Info level for normal operations
log_and_print(f"Processing Table_ID {table_id} from {source_table}", "info")

# Warning for non-critical issues
log_and_print(f"Schema mismatch detected - auto-evolving schema", "warn")

# Error for failures
log_and_print(f"Failed to process Table_ID {table_id}: {error_msg}", "error")
```

Together, these logging mechanisms provide comprehensive observability across the entire data processing pipeline.

---

## DevOps and CI/CD

### How do I set up CI/CD pipelines?
**Keywords:** CI/CD, continuous integration, continuous deployment, DevOps, automation, Azure DevOps, GitHub Actions, pipeline setup, automated deployment, yaml, deployment pipeline, release pipeline

**Also answers:** How do I automate deployments? How do I deploy metadata automatically? How do I set up Azure DevOps? How do I use GitHub Actions with Databricks?

Use [Workspace_CICD_Guide.md](Workspace_CICD_Guide.md) as the canonical CI/CD setup guide and [DevOps.md](DevOps.md) for broader operating guidance.

Short answer:
- use Git as the source of truth and promote through `dev` → `release/*` → approved `main` (preferably tagged)
- configure branch triggers, environment variables and secrets, and trigger paths in the CI/CD platform you use
- expect the workspace CI/CD flow to handle deployment, ID replacement, metadata deployment, and testing

> The shipped sample automation now splits the wrappers by environment: the DEV release flow runs from `dev`, the QA/UAT orchestration wrapper runs from `release/*`, and the PROD release wrapper runs from `main`. Manual dispatch can still run QA/UAT from another selected ref. If you require tag-only production deployment, add a tag trigger or approval gate on top of the sample YAML.

### What IDs does the Workspace CI/CD pipeline replace automatically?
**Keywords:** auto-replacement, automatic ID replacement, CI/CD replacement, workspace CI/CD, Deploy-WorkspaceArtifacts-And-ReplaceIds, environment ID replacement, notebook ID replacement, catalog ID replacement, warehouse ID replacement, GUID replacement, parameter.yml, manual replacement, token substitution, execute_databricks_notebook, execute_databricks_job, item_id, workspace_id, spark_environment_id

**Also answers:** Do I need to use parameter.yml for environment IDs? Do notebook IDs get replaced automatically? What needs parameter.yml? What gets replaced automatically during deployment? How does the CI/CD pipeline handle different GUIDs across dev/QA/prod?

Use [Workspace_CICD_Guide.md](Workspace_CICD_Guide.md#3-how-id-replacement-works) as the canonical explanation.

Short version:
- the pipeline replaces dev workspace IDs with target-environment IDs by matching artifacts on `displayName`
- workspace, notebook, warehouse, catalog, and cluster IDs that belong to the deployed workspace participate in that flow
- `spark_environment_id` should use the dev GUID in metadata so CI/CD can rewrite it in higher environments
- external items and cross-workspace execution targets should be resolved through per-environment `Datastore_Configuration` notebooks instead of relying on ID replacement

Rule of thumb: if the artifact deploys as part of the workspace, ID replacement usually handles it. If it is external or resolved at runtime, register it in `Datastore_Configuration`.

### What documentation do I update when modifying the accelerator?
**Keywords:** notebook changes, documentation sync, update docs, modify helper functions, extend accelerator, helper functions, batch_processing.py, documentation updates, sync documentation, notebook modifications, new ingestion pattern, new metadata attribute, file options, source options, ingestion changes

**Also answers:** What files need updating when I change notebooks? How do I keep documentation in sync? What do I update when adding a new transformation? How do I extend the accelerator properly? What do I update when adding new ingestion options? How do I add new file format options?

See [Notebook Update Checklist](Notebook_Update_Checklist.md) for the complete step-by-step checklist.

**Quick reference:** New transformations → update `METADATA_REFERENCE.md`, `DATA_TRANSFORMATION_GUIDE.md`, `TRANSFORMATION_PATTERNS_REFERENCE.md`, `validate_metadata_sql.py`. New config attributes → update `METADATA_REFERENCE.md`, `validate_metadata_sql.py`. New ingestion patterns → update `DATA_INGESTION_GUIDE.md`, `METADATA_GENERATION_GUIDE.md`. All changes → add/update unit tests in `integration_tests/notebooks/`.

### What variables do I need to configure for CI/CD pipelines?
**Keywords:** pipeline variables, WORKSPACE_GIT_FOLDER_NAME, DEV_WORKSPACE_ID, QA_WORKSPACE_ID, PROD_WORKSPACE_ID, variable group, databricks_settings, repository variables, secrets, trigger paths

**Also answers:** What is WORKSPACE_GIT_FOLDER_NAME? How do I configure pipeline triggers? Why isn't my pipeline triggering? What variables are required for workspace CI/CD?

Use [Workspace_CICD_Guide.md](Workspace_CICD_Guide.md#5-setup-azure-devops), [Workspace_CICD_Guide.md](Workspace_CICD_Guide.md#6-setup-github-actions), and [Workspace_CICD_Guide.md](Workspace_CICD_Guide.md#7-configure-trigger-paths) as the canonical references.

Short answer:
- Azure DevOps uses the `databricks_settings` variable group model
- GitHub Actions uses repository variables plus environment-scoped variables and secrets
- trigger paths are configured manually, and `WORKSPACE_GIT_FOLDER_NAME` is runtime input rather than part of path evaluation

If a pipeline is not triggering, check the path filters first.

### What's the difference between the DEV, QA/UAT, and PROD workspace CI/CD pipelines?
**Keywords:** orchestration_dev, orchestration_qa, orchestration_prod, workspace_cicd_dev, workspace_cicd_qa, workspace_cicd_prod, dev branch, release branch, main branch, metadata deployment, pipeline triggers, dev vs prod pipeline

**Also answers:** When does the dev pipeline run? When does the QA/UAT pipeline run? When does the prod pipeline run? Why are there three pipelines? Which pipeline deploys to which environment?

Use [Workspace_CICD_Guide.md](Workspace_CICD_Guide.md#2-architecture) for the canonical flow.

Short version:
- the DEV release pipeline should deploy the shared `dev` branch to the dev workspace and run fast feedback tests
- the QA/UAT orchestration pipeline should validate an active `release/*` branch so the candidate stays frozen during testing
- the PROD release pipeline should deploy only from the approved `main` commit, which you can tag if you want an explicit release marker
- the QA/UAT and PROD wrappers both call the shared release template rather than duplicating deployment logic
- DEV skips ID replacement because source and target are the same workspace

> The shipped sample automation now maps the DEV release pipeline to `dev`, the QA/UAT orchestration wrapper to `release/*`, and the PROD release wrapper to `main`. Manual dispatch can still run QA/UAT from another selected ref when needed.

Use the workspace CI/CD guide when you need the full branch-to-environment mapping, deployment sequence, or sample pipeline inventory.

### What integration tests are run during CI/CD?
**Keywords:** integration tests, CI/CD tests, automated testing, stored procedure tests, pipeline tests, notebook tests, test coverage, test categories

**Also answers:** What tests does the CI/CD pipeline run? How do I know what's being tested? What happens if tests fail? Where are the tests located?

Use [Workspace_CICD_Guide.md](Workspace_CICD_Guide.md#9-testing) as the canonical reference.

Short version:
- three suites run in parallel: stored procedure tests, pipeline integration tests, and notebook integration tests
- DEV can run the same suites for fast feedback, while QA is the promotion gate
- tests are isolated using patterns like negative `Table_ID` values and dedicated test file locations
- failures in the gated environment stop promotion

If you need exact suite locations, durations, or local execution details, use [Workspace_CICD_Guide.md](Workspace_CICD_Guide.md#9-testing) and [integration_tests/stored_procedures/README.md](../integration_tests/stored_procedures/README.md).

### Should I run integration tests in production?
**Keywords:** production tests, integration tests prod, PROD tests, test in production, smoke tests, post-deployment validation

**Also answers:** Why don't we run integration tests in PROD? Should I test after deploying to production? What should I do instead of integration tests in production?

No. Integration tests belong in QA, not PROD.

Reason:
- they write test data and consume real compute
- QA already validates the promoted code path
- production validation should rely on smoke checks, monitoring, and real data-quality signals instead of write-heavy test flows

See [Workspace_CICD_Guide.md](Workspace_CICD_Guide.md#9-testing) for the canonical policy.

### Why are my integration tests timing out in the release pipeline?
**Keywords:** integration tests timeout, release pipeline timeout, tests timing out, slow tests, test execution time, compute resources, parallel tests, integration_tests.py, orchestration_qa.yml

**Also answers:** Why do my tests take so long? How do I speed up integration tests? Why is orchestration_qa.yml failing with timeout? How do I fix test timeout errors?

Most timeout issues come from one of three causes:
- constrained cluster capacity
- Spark or notebook contention during the notebook suite
- timeout values that are too aggressive for the current environment

Use [Workspace_CICD_Guide.md](Workspace_CICD_Guide.md#9-testing) as the canonical baseline for expected durations.

Typical fixes:
- retry first if the environment was transiently busy
- scale capacity or reduce contention during release windows
- increase timeout settings in the relevant test runner only when the environment is consistently slower than the default assumptions
- skip a specific suite only as an explicit short-term tradeoff

If timeouts become frequent, treat that as an environment-capacity or pipeline-tuning issue rather than only a flaky test symptom.

### How do I enable a multi-developer environment?
**Keywords:** multi-developer, collaboration, team development, feature workspaces, developer workspaces, team environment, parallel development

**Also answers:** How do multiple developers work together? How do I create feature workspaces? How do I set up team development? How do developers test in isolation?

Use [DevOps.md](DevOps.md#2-enabling-a-multi-developer-environment) as the canonical setup guide.

Short answer:
- after the feature workspace is synced from Git, open `setup_feature_workspace.py`
- set `DATASTORE_NOTEBOOK` and `METADATA_NOTEBOOKS`, then run all cells
- let the notebook discover artifacts, update overrides, execute SQL, and prepare the workspace for isolated testing

Use the DevOps guide for the full branching model, diagrams, and setup sequence.

### Why isn't feature workspace deployment fully automated?
**Keywords:** feature workspace automation, manual steps, service principal, warehouse git sync, automated deployment limitation, platform limitation, setup_feature_workspace.py

**Also answers:** Why do I have to manually sync feature workspaces? Why can't CI/CD create feature workspaces automatically? What's blocking full automation? How do I automate feature workspace setup?

Short answer: Databricks SQL Warehouse Git sync still blocks full service-principal-driven workspace sync, and Git sync is all-or-nothing at the workspace level.

After the workspace is synced from Git, `setup_feature_workspace.py` is the only notebook you need to run: fill in the parameters and use Run All. There should be no separate manual variable-library or ID-copying step after that.

Use [DevOps.md](DevOps.md#steps-after-branching-out) for the full explanation and the supported workflow.

### Should I deploy to a single workspace or multiple workspaces?
**Keywords:** single workspace, multiple workspaces, monolithic deployment, multi-workspace deployment, workspace architecture, deployment pattern, workspace design, workspace strategy, capacity, cluster capacity

**Also answers:** What are the tradeoffs between single and multi-workspace deployment? When should I use multiple workspaces? What's the difference between monolithic and distributed deployment? How do I choose a deployment pattern?

Both are valid, but the built-in accelerator CI/CD path is simplest when compute and orchestration artifacts stay in a single workspace.

Short answer:
- choose a single workspace for the lowest DevOps overhead and built-in ID replacement behavior
- choose multiple workspaces when you need stronger isolation, governance boundaries, or separate capacities
- expect custom deployment orchestration once notebooks, pipelines, warehouses, or environments are split across workspaces

For the full tradeoff discussion, see [DevOps.md](DevOps.md#workspace-cicd), [Workspace_CICD_Guide.md](Workspace_CICD_Guide.md), and [Databricks workspace topology guidance](https://learn.microsoft.com/azure/databricks/admin/workspace/manage-workspaces).

### How do I set up Git repositories if I deployed across multiple workspaces?
**Keywords:** multi-workspace Git, repository structure, Git setup, multiple workspaces, workspace folders, Git organization, repository layout, single workspace Git, repo structure, clone, copy assets

**Also answers:** What should my repository look like? How do I connect my workspace to Git? What files do I copy from the accelerator repo? What's the recommended repo layout?

If you split the platform across multiple Databricks workspaces, keep a single repository with one top-level folder per workspace. Use [DevOps.md](DevOps.md#1-initial-setup--prerequisites) as the canonical reference.

Short answer:
- keep Git-synced artifacts in dedicated workspace folders rather than mixing them with repo-root files
- keep the repository focused on data engineering assets; consumer-facing reports belong in a separate reporting repo
- copy the accelerator automation and CI/CD assets only if you want the built-in deployment flow
- connect each Databricks workspace to the same repo but point it at its own folder

If you want to operationalize multi-workspace CI/CD, also review [Workspace_CICD_Guide.md](Workspace_CICD_Guide.md) because deployment orchestration becomes a per-workspace problem.

### How do I manage metadata table records in Git?
**Keywords:** metadata in Git, version control metadata, Git metadata, idempotency, DELETE INSERT pattern, metadata workflow

See [DevOps Guide - Metadata Table Records](DevOps.md#metadata-table-records) for:
- How to author metadata SQL notebooks
- Idempotency patterns
- Git workflow for metadata changes

### How do I trigger full data reloads?
**Keywords:** full reload, reload data, refresh data, reprocess, reset, full refresh, reload all, rerun, historical load

**Also answers:** How do I reset the watermark? How do I reprocess all data? How do I do a full refresh? How do I reload historical data?

See [DevOps Guide - Trigger Full Reloads](DevOps.md#trigger-full-reloads) for the automated process using CSV files and CI/CD pipelines.

### How do I test metadata changes in my feature workspace before promoting to dev?
**Keywords:** testing, feature workspace, testing changes, validate changes, test environment, sandbox, test metadata, feature testing, setup_feature_workspace.py

Use [DevOps.md](DevOps.md#steps-after-branching-out) as the canonical process.

Short version:
- update the metadata notebook for your trigger
- open `setup_feature_workspace.py`, set the datastore and metadata notebook parameters, and run all cells
- validate with `Trigger Step Orchestrator_For_Each_Trigger_Step`
- promote to dev only after the feature workspace run succeeds

`setup_feature_workspace.py` is rerunnable, handles the feature-workspace overrides for you, and is the standard tool for iterative feature-workspace validation.

### What happens if two developers modify the same Table_ID?
**Keywords:** conflicts, merge conflicts, concurrent changes, duplicate Table_ID, collision, overlap, coordination, Table_ID assignment, Table_ID range, pick Table_ID, choose Table_ID

**Also answers:** How do I pick a Table_ID? What Table_ID should I use? How do I avoid Table_ID collisions? What's the Table_ID naming strategy?

**How to pick a Table_ID:**

Table_IDs are integers that uniquely identify each metadata entity. Use a **range-based strategy** to avoid collisions across developers and data products:

| Range | Assigned To | Example |
|-------|-------------|---------|
| 1–99 | Shared/common tables | Reference data, date dimensions |
| 100–199 | Sales data product | `101` = orders, `102` = customers |
| 200–299 | Finance data product | `201` = GL entries, `202` = budgets |
| 300–399 | HR data product | `301` = employees, `302` = positions |
| 1000–1999 | Developer A (feature work) | Temporary during development |
| 2000–2999 | Developer B (feature work) | Temporary during development |
| Negative values | Integration tests only | `-99999` = test isolation |

> **Tip:** Document your Table_ID ranges in a shared wiki or README so all team members know which ranges are available.

**Practices to avoid conflicts:**
1. **Coordinate**: Use different Table_ID ranges per developer and data product
2. **Use Git**: Git will detect conflicting changes in metadata SQL files — resolve before merge
3. **Feature workspaces**: Each developer tests in their own feature workspace
4. **Review PRs**: DevOps team reviews pull requests for Table_ID conflicts before merging
5. **Idempotent SQL**: Metadata SQL files use DELETE + INSERT pattern, so last merge wins

### How do I revert a metadata change that was deployed to production?
**Keywords:** revert, rollback, undo, restore, revert changes, roll back, undo deployment, fix mistake, restore previous version

Use [DevOps.md](DevOps.md#handling-qa-defects-and-hotfixes) as the canonical release-management reference.

Short answer:
- cut a `hotfix/*` or revert branch from `main` or the current production tag
- validate it in QA/UAT or on the active `release/*` branch
- merge the approved fix into `main`, tag the new production commit, and forward-merge it into `dev`

Avoid direct production edits unless you have no other recovery path.

### How do I deploy volume shortcuts via CI/CD?
**Keywords:** shortcuts, shortcut deployment, CI/CD shortcuts, workspace-cicd, enable_shortcut_publish, feature flag, workspace config, dynamic shortcuts, environment-specific shortcuts, ADLS Gen2, external volume shortcuts, connection ID, workspace variables

**Also answers:** How do I make shortcuts work across dev/QA/prod? Do I need a separate config for shortcuts? How do shortcuts get deployed to other environments? How do I handle environment-specific connection IDs in shortcuts?

Use [DevOps Guide - Shortcuts](DevOps.md#shortcuts) as the canonical reference.

Short answer:
- shortcut publishing through `workspace-cicd` is already enabled for catalog-defined shortcuts
- use a dedicated `VL_Shortcut_Variables` library with environment value sets for shortcut-specific values
- do not reuse `VL_Workspace_Variables` for shortcut variables because it follows the CI/CD ID-replacement pattern, not the runtime value-set pattern
- use the CSV-based shortcut workflow instead when you need REST-driven bulk management across workspaces

Use the DevOps guide for the full setup, the CSV-versus-`workspace-cicd` comparison, and the current preview limitations.

---

### How does file change detection work in CI/CD pipelines?
**Keywords:** Get-UpdatedFilePaths, git diff, file detection, changed files, re-run, rerun, failed pipeline, merge strategy, squash merge, merge commit, rebase merge, commit SHA, BeforeSha, GITHUB_OUTPUT, output variables, shortcuts pipeline, data access pipeline, full reloads pipeline

**Also answers:** How does the pipeline know which files changed? What happens if I re-run a failed pipeline? Do I need to re-update the file after a failure? What if another commit lands after a failure? Does this work with merge commits? Do I need to squash merge?

Several CI/CD pipelines (Shortcuts, Data Access Control, Trigger Full Reloads, Workspace Deployment, etc.) use `Get-UpdatedFilePaths.ps1` to detect which files changed in the current push and route them to the correct environment.

**How it works:**
1. The script runs `git diff --name-only <BeforeSha> <CommitSha>` to find all changed files between the previous branch head and the current commit
2. Filters results to match a specific file pattern (e.g., `workspaces/*/*/*_shortcuts.csv`)
3. Optionally splits results into dev/qa/prod buckets based on folder paths
4. Outputs the file lists as pipeline variables (ADO `##vso` or GitHub `$GITHUB_OUTPUT`)

**Merge strategy support:**

The script works with **any merge strategy** — squash merge, regular merge commit, or rebase merge:

| Platform | How `BeforeSha` is resolved | Merge commit | Squash merge | Rebase merge |
|----------|---------------------------|:---:|:---:|:---:|
| **GitHub Actions** | `github.event.before` (passed explicitly) | ✅ | ✅ | ✅ |
| **Azure DevOps** | Falls back to `CommitSha~1` (first parent) | ✅ | ✅ | ✅ |

> 💡 **Why this works for all merge types:** Instead of inspecting a single commit tree (which produces a combined diff that hides changes on merge commits), the script diffs between the branch head *before* and *after* the push. This captures all introduced changes regardless of how the commits were structured.

**Re-running failed pipelines:**

| Scenario | What happens | Result |
|----------|-------------|--------|
| **"Re-run all jobs"** from original run | `get-updated-files` re-runs with same SHA pair | ✅ Same files detected |
| **"Re-run failed jobs"** from original run | `get-updated-files` outputs preserved from original run | ✅ Downstream jobs use original file list |
| **New commit lands, original pipeline re-run** | Original run still uses original SHA pair | ✅ Not affected by new commits |
| **New commit lands, new pipeline auto-triggers** | Path filter prevents triggering if new commit didn't touch matching files | ✅ Won't run with wrong SHA |

> 💡 **Best practice:** If a deployment fails, always use **"Re-run all jobs"** or **"Re-run failed jobs"** from the original GitHub Actions run (or re-queue the original Azure DevOps build). Don't trigger a new pipeline run manually — if other commits have landed since, the new run would diff against the wrong parent.

---

### What naming conventions are used in CI/CD pipelines?
**Keywords:** naming convention, snake_case, camelCase, PascalCase, UPPER_SNAKE_CASE, parameter names, variable names, output variables, pipeline variables, PowerShell parameters

**Also answers:** Why are some variables snake_case and others UPPER_SNAKE_CASE? What naming convention should I use for new parameters? Are ADO and GitHub Actions pipelines consistent?

See [DevOps Guide — Naming Conventions](DevOps.md#naming-conventions) for the canonical naming table covering pipeline parameters, output variables, environment variables, PowerShell parameters, template parameters (ADO), reusable workflow inputs (GH), and stage/job names.

> 💡 **When adding new parameters**, follow the convention for the element type. Pipeline parameters and output variables use `snake_case`. PowerShell script parameters use `PascalCase`. Environment/repository variables use `UPPER_SNAKE_CASE`.

---

### How do I generalize the CI/CD pipeline to deploy multiple workspaces?
**Keywords:** generalize CI/CD, generic workspace, multi-workspace CI/CD, workspace mapping, workspace_mapping.csv, variable sprawl, non-accelerator workspace, reuse pipeline, deploy any workspace

**Also answers:** How do I deploy non-accelerator workspaces with the same pipeline? How do I avoid creating variables for every workspace? How do I scale CI/CD to many workspaces? Can I use the pipeline for workspaces that don't use the accelerator?

Use [Generalizing the Workspace CI/CD Pipeline](Generalizing_Workspace_CICD.md) as the canonical design reference.

Short answer:
- replace per-workspace CI/CD variables with a repo-managed workspace mapping CSV
- use that CSV to map git folders to dev, QA, and prod workspaces plus feature flags
- keep authentication in secrets or service connections rather than in the CSV

Add a workspace by adding a row to the mapping file.

---

## Data Engineering

### How do I ingest data from an Oracle database?
**Keywords:** Oracle, database ingestion, ingest from Oracle, extract from Oracle, Oracle connection, database source, external database

See [Runtime and Workflows Guide - Ingest data from an external database](DATA_INGESTION_GUIDE.md#ingest-data-from-an-external-database-eg-azure-sql-oracle-postgresql-into-a-delta-table) for complete configuration steps including watermarks and incremental loading.

### How do I fix Oracle ParquetInvalidDecimalPrecisionScale errors (Precision 38 Scale 127)?
**Keywords:** Oracle error, ParquetInvalidDecimalPrecisionScale, Precision 38 Scale 127, NUMBER scale 127, Oracle decimal error, CSV staging fallback

**Also answers:** Why is Oracle NUMBER failing with scale 127? How do I avoid Parquet decimal scale errors in Oracle copy? Is there a CSV staging workaround?

If Oracle ingestion fails with `ParquetInvalidDecimalPrecisionScale` (for example `Precision: 38 Scale: 127`), use this priority order:

1. **Preferred (Option A)**: Update `source_details.query` to `CAST` problematic Oracle `NUMBER` columns to explicit precision/scale.
2. **Fallback (Option B)**: Use the CSV staging pattern and convert to Parquet after staging:
   - `(Table_ID, 'source_details', 'copy_pattern_suffix', 'csv')`
   - `(Table_ID, 'source_details', 'convert_csv_to_parquet', 'true')`

This keeps metadata/watermark routing stable because `source_details.source` stays `oracle`.

> ⚠️ **Use one strategy per Table_ID**: either Option A (CAST in query) **or** Option B (CSV fallback pattern). Do not mix both unless you have a specific connector behavior that requires it.

**Option A example (CAST in query):**
```sql
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
   (101, 'source_details', 'source', 'oracle'),
   (101, 'source_details', 'datastore_name', 'oracle_sales'),
   (101, 'source_details', 'schema_name', 'SALES_SCHEMA'),
   (101, 'source_details', 'table_name', 'SALES'),
   (101, 'source_details', 'query', 'SELECT CAST(CUST_ID AS NUMBER(38,0)) AS CUST_ID, CAST(CREDIT_LIMIT AS NUMBER(38,18)) AS CREDIT_LIMIT FROM SALES_SCHEMA.SALES WHERE MODIFIED_DATE > TIMESTAMP ''{WATERMARKVALUE}'' ')
```

**Option B example (CSV staging fallback):**
```sql
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
   (101, 'source_details', 'source', 'oracle'),
   (101, 'source_details', 'datastore_name', 'oracle_sales'),
   (101, 'source_details', 'schema_name', 'SALES_SCHEMA'),
   (101, 'source_details', 'table_name', 'SALES'),
   (101, 'source_details', 'query', 'SELECT * FROM SALES_SCHEMA.SALES WHERE MODIFIED_DATE > TIMESTAMP ''{WATERMARKVALUE}'' '),
   (101, 'source_details', 'copy_pattern_suffix', 'csv'),
   (101, 'source_details', 'convert_csv_to_parquet', 'true')
```

### How does incremental loading work?
**Keywords:** incremental, watermark, incremental copy, incremental load, delta loading, change tracking, CDC, change data capture, incremental ingestion, WATERMARKVALUE, high watermark, last modified

**Also answers:** How do watermarks work? What is {WATERMARKVALUE}? How do I avoid reloading all data? How do I only load new records? How does the accelerator track what data has been processed?

The accelerator supports three incremental loading methods depending on your source type:

| Source Type | Method | Configuration |
|-------------|--------|---------------|
| **External DBs** (Oracle, SQL Server, etc.) | Watermark column with `{WATERMARKVALUE}` placeholder | `watermark_details` category |
| **Files** (CSV, JSON, Excel, XML, Parquet) | Automatic file `last_modified` timestamp | No configuration needed |
| **Delta Tables** (Bronze/Silver/Gold) | Watermark column OR Change Data Feed | `watermark_details` category OR enable CDF |

**How Watermark Columns Work:**
1. **First run**: Full load (no watermark exists yet — defaults to `1900-01-01` for datetime columns, `-1` for integer columns)
2. **After success**: Maximum value of the watermark column is saved to `Data_Pipeline_Logs`
3. **Subsequent runs**: The `{WATERMARKVALUE}` placeholder is replaced with the stored value
4. **Only new data**: Records where watermark column > stored value are extracted

**Watermark Data Types:**
- `datetime`: Use quotes in query — `WHERE col > '{WATERMARKVALUE}'`
- `numeric`: No quotes — `WHERE col > {WATERMARKVALUE}`

**Delta Table Bonus - Multiple Watermark Columns:**
For Delta tables only, you can specify comma-separated columns (e.g., `'ModifiedDate, CreatedDate'`). The framework uses OR logic to capture rows where ANY column exceeds the watermark.

See also:
- [Incremental file ingestion](#how-does-incremental-copy-work-when-ingesting-files)
- [Change Data Feed for Delta tables](#what-is-change-data-feed-and-when-should-i-use-it)
- [Late-arriving data handling](#how-do-i-handle-late-arriving-data-with-watermarks)
- [Runtime and Workflows Guide - Incremental Load with Watermark](DATA_INGESTION_GUIDE.md#incremental-load-with-watermark-recommended-for-large-tables)

### How do I handle file ingestion from the volume?
**Keywords:** file ingestion, CSV, JSON, Excel, XML, files, file sources, load files, read files, file data, shortcuts

**Also answers:** How do I load CSV files? How do I ingest JSON? How do I read Excel files? How do I process XML? How do I ingest files from shortcuts?

See [Runtime and Workflows Guide - Ingest file data from UC Volumes](DATA_INGESTION_GUIDE.md#ingest-file-data-from-the-catalog-files-section-into-a-delta-table) for CSV, JSON, Excel, and XML file configurations.

### How does incremental copy work when ingesting files?
**Keywords:** incremental files, file timestamps, last_modified, incremental file loading, change detection, file tracking, delta files

Each run writes the most recent file `last_modified` timestamp to the metadata logs. The next ingestion compares incoming files against that stored timestamp and only processes files whose `last_modified` value is greater than the recorded value, so you avoid re-reading previously ingested files. See the incremental file ingestion notes in [Runtime and Workflows Guide - Ingest file data from UC Volumes](DATA_INGESTION_GUIDE.md#ingest-file-data-from-the-catalog-files-section-into-a-delta-table) for configuration details.

### What file formats are supported and what options are available?
**Keywords:** file formats, CSV, JSON, Excel, XML, Parquet, file options, delimiter, encoding, multiline, sheet_name, header row, file ingestion options

**Also answers:** What file types can I ingest? How do I configure CSV options? How do I read Excel files? What encoding options are available? How do I handle multi-line JSON?

The accelerator supports these file formats with format-specific options:

| Format | Extensions | Key Options |
|--------|------------|-------------|
| **CSV** | `.csv`, `.tsv`, `.txt` | `delimiter`, `header`, `encoding`, `multiline` |
| **JSON** | `.json` | `encoding`, `multiline` |
| **Parquet** | `.parquet` | `allow_missing_columns` (schema merge) |
| **Excel** | `.xls`, `.xlsx` | `sheet_name` (required) |
| **XML** | `.xml` | `xpath`, `namespaces` (natively supported via pandas `read_xml`) |

**Common Configuration Options:**

| Option | Configuration | Default | Description |
|--------|---------------|---------|-------------|
| **Delimiter** | `source_details.delimiter` | `,` | Field separator for CSV/TSV |
| **Header Row** | `source_details.file_has_header_row` | `true` | Whether first row contains column names |
| **Encoding** | `source_details.encoding` | `UTF-8` | Character encoding (UTF-8, UTF-16, ISO-8859-1, etc.) |
| **Multiline** | `source_details.multiline` | `false` | Allow records to span multiple lines |
| **Sheet Name** | `source_details.sheet_name` | (required) | Excel sheet: single name, comma-separated list, or `*` for all |
| **Allow Missing Columns** | `source_details.allow_missing_columns` | `false` | Merge schemas across files with different columns |

**Excel Multi-Sheet Example:**
```sql
-- Read all sheets from Excel file
(101, 'source_details', 'sheet_name', '*'),

-- Read specific sheets
(102, 'source_details', 'sheet_name', 'Sales,Inventory,Returns'),

-- Read single sheet
(103, 'source_details', 'sheet_name', 'Q1_Data')
```

**CSV with Custom Options:**
```sql
(104, 'source_details', 'wildcard_folder_path', 'data/*.csv'),
(104, 'source_details', 'delimiter', '|'),
(104, 'source_details', 'encoding', 'ISO-8859-1'),
(104, 'source_details', 'file_has_header_row', 'true'),
(104, 'source_details', 'multiline', 'true')
```

> **Note:** The `delta__raw_folderpath` column is automatically added during file ingestion to track the source file path for each record.

### How does batch vs. sequential file reading work?
**Keywords:** batch reading, sequential reading, process_one_file_at_a_time, file reading method, batch mode, sequential mode, mixed file types, multiple file formats, performance, file ingestion mode

**Also answers:** When should I use sequential file reading? What's the difference between batch and sequential? How do I read files one at a time? Why are my files being read sequentially? How do I process mixed file types?

The accelerator supports two file reading modes, controlled by the `process_one_file_at_a_time` configuration:

| Mode | Default For | How It Works | Performance |
|------|-------------|--------------|-------------|
| **Batch** (default) | CSV, TSV, TXT, JSON, Parquet | Passes all file paths to Spark's native reader in a single action — Spark parallelizes reads across executors | ⚡ Fast |
| **Sequential** | XML, Excel (always) | Reads files one at a time in a loop, merging via `unionByName(allowMissingColumns=<config>)` | 🐢 Slower but safer |

**When to use sequential mode (`process_one_file_at_a_time = 'true'`):**

Sequential mode should primarily be used when your **data source contains multiple file types** — for example, a folder with both Parquet and CSV files. In batch mode, Spark uses a single reader type, so it can't handle mixed formats in one pass. Sequential mode reads each file individually, detecting the correct format per file.

Other valid use cases:
- Files with **inconsistent schemas** across batches (sequential uses `unionByName` with `allowMissingColumns` controlled by `fail_on_new_schema` — defaults to allowing missing columns)
- **Memory-constrained** environments where processing all files at once causes OOM errors
- Need for **per-file error isolation** (a bad file won't fail the entire batch)

**Configuration:**
```sql
-- Force sequential reading for a specific Table_ID
(100, 'source_details', 'process_one_file_at_a_time', 'true')
```

**How the decision is made internally:**
```
Is format in {csv, tsv, txt, json, parquet}?
  ├── YES → Is process_one_file_at_a_time = true?
  │         ├── YES → Sequential
  │         └── NO  → Batch (default)
  └── NO (xml, excel) → Always Sequential
```

> **Don't confuse file reading mode with batch writing.** The `merge_in_batches_with_columns` configuration controls how data is *written* to the target Delta table (splitting writes into sequential batches by column values). File reading mode controls how data is *read* from source files.

### What is Change Data Feed and when should I use it?
**Keywords:** change data feed, CDF, incremental loading, delta changes, transaction tracking, change tracking, CDC, change data capture

**Also answers:** How do I capture deletes? How do I track all changes? What is CDF? When should I use CDF vs watermarks? How do I get delete events from Delta tables?

Change Data Feed (CDF) is Delta Lake's built-in change tracking feature that captures all inserts, updates, and deletes at the transaction level. Use CDF instead of traditional watermark columns when:

**When to Use CDF:**
- ✅ **Reliable change tracking**: Need to capture ALL changes without depending on business logic
- ✅ **Delete capture**: Need to track deleted records (not possible with traditional watermarks)
- ✅ **Simplified configuration**: Don't want to identify/maintain watermark columns
- ✅ **Complete audit trail**: Need transaction-level history with timestamps
- ✅ **Complex update patterns**: Data updates don't follow predictable timestamp patterns

**When to Use Traditional Watermarks:**
- Source is an external database (Oracle, SQL Server, etc.) - CDF only works with Delta tables
- Source is file-based (CSV, JSON, etc.)
- Simple append-only patterns where watermarks are straightforward
- Storage constraints (CDF adds overhead for change tracking)

**Key Benefits:**
- Uses `_commit_version` for watermarking (transaction-level tracking)
- Automatically captures deletes without soft-delete columns
- Transaction-level guarantees prevent missing changes
- No concerns about late-arriving data or out-of-order updates
- Framework automatically filters out `update_preimage` rows (only includes insert, update_postimage, delete)
- `_commit_timestamp` available in data for logging/observability

**Important Considerations:**
- CDF returns **one row per change event** - if a record is updated 3 times between runs, you get 3 rows with the same primary key
- **Recommended**: Add `drop_duplicates` transformation with `column_name='<primary_keys>'` and `order_by='_commit_version'` to keep only the most recent change
- Without deduplication, your merge may process the same primary key multiple times (last one wins, but adds processing overhead)
- **CDF system columns are automatically filtered out during merge** — the framework removes `_change_type`, `_commit_version`, and `_commit_timestamp` from the DataFrame before writing to the target table, so they won't appear in your Bronze/Silver/Gold tables

See [Runtime and Workflows Guide - Transform delta tables using Change Data Feed](DATA_TRANSFORMATION_GUIDE.md#transform-delta-tables-using-change-data-feed-cdf) for complete configuration steps and examples.

### How do I enable Change Data Feed on my Delta tables?
**Keywords:** enable CDF, setup change data feed, configure CDF, activate change tracking, CDF setup, enable change tracking

You can enable Change Data Feed when creating a new table or on existing tables:

**For New Tables:**
```sql
CREATE TABLE silver.dbo.sales (
    sale_id INT,
    amount DECIMAL(10,2),
    sale_date DATE
)
TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

**For Existing Tables:**
```sql
ALTER TABLE silver.dbo.sales
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

**Verify CDF is Enabled:**
```sql
DESCRIBE DETAIL silver.dbo.sales;
-- Look for: enableChangeDataFeed = true in properties
```

**Important Notes:**
- Enabling CDF on existing tables only tracks **future** changes, not historical data
- CDF adds storage overhead for change tracking - monitor your storage usage
- CDF data is retained based on `delta.deletedFileRetentionDuration` (default 7 days)
- Once enabled, configure your metadata with `watermark_details.use_change_data_feed = 'true'`

See [Runtime and Workflows Guide - Transform delta tables using Change Data Feed](DATA_TRANSFORMATION_GUIDE.md#transform-delta-tables-using-change-data-feed-cdf) for complete setup and configuration.

### How do I write files to date-partitioned folders (YYYY/MM/DD)?
**Keywords:** date partitioned folders, YYYY/MM/DD, file output, date folders, partition by date, dynamic path, date substitution, folder structure, file export, output_file, CSV export, Parquet export, Excel export, Target_Entity

**Also answers:** Can I write files to date-based folder structures? How do I create date-partitioned file outputs? Is there a way to bring files into root/YYYY/MM/dd/*.csv? How do I organize output files by date?

Put a **folder path with `/`** in the `Target_Entity` column of the Orchestration table. The system automatically sets `merge_type = 'output_file'`. Paths are relative to `Files/` in the target datastore.

**Date placeholders:** `%Y` (year), `%m` (month), `%d` (day), `%B` (full month name), `%b` (abbrev month), `%H` (hour), `%M` (minute), `%S` (second), plus `%f`, `%j`, `%W`, `%A`, `%a`.

**Example:**
```sql
-- Orchestration table: Target_Entity with date placeholders
('DailyExport', 1, 101, 'bronze', 'exports/%Y/%m/%d/data.csv', '', 'batch', 1)
```
Result on Jan 19, 2026: `Files/exports/2026/01/19/data.csv`

**Supported formats:** `.csv`, `.txt` (Spark), `.parquet` (Spark), `.json`/`.jsonl`/`.ndjson` (Spark), `.xls`/`.xlsx` (Pandas — ~1M row limit). CSV delimiter options: `,` (default), `\t`, `|`, `;`. Configure via `target_details` / `delimiter` and `sheet_name` in Primary Config.

See [METADATA_REFERENCE.md](METADATA_REFERENCE.md) for all `target_details` attributes.

### What system columns does the accelerator create automatically?
**Keywords:** system columns, delta columns, delta__created_datetime, delta__modified_datetime, delta__raw_folderpath, delta__schema_id, lineage, audit

**Also answers:** What metadata columns are added? What is delta__created_datetime? What is delta__modified_datetime? How do I track when records were created? How do I trace file lineage?

The notebooks add lineage columns so you do not have to model them in metadata:

- **`delta__created_datetime`** – `helper_functions_1.py.sqladd_timestamp_metadata_columns` adds this timestamp the first time a row lands in a catalog/warehouse table. It is persisted for Bronze, Silver, and Gold loads so you always know when the row was originally ingested.
- **`delta__modified_datetime`** – The same helper now stamps this column for **every** medallion layer. It records the time of the current write operation and is recalculated on every write so you can trace the latest successful merge even in Bronze tables.
- **`delta__raw_folderpath`** – File ingestion helpers in `helper_functions_2.py.sql` attach the originating file path to every record (via `input_file_name()` for bulk reads or a literal path when staging data). This value travels with the row into Bronze/Silver tables, making it easy to trace a record back to the physical file that produced it.
- **`delta__schema_id`** – When data is written to Bronze or Silver tables, `finalize_processing` in `helper_functions_1.py.sql` stamps the schema hash into this column. The value ties each batch to the snapshot stored in `Schema_Logs`, so schema drift investigations can be correlated with specific records.

### What built-in transformations are available?
**Keywords:** transformations, built-in transformations, data transformation steps, derived_column, filter_data, join_data, aggregate_data, pivot, unpivot, window function, rename columns, transformation list

**Also answers:** What transformations can I use? How do I transform data? What operations are supported? How do I rename columns? How do I filter data? How do I join tables?

The accelerator provides 30 built-in transformations configured via `data_transformation_steps` in Advanced Configuration. Use `Configuration_Name_Instance_Number` to control execution sequence.

| Transformation | Purpose | Key Attributes |
|----------------|---------|----------------|
| **`columns_to_rename`** | Rename columns | `existing_column_name`, `new_column_name` |
| **`derived_column`** | Create calculated columns using SQL expressions | `column_name`, `expression` |
| **`filter_data`** | Filter rows using SQL WHERE conditions | `filter_logic` |
| **`remove_columns`** | Drop columns from output | `column_name` |
| **`select_columns`** | Keep only specified columns | `column_name`, `retain_metadata_columns` |
| **`change_data_types`** | Cast columns to different types | `column_name`, `new_type` |
| **`drop_duplicates`** | Remove duplicate rows | `column_name`, `order_by`, `order_direction` |
| **`create_surrogate_key`** | Generate surrogate keys for dimension tables | `column_name` |
| **`attach_dimension_surrogate_key`** | Attach surrogate keys from dimension tables | `dimension_table_name`, `dimension_table_join_logic`, `dimension_table_key_column_name` |
| **`join_data`** | Join with another table | `table_name`, `join_type`, `join_condition` |
| **`union_data`** | Union with another table | `table_name` |
| **`aggregate_data`** | GROUP BY with aggregations | `group_by_columns`, `aggregations` |
| **`add_window_function`** | Window functions (ROW_NUMBER, LAG, etc.) | `function`, `partition_by`, `order_by`, `output_column_name` |
| **`pivot_data`** | Rows to columns | `group_columns`, `pivot_column`, `value_columns`, `aggregation` |
| **`unpivot_data`** | Columns to rows | `id_columns`, `unpivot_columns`, `variable_column_name`, `value_column_name` |
| **`sort_data`** | Order rows | `column_name`, `sort_direction` |
| **`transform_datetime`** | Date/time transformations (extract parts, format, diff) | `column_name`, `operation`, `output_column_name` |
| **`explode_array`** | Expand array column to rows | `column_name`, `output_column_name`, `preserve_nulls` |
| **`flatten_struct`** | Expand struct fields to columns | `column_name`, `prefix`, `fields_to_include` |
| **`split_column`** | Split string into multiple columns | `column_name`, `delimiter`, `output_column_names` |
| **`concat_columns`** | Concatenate multiple columns into one | `column_name`, `output_column_name`, `separator` |
| **`conditional_column`** | CASE WHEN logic for conditional values | `column_name`, `conditions`, `output_column_name` |
| **`string_functions`** | String transformations (upper, lower, trim, etc.) | `column_name`, `function`, `output_column_name` |
| **`apply_null_handling`** | Replace NULLs with default values | `column_name`, `default_value` |
| **`replace_values`** | Replace specific values across columns | `column_name`, `old_value`, `new_value` |
| **`mask_sensitive_data`** | Mask PII data (email, phone, SSN, etc.) | `column_name`, `upper_char`, `lower_char`, `digit_char` |
| **`entity_resolution`** | Match and merge records across sources | `column_name`, `match_type`, `threshold` |
| **`normalize_text`** | Standardize text (trim, case, unicode) | `column_name`, various text options |
| **`add_row_hash`** | Generate hash from column values | `column_name`, `output_column_name` |
| **`custom_transformation_function`** | Call custom PySpark function | `function_name`, `notebook_name` |

**Example - Multi-Step Transformation:**
```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
-- Step 1: Rename columns
(101, 'data_transformation_steps', 'columns_to_rename', 1, 'existing_column_name', 'cust_nm, ord_dt'),
(101, 'data_transformation_steps', 'columns_to_rename', 1, 'new_column_name', 'customer_name, order_date'),

-- Step 2: Filter active records
(101, 'data_transformation_steps', 'filter_data', 2, 'filter_logic', 'status = ''Active'''),

-- Step 3: Create calculated column
(101, 'data_transformation_steps', 'derived_column', 3, 'column_name', 'total_amount'),
(101, 'data_transformation_steps', 'derived_column', 3, 'expression', 'quantity * unit_price'),

-- Step 4: Join with dimension
(101, 'data_transformation_steps', 'join_data', 4, 'table_name', 'silver.dbo.dim_product'),
(101, 'data_transformation_steps', 'join_data', 4, 'join_type', 'left'),
(101, 'data_transformation_steps', 'join_data', 4, 'join_condition', 'a.product_id = b.product_id')
```

See [Runtime and Workflows Guide - Built-in Transformations](DATA_TRANSFORMATION_GUIDE.md#execute-pre-built-transformations) and [Complex Transformation Patterns](Complex_Transformation_Patterns.md) for detailed examples.

### How do I convert TimestampNTZ values to timestamps with the correct time zone?
**Keywords:** TimestampNTZ, timestamp_ntz, timezone, time zone, UTC, local time, derived_column, change_data_types, to_utc_timestamp, from_utc_timestamp, timestamp conversion

**Also answers:** How do I handle timezone-less timestamps? Should I use `change_data_types` or `derived_column`? How do I convert local time to UTC? How do I create a reporting-zone timestamp? How do I avoid corrupting timestamps?

Use `derived_column` for time zone-aware conversion. Do **not** rely on `data_cleansing`, and do **not** assume a plain `change_data_types` cast to `timestamp` is enough when the source value has no time zone.

**Why:**
- `TimestampNTZ` stores a date/time value without time zone semantics.
- `change_data_types` can change the column type, but it does not tell Spark what time zone the original wall-clock value represents.
- `derived_column` is the right tool here because it runs a Spark SQL expression via `expr(...)`, so you can use functions like `to_utc_timestamp(...)` and `from_utc_timestamp(...)`.

**Best practice:**
1. Keep the original source column for lineage.
2. Add a new normalized column such as `created_utc` or `created_pacific`.
3. Only overwrite the original column if the source time zone is explicitly documented.
4. If the source time zone is unknown, do not invent one in metadata. Preserve the original value and resolve the source contract first.

**Use these patterns:**

- **Source value is local wall-clock time in a known zone; normalize to UTC**

```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
   (101, 'data_transformation_steps', 'derived_column', 4, 'column_name', 'created_utc'),
   (101, 'data_transformation_steps', 'derived_column', 4, 'expression', 'to_utc_timestamp(created_ntz, ''America/New_York'')');
```

- **Source value is already UTC; create a reporting-zone column**

```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
   (101, 'data_transformation_steps', 'derived_column', 5, 'column_name', 'created_pacific'),
   (101, 'data_transformation_steps', 'derived_column', 5, 'expression', 'from_utc_timestamp(created_utc, ''America/Los_Angeles'')');
```

**When should I use `change_data_types`?**

Use `change_data_types` when you only need a type conversion and the time zone semantics are already handled elsewhere. If you need to interpret a `TimestampNTZ` value as being in a specific source time zone, use `derived_column` first.

> **Rule of thumb:** `change_data_types` changes the data type. `derived_column` handles time zone meaning.

### How does column name cleansing work?
**Keywords:** column name cleansing, column_cleansing, column standardization, rename columns, lowercase columns, replace special characters, column naming, regex column names

**Also answers:** Why did my column names change? How do I standardize column names? How do I control column naming? What order are column transformations applied? How do I use regex for column names?

Column name cleansing standardizes column names during ingestion. The transformations are applied **in this exact order**:

1. **Trim whitespace** (`column_cleansing.trim`) - Remove leading/trailing spaces
2. **Exact string replacement** (`column_cleansing.exact_find`, `exact_replace`) - Replace specific strings
3. **Regex replacement** (`column_cleansing.regex_find`, `regex_replace`) - Pattern-based replacement
4. **Replace non-alphanumeric** (`column_cleansing.replace_non_alphanumeric_with_underscore`) - Replace special chars with `_`
5. **Collapse underscores** - Automatically reduces `__` to `_` (only if step 4 is enabled)
6. **Apply case** (`column_cleansing.apply_case`) - Convert to `lower`, `upper`, or `title` case

**Default Behavior:**

Column cleansing has **no source-type-specific defaults**. All configurations default to:
- `apply_case`: empty (no case change)
- `replace_non_alphanumeric_with_underscore`: `false`

You must explicitly configure any cleansing behavior you want:

| Setting | Values | Description |
|---------|--------|-------------|
| `trim` | `true`/`false` | Trim whitespace from column names |
| `apply_case` | `lower`, `upper`, `title`, or empty | Case conversion |
| `replace_non_alphanumeric_with_underscore` | `true`/`false` | Replace `@#$%` etc. with `_` |
| `exact_find` | Comma-separated strings | Strings to find (e.g., `'ID,Num'`) |
| `exact_replace` | Single value or comma-separated | Replacement(s) (e.g., `'_id,_number'`) |
| `regex_find` | Regex pattern | Pattern to match (e.g., `'\\d+$'`) |
| `regex_replace` | Replacement string | Replacement for regex matches |

**Example - Custom Column Cleansing:**
```sql
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
-- Override defaults for a table source
(101, 'column_cleansing', 'apply_case', 'lower'),
(101, 'column_cleansing', 'replace_non_alphanumeric_with_underscore', 'true'),

-- Remove common prefixes
(101, 'column_cleansing', 'exact_find', 'tbl_,col_'),
(101, 'column_cleansing', 'exact_replace', ''),

-- Remove trailing numbers using regex
(101, 'column_cleansing', 'regex_find', '_\\d+$'),
(101, 'column_cleansing', 'regex_replace', '')
```

**Result:** Column `TBL_Customer_Name_01` becomes `customer_name`

> **Note:** Column cleansing is separate from data cleansing (`data_cleansing` category), which handles whitespace trimming and blank-to-null conversion in column **values**, not names.

### How do I create a dimension table with SCD Type 2?
**Keywords:** SCD Type 2, dimension table, slowly changing dimension, SCD2, historical tracking, dimension, versioning, effective dates, history, is_current, valid_from, valid_to

**Also answers:** How do I track historical changes? How do I maintain dimension history? How do I implement slowly changing dimensions? How do I version dimension records?

See [Runtime and Workflows Guide - Loading data into a dimension table](DATA_MODELING_GUIDE.md#loading-data-into-a-dimension-table) for SCD2 configuration and examples.

### How does SCD Type 2 change detection work?
**Keywords:** SCD2 change detection, hash comparison, scd_start_date, scd_end_date, scd_active, SCD columns, dimension history, change tracking, SCD internals

**Also answers:** What columns does SCD2 add? How does the accelerator detect dimension changes? What is scd_active? How are SCD2 dates managed?

The accelerator implements SCD Type 2 using **hash-based change detection** for efficiency. Here's how it works:

**SCD2 System Columns (added automatically):**

| Column | Type | Description |
|--------|------|-------------|
| `scd_start_date` | TIMESTAMP | When this version became effective |
| `scd_end_date` | TIMESTAMP | When this version was superseded (NULL = current) |
| `scd_active` | INT | `1` = current version, `0` = historical version |

**Change Detection Process:**

1. **Load existing active records**: Query target table for `scd_active = 1`
2. **Hash comparison**: Calculate hash (Spark's `f.hash()` / MurmurHash3) of all non-system columns for both new and existing data
3. **Identify changes**: Compare hashes to find:
   - **New records**: Primary key doesn't exist in target
   - **Changed records**: Primary key exists but hash differs
   - **Unchanged records**: Primary key exists and hash matches (skipped)
4. **Process changes**:
   - Set `scd_end_date` and `scd_active = 0` on old version
   - Insert new version with `scd_start_date = current_timestamp`, `scd_end_date = NULL`, `scd_active = 1`

**Columns Excluded from Hash Comparison:**
- Surrogate key column (e.g., `customer_key`)
- SCD system columns (`scd_start_date`, `scd_end_date`, `scd_active`)
- Delta metadata columns (`delta__created_datetime`, `delta__modified_datetime`)
- Watermark column (if configured)

**Handling Deletes in SCD2:**

If you configure `column_to_mark_source_data_deletion` and `delete_rows_with_value`, deleted records are closed (not physically removed):
```sql
-- When source sends is_deleted = 1, close the dimension record
(101, 'target_details', 'column_to_mark_source_data_deletion', 'is_deleted'),
(101, 'target_details', 'delete_rows_with_value', '1')
```

**Unknown Member Row:**

When creating SCD2 dimensions with surrogate keys, the accelerator automatically creates an "unknown member" row with surrogate key = `-1`. This ensures fact table referential integrity when dimension lookups fail.

**Querying SCD2 Dimensions:**
```sql
-- Current state (most common)
SELECT * FROM gold.dbo.dim_customer WHERE scd_active = 1

-- Point-in-time query (what did it look like on Jan 1, 2025?)
SELECT * FROM gold.dbo.dim_customer 
WHERE scd_start_date <= '2025-01-01' 
  AND (scd_end_date > '2025-01-01' OR scd_end_date IS NULL)

-- Full history for a specific customer
SELECT * FROM gold.dbo.dim_customer 
WHERE customer_id = 12345 
ORDER BY scd_start_date
```

### How do I build a fact table with surrogate keys?
**Keywords:** fact table, surrogate keys, foreign keys, dimension keys, star schema, fact, denormalization, key lookup

**Also answers:** How do I look up dimension keys? How do I create a star schema? How do I denormalize data? How do I insert surrogate keys into facts?

See [Runtime and Workflows Guide - Loading data into a fact table](DATA_MODELING_GUIDE.md#loading-data-into-a-fact-table) for surrogate key insertion and denormalization patterns.

### What data quality checks are available?
**Keywords:** data quality, DQ checks, validation rules, quality checks, data validation, pattern matching, business rules, data integrity, validate_pattern, validate_condition, referential integrity, primary key validation, validate_anomaly, anomaly detection, outliers

**Also answers:** How do I validate data quality? How do I check for duplicates? How do I validate email addresses? How do I enforce business rules? What happens to bad records? How do I detect outliers?

The solution automatically validates **primary keys for uniqueness** on every pipeline execution whenever primary keys are specified in your metadata. It also automatically checks for **corrupt records** (rows that don't conform to the expected schema). These are mandatory checks that run regardless of configuration.

**Automatic Checks (always enabled):**
- ✅ **Primary Key Validation** - Primary key uniqueness is validated for every Table_ID that has `Primary_Keys` configured
- ✅ **Corrupt Record Detection** - Records that don't conform to the schema are identified and quarantined
- ✅ **Schema Contract Validation** - When a `schema` is defined in `source_details`, the pipeline validates that the source DataFrame has the expected columns and data types before writing. Mismatches fail the pipeline explicitly (via `validate_table_schema_contract`)
- ✅ **Schema Change Detection** - After every write, the framework automatically detects added/removed/changed columns vs. the previously logged schema and records changes in `Schema_Logs` and `Schema_Changes`
- ✅ **Schema Drift Enforcement** - Set `fail_on_new_schema = 'true'` in `source_details` to fail the pipeline if the source schema has changed since the last run
- ✅ **Configurable action** - Control how violations are handled: `warn` (log warning), `quarantine` (isolate records), or `fail` (stop pipeline)
- ✅ **Includes composite keys** - Supports multi-column primary keys automatically
- ✅ **No configuration required** - Just provide `Primary_Keys` in your Orchestration metadata; schema validation activates when `schema` is defined

**Additional Optional Data Quality Checks** (configured via Advanced Configuration):

| Check Type | Purpose |
|------------|---------|
| `validate_condition` | Custom SQL filter expressions for business rules |
| `validate_pattern` | Regex pattern matching (email, phone, SSN, dates, URLs, etc.) |
| `validate_not_null` | Ensure specified columns have no NULL values |
| `validate_unique` | Ensure column values are unique (beyond primary keys) |
| `validate_referential_integrity` | Referential integrity checks against reference tables |
| `validate_batch_size` | Ensure minimum batch size thresholds |
| `validate_completeness` | Check required fields are populated above a percentage threshold |
| `validate_freshness` | Ensure data is recent enough |
| `validate_range` | Numeric/date range validation |
| `validate_anomaly` | Statistical anomaly detection using Z-score or IQR methods |

See [Runtime and Workflows Guide - Transform data with filters and data quality checks](DATA_TRANSFORMATION_GUIDE.md#transform-data-with-filters-and-data-quality-checks) for complete configuration details.

**Example Primary Key Validation in Action:**
```sql
-- This metadata ALWAYS validates that order_id is unique
INSERT INTO Data_Pipeline_Metadata_Orchestration (Trigger_Name, Order_Of_Operations, Table_ID, Target_Datastore, Target_Entity, Primary_Keys, Processing_Method, Ingestion_Active)
VALUES ('OrdersETL', 1, 101, 'silver', 'dbo.orders', 'order_id', 'batch', 1);

-- Configure how to handle duplicates
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES (101, 'target_details', 'if_duplicate_primary_keys', 'quarantine');
-- If duplicate order_ids found, they will be quarantined and logged
```

**What Happens When Duplicates Are Found:**
- **warn**: Records the duplicate occurrence in logs as a warning and continues processing
- **quarantine**: Moves duplicate records to the quarantine table for manual review and continues
- **fail**: Stops the pipeline with an error (ensures no duplicates reach production)

### How do I configure data quality check actions (warn, quarantine, fail)?
**Keywords:** if_not_compliant, warn, quarantine, mark, fail, data quality action, DQ action, violation handling, data quality behavior, quarantine records, message attribute

**Also answers:** What happens when data quality checks fail? How do I quarantine bad records? How do I make pipelines fail on bad data? What's the difference between warn and fail? What does mark do?

Every data quality check has an `if_not_compliant` attribute that controls what happens when records violate the rule.

> **🔴 REQUIRED: `message` attribute** — Every data quality check MUST include a `message` attribute containing a short, human-readable description (e.g., `'message', 'Amount must be positive'`). This message is used in quarantine reasons, marker column details, and warning/failure logs.

| Action | Behavior | Use When |
|--------|----------|----------|
| **`warn`** (default) | Log warning, continue processing, keep records | Monitoring data quality trends without blocking pipelines |
| **`mark`** | Add row-level detail to `delta__dq_failure_details`, keep records | Downstream observability and triage without removing rows |
| **`quarantine`** | Move violating records to `{table}_quarantined`, continue with clean data | Isolating bad records for review while allowing good data through |
| **`fail`** | Stop pipeline immediately with error | Critical validations where bad data must never reach production |

**Action by Check Type:**

| Check Type | `warn` | `mark` | `quarantine` | `fail` |
|------------|--------|--------|--------------|--------|
| `validate_condition` | ✅ | ✅ | ✅ | ✅ |
| `validate_pattern` | ✅ | ✅ | ✅ | ✅ |
| `validate_not_null` | ✅ | ✅ | ✅ | ✅ |
| `validate_unique` | ✅ | ✅ | ✅ | ✅ |
| `validate_range` | ✅ | ✅ | ✅ | ✅ |
| `validate_referential_integrity` | ✅ | ✅ | ✅ | ✅ |
| `validate_batch_size` | ✅ | ⚠️ treated as warn | ⚠️ treated as warn | ✅ |
| `validate_freshness` | ✅ | ⚠️ treated as warn | ⚠️ treated as warn | ✅ |
| `validate_completeness` | ✅ | ⚠️ treated as warn | ⚠️ treated as warn | ✅ |
| `validate_anomaly` | ✅ | ✅ | ✅ | ✅ |

> **Note:** `validate_batch_size`, `validate_freshness`, and `validate_completeness` are **batch-level metrics** (not row-level), so quarantine and mark don't apply—these default to `warn` behavior when `quarantine` or `mark` is specified.

**Configuration Example:**
```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
-- Critical: Fail if amounts are negative (should never happen)
(101, 'data_quality', 'validate_condition', 1, 'condition', 'amount >= 0'),
(101, 'data_quality', 'validate_condition', 1, 'if_not_compliant', 'fail'),
(101, 'data_quality', 'validate_condition', 1, 'message', 'Amount must not be negative'),

-- Warning: Log invalid emails but don't block
(101, 'data_quality', 'validate_pattern', 2, 'column_name', 'email'),
(101, 'data_quality', 'validate_pattern', 2, 'pattern', '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'),
(101, 'data_quality', 'validate_pattern', 2, 'if_not_compliant', 'warn'),
(101, 'data_quality', 'validate_pattern', 2, 'message', 'Email must be a valid format'),

-- Quarantine: Isolate records with invalid customer references
(101, 'data_quality', 'validate_referential_integrity', 3, 'current_table_column_name', 'customer_id'),
(101, 'data_quality', 'validate_referential_integrity', 3, 'reference_table_name', 'silver.dbo.dim_customer'),
(101, 'data_quality', 'validate_referential_integrity', 3, 'reference_table_column_name', 'customer_id'),
(101, 'data_quality', 'validate_referential_integrity', 3, 'if_not_compliant', 'quarantine'),
(101, 'data_quality', 'validate_referential_integrity', 3, 'message', 'Customer ID must exist in dim_customer')
```

**Viewing Results:**
- **Warnings**: Query `Data_Quality_Notifications` in Metadata Warehouse
- **Quarantined records**: Query `{target_table}_quarantined` table (includes `delta__quarantine_reason` column)
- **Row-level DQ details**: `delta__dq_failure_details` is populated only for checks configured with `if_not_compliant = 'mark'`
- **Failures**: Pipeline stops with error message in the workspace run history

> **⚠️ Important: NULL handling in `validate_condition` and `validate_range`**
>
> Both `validate_condition` and `validate_range` use standard SQL three-valued logic. This means **NULL values silently pass these checks**:
>
> | Check | Condition | Value | Result |
> |-------|-----------|-------|--------|
> | `validate_condition` | `amount > 0` | `NULL` | ✅ Row passes (not flagged) |
> | `validate_condition` | `amount > 0` | `-5` | ❌ Row fails (flagged) |
> | `validate_range` | `min_value = 0` | `NULL` | ✅ Row passes (not flagged) |
> | `validate_range` | `min_value = 0` | `-1` | ❌ Row fails (flagged) |
>
> **Why?** SQL evaluates `NOT (NULL > 0)` as `NOT (NULL)` → `NULL` → row is excluded from the violation set. Similarly, `NULL < 0` evaluates to `NULL`, so the row is not considered out-of-range.
>
> **If you need to catch NULLs**, pair these checks with `validate_not_null`:
> ```sql
> -- Step 1: Catch NULLs explicitly
> (101, 'data_quality', 'validate_not_null', 1, 'column_name', 'amount'),
> (101, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'quarantine'),
> (101, 'data_quality', 'validate_not_null', 1, 'message', 'Amount must not be null'),
> -- Step 2: Catch out-of-range values (NULLs already handled above)
> (101, 'data_quality', 'validate_range', 2, 'column_name', 'amount'),
> (101, 'data_quality', 'validate_range', 2, 'min_value', '0'),
> (101, 'data_quality', 'validate_range', 2, 'if_not_compliant', 'quarantine'),
> (101, 'data_quality', 'validate_range', 2, 'message', 'Amount must be non-negative'),
> ```
>
> Alternatively, for `validate_condition`, write a NULL-aware condition:
> ```sql
> (101, 'data_quality', 'validate_condition', 1, 'condition', 'amount IS NOT NULL AND amount > 0'),
> ```

### How do I reload data after an error?
**Keywords:** reload, retry, reprocess, error recovery, failed loads, rerun, fix errors, data recovery, restart

See [Runtime and Workflows Guide - Reload Data](Restarting_Pipelines_From_Failure.md) for different reload scenarios based on data source type.

### How are data quality checks executed?
**Keywords:** DQ execution, parallel checks, single loop, deterministic quarantine, quarantine order, DQ performance, ThreadPoolExecutor, check execution order, caching, MEMORY_AND_DISK, mark mode

**Also answers:** Do DQ checks run in parallel? How are quarantine checks kept deterministic? How does the accelerator speed up data quality? What happens when I have many checks? Is there a limit on parallel checks?

The accelerator evaluates all configured data quality checks in one parallel loop over the same cached DataFrame snapshot.

**How it works:**
1. Primary key validation and corrupt record detection run first.
2. All configured checks run concurrently (up to 8 threads) on the same immutable snapshot.
3. Quarantine candidates are collected during that loop, then applied once in deterministic metadata order.
4. If multiple quarantine rules match the same row, metadata order determines precedence and ALL matching reasons are concatenated with `"; "` (in that order) into a single `delta__quarantine_reason` value. The quarantine table does **not** include `delta__dq_failure_details`.
5. Row-level marker details (`delta__dq_failure_details`) are created only for checks configured with `if_not_compliant = 'mark'`. When multiple mark checks fail for the same row, their messages are concatenated with `"; "`.

**Implementation note (edge-case handling):**
- Deterministic quarantine application expects candidate rows to include `delta__idx`, `__dq_step_order`, and `delta__quarantine_reason`.
- If no check produced quarantine rows (for example, all checks were `warn`/`mark`, or no rows violated quarantine rules), candidate frames can be schema-empty for quarantine metadata and the deterministic quarantine phase is skipped safely.

**Performance impact:**
- With 30 checks (25 warn + 5 quarantine), the 25 warn checks run simultaneously instead of one-by-one
- The DataFrame is cached once and shared across all parallel checks, avoiding redundant Spark DAG re-evaluation
- Maximum 8 concurrent threads (to avoid overwhelming the Spark cluster)

> **No configuration needed** — this is automatic. Just set `if_not_compliant` on each check and the engine picks the optimal execution strategy.

### Can I write to multiple target tables from one source?
**Keywords:** multiple targets, one-to-many, split data, fan-out, multiple outputs, duplicate to multiple tables

No, one Table_ID = one target table. To write to multiple targets from the same source:
1. Create multiple Table_IDs with the same source configuration
2. Use same `Order_Of_Operations` to process in parallel
3. Apply different transformations or filters to each Table_ID as needed

### How do I handle late-arriving data with watermarks?
**Keywords:** late arriving data, late data, watermarks, backfill, historical data, out-of-order data, delayed data

Watermarks track the maximum value processed, so late-arriving data with older timestamps won't be picked up automatically. Options:
1. **Full reload**: Use the [Trigger Full Reloads](DevOps.md#trigger-full-reloads) process
2. **Manual watermark reset**: Update the watermark value in the logging table to an earlier date
3. **Design for late arrival** (Delta tables only): Use multiple watermark columns (e.g., `ModifiedDate, CreatedDate`) with comma-separated configuration. **Note:** Multiple watermark columns are only supported when reading from Delta tables (Bronze/Silver/Gold layers), not when ingesting from external databases (Oracle, SQL Server, etc.).

### Can I schedule pipelines to run at specific times?
**Keywords:** schedule, scheduling, automation, cron, recurring, timer, automatic runs, scheduled execution

Yes, use the platform's built-in scheduling:
1. Open your orchestration pipeline (`Trigger Step Orchestrator_For_Each_Trigger_Step`)
2. Click "Schedule" in the toolbar
3. Configure recurrence pattern (hourly, daily, weekly, etc.)
4. Set start time and time zone
5. Pipeline will automatically execute on schedule with your configured trigger name

### I have multiple triggers that need to run in a specific order - what's the best way to do that?
**Keywords:** wrapper pipeline, multiple triggers, orchestration, parallel execution, dependencies, trigger orchestration, parent pipeline, sequence, order

**Also answers:** How do I run triggers in sequence? How do I run data products in parallel? How do I ensure shared tables load before data products? Can I schedule multiple triggers?

Create a wrapper pipeline that calls `Trigger Step Orchestrator_For_Each_Trigger_Step` multiple times with different `Trigger_Name` parameters:

```
PL_Daily_Orchestration (wrapper)
│
├─► Call Trigger Step Orchestrator (Trigger_Name = 'SharedSources_Bronze')
│         │
│         ▼ (on success)
│   ┌─────┴─────┐
│   │           │
│   ▼           ▼
├─► Call Trigger Step Orchestrator    Call Trigger Step Orchestrator
│   (Sales)        (Finance)
│   [parallel]     [parallel]
```

**Benefits:**
- Shared data assets ingest first, then data products run in parallel
- Single schedule on the wrapper pipeline
- Clear dependency chain
- Easy to monitor end-to-end execution

**Note on direct scheduling:** The platform supports scheduling per job, but custom parameter values per schedule are not yet available (expected Q1 2026). Once available, you'll be able to schedule `Trigger Step Orchestrator` directly with different `Trigger_Name` values, though you'd lose the dependency orchestration the wrapper provides.

> **Intra-trigger ordering:** Within a *single* trigger, use `Order_Of_Operations` in the Orchestration table to control the sequence. Table_IDs with the same `Order_Of_Operations` value run in parallel; different values run sequentially (1 first, then 2, then 3, etc.). This is how you handle dependencies within a trigger — for example, ensure shared Bronze tables load before their Silver dependents.

### Two of my triggers need the same source table - is that going to cause problems?
**Keywords:** shared tables, common tables, cross-trigger, multiple data products, shared sources, dependency management, execution order, duplicate ingestion

**Also answers:** How do I handle shared tables? How do I ensure shared tables load first? What if two triggers need the same source table? How do I avoid ingesting the same table twice?

It depends on how you've set it up. You have two options:

**Option 1: Wrapper pipeline (recommended)** - See [I have multiple triggers that need to run in a specific order](#i-have-multiple-triggers-that-need-to-run-in-a-specific-order---whats-the-best-way-to-do-that). Create a dedicated trigger for shared sources that runs first, then your data product triggers read from the already-ingested Bronze tables.

**Option 2: Same Table_ID in both triggers** - If both triggers reference the exact same `Table_ID`, the accelerator's built-in FIFO queue prevents concurrent execution. The second trigger waits for the first to complete. This works but isn't ideal since you lose control over which trigger "wins."

**Bottom line:** If multiple data products share source tables, pull those into a dedicated trigger and use a wrapper pipeline to guarantee order.

> 📖 For a complete guide on trigger design patterns (per-table, data-product, wrapper) and external scheduler integration, see [Trigger Design and Dependency Management](Trigger_Design_and_Dependency_Management.md).

### How should I design triggers to avoid unnecessary waiting between silver and gold?
**Keywords:** trigger design, trigger granularity, silver gold dependency, unnecessary waiting, parallel triggers, trigger patterns, broad trigger, granular trigger, gold waiting for silver

**Also answers:** Why is my gold table waiting for unrelated silver tables? How do I make gold run as soon as its silver table finishes? What trigger patterns are available? How granular should my triggers be?

If a single trigger contains many silver and gold tables, gold processing must wait for *all* silver tables to complete — even unrelated ones. For example, if Silver_A finishes in 2 minutes but Silver_T takes 45 minutes, Gold_A waits 43 minutes for no reason.

**Solution: Use granular triggers that match actual dependency chains.** Three patterns are available:

| Pattern | Description | Works Well When |
|---------|-------------|-----------------|
| **Per-table triggers** | Each table gets its own trigger (Silver→Gold chain) | Each table has independent scheduling needs |
| **Data product triggers** | Group tables by business domain | A gold table genuinely needs multiple silver tables |
| **Wrapper pipeline** | Shared sources load first, then data products run in parallel | Multiple data products share common source tables |

These patterns can be mixed and matched freely. See [Trigger Design and Dependency Management](Trigger_Design_and_Dependency_Management.md) for detailed diagrams, side-by-side comparisons, external scheduler integration guidance, and migration steps.

### We're thinking about using a Warehouse for Gold instead of Catalog - anything we should know?
**Keywords:** warehouse, gold warehouse, catalog vs warehouse, warehouse limitations, spark connector, warehouse target, T-SQL

**Also answers:** Should I use Warehouse or Catalog for Gold? What features don't work with Warehouse? What's the maintenance overhead? What breaks if I use Warehouse?

Databricks SQL Warehouse's Spark Connector supports `append` and `overwrite` only. The accelerator's built-in capabilities require custom implementation when targeting Warehouse:

| Capability | In Catalog | In Warehouse |
|------------|--------------|--------------|
| Merge/Upsert | Automatic | Requires custom stored procedure per table |
| SCD Type 2 | Automatic | Requires custom stored procedure per table |
| Schema Evolution | Automatic | Requires manual ALTER TABLE statements |
| Data Quality Quarantine | Automatic | Requires custom logic |
| Soft Deletes | Automatic (`merge_mark_*`) | Requires custom stored procedure |

**Maintenance implications:**

For tables requiring merge (most dimension and fact tables with updates), each table needs:
- A staging table for Spark to write to
- A stored procedure to MERGE from staging to target
- Ongoing maintenance when business logic changes

Example: 50 Gold tables with merge requirements = 50 stored procedures to write, test, version control, and maintain.

**Performance considerations:**

The accelerator applies Microsoft-proprietary Spark optimizations to Delta tables (V-Order, auto-compaction, optimized writes). Query performance should be comparable to Warehouse for most analytical workloads.

**When Warehouse works well:**
- Append-only fact tables (no updates ever)
- Full-refresh tables (daily overwrite)
- Teams with strong existing T-SQL investment

### Can I write to multiple Warehouses? Different teams want their own.
**Keywords:** multiple warehouses, warehouse targets, multi-warehouse, different warehouses, warehouse configuration, separate warehouses

**Also answers:** Can I write to more than one warehouse? How do I set up additional warehouse targets? How do I configure multiple warehouse destinations?

Yes - adding new target datastores or warehouses is simple. Just add a record to the `Datastore_Configuration` table in your environment's datastore notebook. See [How do I add a new catalog or warehouse as a target datastore?](#how-do-i-add-a-new-catalog-or-warehouse-as-a-target-datastore) for the setup steps.

For warehouses specifically, also review [the warehouse implications](#were-thinking-about-using-a-warehouse-for-gold-instead-of-catalog---anything-we-should-know) - you'll need stored procedures for any tables that require merge/upsert.

### How do I optimize pipeline performance for ingesting large tables from external sources?
**Keywords:** performance, optimization, tuning, speed up, faster, slow pipelines, performance tuning, partitioning, parallelization

See [Data Ingestion Guide - Advanced Topics](DATA_INGESTION_GUIDE.md#advanced-topics) for detailed guidance:
1. **Enable source partitioning**: Implement partitioned copy activities in the pipeline (see question below)
2. **Use watermarking**: Incremental loading reduces data volume significantly by only extracting changed records
3. **Apply liquid clustering**: Cluster on frequently filtered/joined columns for faster query performance
4. **Split large tables using multiple Table_IDs**: Create multiple metadata records with different filter conditions in source queries to parallelize ingestion
5. **Parallel execution**: Set the same `Order_Of_Operations` for independent Table_IDs to run them simultaneously

### How do I enable source partitioning for external database ingestion?
**Keywords:** source partitioning, parallel copy, DynamicRange, partitioning_option, large table extraction, Oracle partition, SQL Server partition, parallel extraction, database partitioning

**Overview:** The accelerator's pipeline infrastructure fully supports source-side partitioning through the `partitioning_option` metadata configuration. To enable it for a specific database connector, you need to add a new Copy activity to the switch statement in [External Data Staging_Copy](../src/External Data Staging_Copy.DataPipeline/pipeline-content.json).

**How it works:**

1. The pipeline switch evaluates: `source_details_source` + `_` + `partitioning_option`
2. For example, if you configure `source = 'oracle'` and `partitioning_option = 'DynamicRange'`, the switch looks for case `oracle_DynamicRange`
3. Once the partitioned copy activity exists, the rest of the pipeline (watermarking, file conversion, logging) continues to work automatically

**Implementation Steps:**

1. **Open** [staging task_Ingest_External_Data_Copy.DataPipeline/pipeline-content.json](../src/staging task_Ingest_External_Data_Copy.DataPipeline/pipeline-content.json)

2. **Add a new switch case** (e.g., `oracle_DynamicRange`) by copying the existing `oracle` case and modifying the Copy activity's source configuration:

```json
{
  "value": "oracle_DynamicRange",
  "activities": [
    {
      "name": "Oracle Copy DynamicRange",
      "type": "Copy",
      "typeProperties": {
        "source": {
          "type": "OracleSource",
          "oracleReaderQuery": {
            "value": "@replace(pipeline().parameters.primary_config.source_details_query, '{WATERMARKVALUE}', pipeline().parameters.watermark_value)",
            "type": "Expression"
          },
          "partitionOption": "DynamicRange",
          "partitionSettings": {
            "partitionColumnName": {
              "value": "@pipeline().parameters.primary_config.source_details_partition_column_name",
              "type": "Expression"
            },
            "partitionUpperBound": {
              "value": "@pipeline().parameters.primary_config.source_details_partition_upper_bound",
              "type": "Expression"
            },
            "partitionLowerBound": {
              "value": "@pipeline().parameters.primary_config.source_details_partition_lower_bound",
              "type": "Expression"
            }
          },
          "queryTimeout": "02:00:00",
          "datasetSettings": { ... }
        },
        "sink": { ... }
      }
    },
    { "name": "Oracle rows copied", ... },
    { "name": "Oracle Source Details", ... }
  ]
}
```

3. **Configure metadata** to use partitioning:

```sql
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (1, 'source_details', 'partitioning_option', 'DynamicRange'),
    (1, 'source_details', 'partition_column_name', 'ORDER_ID'),
    (1, 'source_details', 'partition_upper_bound', '10000000'),
    (1, 'source_details', 'partition_lower_bound', '1');
```

**Supported partition options by connector:**

| Connector | Partition Options | Documentation |
|-----------|------------------|---------------|
| Oracle | `DynamicRange`, `PhysicalPartitionsOfTable` | Oracle parallel copy docs |
| Azure SQL | `DynamicRange`, `PhysicalPartitionsOfTable` | Azure SQL parallel copy docs |
| SQL Server | `DynamicRange`, `PhysicalPartitionsOfTable` | SQL Server parallel copy docs |
| PostgreSQL | `DynamicRange` | [PostgreSQL parallel copy](https://learn.microsoft.com/en-us/azure/data-factory/connector-postgresql#parallel-copy-from-postgresql) |

### How do I add a new ingestion source type?
**Keywords:** new source, add source, custom source, new connector, extend sources, add connector, custom ingestion, REST API source, external source, salesforce, dynamics365, new database type

**Also answers:** How do I connect to Salesforce? How do I add Dynamics 365? How do I create a custom connector? How do I extend the pipeline for new sources? How do I ingest from REST APIs?

The accelerator supports adding new source types through three approaches. See [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) for selection rules and [Extending the Accelerator](Extending_the_Accelerator.md) for implementation details.

**Option A: Pipeline Copy Activity** (recommended for external databases; good for simple REST APIs) — Add a new case to the Switch activity in `External Data Staging_Copy`. Existing types: `oracle`, `azure_sql`, `sql_server`, `postgre_sql`, `my_sql`, `db2`, `rest_api`, `sftp`. Credentials are Managed via Databricks secrets (no secrets in code), and extraction doesn't consume Spark compute. Trade-off: less flexibility with complex API patterns (custom pagination, OAuth flows, multi-step calls). Also update `External Data Staging_Watermark` for incremental loads and register a DDL generator in `helper_functions_2.py` if using DDL extraction.

**Option B: Custom Staging Function** (preferred for complex REST APIs) — Use `custom_staging_function` to land raw API responses as-is in UC Volumes, then parse into Bronze (medallion best practice). Full Python control over auth, pagination, and response parsing. Trade-off: credential management typically requires a managed private endpoint to Key Vault, which can slow Spark startup and development speed. Configure via `source_details` / `custom_staging_function_notebook` + `custom_staging_function`. See [Data Ingestion Guide - Custom Staging Function](DATA_INGESTION_GUIDE.md#using-a-custom-staging-function).

**Option C: Custom Source Function** — Use `custom_source_function` when the function should return a DataFrame directly and let the framework handle DQ, transforms, merge, and logging. Same credential trade-offs as Option B. See [Data Ingestion Guide - Custom Source Function](DATA_INGESTION_GUIDE.md#using-a-custom-source-function).

### Should Bronze layer use raw files or Delta tables?
**Keywords:** bronze format, raw files, delta tables, CSV, JSON, Parquet, bronze layer, storage format, original files, medallion architecture, bronze storage, data duplication, staging, archive, time travel, ACID, schema evolution, governance

**Also answers:** Should I store Bronze as CSV or Delta? Is it okay to keep Bronze as original file format? How do I reduce data duplication across layers? What's the difference between raw files and Delta for Bronze? When should Bronze be raw files? How do I avoid storing data 3 times?

For most operational data platforms, **Delta tables are the recommended default** for Bronze due to ACID guarantees, query performance, schema evolution, time travel, and native platform integration (Direct Lake, SQL endpoint).

**When to use each:**
- **Delta (default):** Database extracts, structured files, semi-structured JSON, streaming sources
- **Raw files:** Binary files (PDFs, images), strict legal compliance requiring original bytes
- **Hybrid (Delta + Cold Archive):** When compliance requires original bytes AND operational queries — archive originals to ADLS Gen2 Cold tier ($0.004/GB vs UC volumes $0.023/GB) via shortcut

**Data duplication concern:** The solution is using lower-cost storage tiers for staging/archive, not keeping Bronze as raw files. At 1 TB, cold archive adds ~$48/year vs ~$276/year for an extra UC volumes copy.

**What you lose with raw file Bronze:** Automated schema change detection (`fail_on_new_schema`), EDA profiling, lineage tracking through Bronze, `DESCRIBE HISTORY` / time travel, GDPR delete support, and 10-100× faster Silver reloads.

**Key patterns:** Append-only Bronze (`merge_type = 'append'`) gives immutability with Delta benefits. Store columns as strings in Bronze to preserve source fidelity (cast types in Silver). Configure VACUUM retention per compliance needs.

See [Medallion Architecture Best Practices](Medallion_Architecture_Best_Practices.md) for architectural guidance.

### How does the accelerator work with Databricks high-concurrency mode?
**Keywords:** high concurrency, high-concurrency mode, shared Spark session, HC mode, concurrent notebooks, spark.conf.set, session-level config, TBLPROPERTIES, table properties, multi-notebook, parallel notebooks, session tag, pipeline notebook activity

**Also answers:** Can I enable high-concurrency mode? What breaks with high-concurrency? Are there any session-level Spark configs that conflict? Is the accelerator safe for high-concurrency mode? How do I use session tags?

High-concurrency mode shares a single SparkSession across concurrent pipeline-triggered notebooks, reducing startup time and cost.

**Already safe:** All per-table Delta properties (`delta.enableChangeDataFeed`, `delta.autoOptimize.*`, `delta.parquet.vorder.enabled`, etc.) are set as TBLPROPERTIES — table-scoped, not session-scoped.

**Universal session configs (no conflict risk):** `spark.databricks.delta.schema.autoMerge.enabled`, `spark.microsoft.delta.optimize.fast.enabled`, `spark.sql.ansi.enabled`, and others are always set to the same value by every notebook.

**Only remaining risk:** Timestamp rebase modes (`spark.sql.parquet.datetimeRebaseModeInWrite/Read`, `int96RebaseModeInWrite/Read`) — default `CORRECTED`, only changed to `LEGACY` for pre-Gregorian timestamps. If two concurrent notebooks use different modes, the last `spark.conf.set()` wins. **Mitigation:** Use session tags on the pipeline Notebook activity to isolate notebooks with `LEGACY` rebase, or assign them to a separate Databricks compute cluster.

**Temp view isolation:** All temp views use unique suffixes (`_<Table_ID>` or `_<UUID>`) to prevent collisions. Enable HC mode via Workspace settings → Data Engineering/Science → Spark settings → High concurrency.

### How do I restart a pipeline after a mid-run failure?
**Keywords:** restart, rerun, re-run, partial failure, resume, start_with_step, table_ids, skip steps, failed tables

**Also answers:** How do I re-run only the failed tables? Can I skip steps that already succeeded? How do I resume from a specific step?

Use the built-in `start_with_step` and `table_ids` parameters on `Trigger Step Orchestrator_For_Each_Trigger_Step`. These are runtime parameters that skip completed work without touching any metadata. Do **not** toggle `Ingestion_Active` — that creates CI/CD drift and risks silently disabling tables.

For the full guide including decision trees, worked examples, and FIFO lock cleanup, see [Restarting Pipelines From Failure](Restarting_Pipelines_From_Failure.md).

### Where can I see why my pipeline failed?
**Keywords:** pipeline failure, errors, troubleshooting, debugging, failed pipeline, error messages, failure logs, why failed

**Also answers:** Where are the error logs? How do I find what went wrong? Where do I see pipeline errors? How do I debug a failure?

Check these locations in order:
1. **Pipeline Run History**: Open the pipeline and view run details
2. **Metadata Warehouse Logs**: Query `Data_Pipeline_Logs` table for error messages
3. **Notebook Output**: If a notebook failed, check the notebook run output in the workspace
4. **Pre-built Report**: Use the Data Movement monitoring report to view all pipeline executions and errors

### Troubleshooting pipeline and notebook failures
**Keywords:** troubleshooting, pipeline failure, notebook failure, nested pipelines, drill down, ForEach, For Each Entity, Operation on target For Each Entity failed, If Data Staging Required, notebook snapshot, parameters, interactive debugging, inner activity failed, debug, error, exception

**Also answers:** How do I debug a failed pipeline? How do I find the root cause of an error? Why did my notebook fail? How do I reproduce a notebook failure? Where do I find the actual error message?

If you see a generic generic message like "Activity failed because an inner activity failed" (especially inside a `ForEach`), the top-level error text is usually not the root cause. The root cause is almost always in a deeper, nested activity.

Common examples of this pattern include messages like:
- "Operation on target For Each Entity failed"
- "Inner activity name: If Data Staging Required"

#### Pipeline failures: drill into the deepest failing activity

1. Open the failed pipeline run in the workspace.
2. In the activity run list (the table/grid), locate the failed activity row.
3. Click the **arrow icon under the Output column** on that same row to open the Output pane.
4. If the failed activity is **an Execute Pipeline / nested pipeline call**:
   - In Output, find `pipelineRunId`, then click the **value** next to it to open the child pipeline run.
   - Use `pipelineName` in that same Output pane to confirm you jumped to the expected nested pipeline.
5. If the failed activity is a `ForEach`, open the failed iteration (or expand the `ForEach`), then repeat steps 2–4 on the failed inner activity row.
6. Keep drilling down until you reach the deepest activity that contains the descriptive error text, then copy that activity name and full error message.

#### Notebook failures: use the notebook snapshot

1. From the failed pipeline activity, open the notebook run details.
2. Open the notebook snapshot for that run and capture:
   - The first failing cell’s error output
   - The surrounding diagnostic prints
3. Fastest path to reproduction: copy the generated parameter cell from the snapshot into your batch notebook and run cell-by-cell; remove that temporary cell afterward to avoid hard-coded parameters impacting future runs.

#### Video walkthrough

See [Video Training](Video_Training.md) for the end-to-end click-through of nested pipelines and notebook snapshots.

When asking an LLM for help, include (at minimum):
- The deepest pipeline activity name that failed and its full error text (not the top-level failure summary)
- The pipeline run URL (or Monitor link) and which nested pipeline level you were in
- If a notebook failed: the notebook snapshot link (or screenshot/text of the failing cell output) and the generated parameter cell
- The affected Table_ID, Trigger_Name, and target table (if available)

### What does "quarantine" mean and how do I review quarantined records?
**Keywords:** quarantine, quarantined records, bad records, rejected data, failed validation, DQ failures, review quarantine

**Also answers:** Where do bad records go? How do I find rejected data? What happens to failed validations? How do I see records that failed DQ checks?

Quarantined records are rows that failed data quality checks but didn't stop the pipeline. They're stored in a separate table for review:
- **Quarantine table naming**: `{target_table_name}_quarantined`
- **Example**: If target is `silver.dbo.customers`, quarantine table is `silver.dbo.customers_quarantined`
- **Review**: Query the quarantine table directly: `SELECT * FROM silver.dbo.customers_quarantined`
- **Columns**: Includes all original columns plus `delta__quarantine_reason` explaining why the record was quarantined

### How do I fix schema mismatch errors?
**Keywords:** schema mismatch, schema errors, schema evolution, column mismatch, schema drift, schema changes, data type errors

**Also answers:** Why is my pipeline failing on schema? How do I handle new columns? How do I enable schema evolution? What do I do when source schema changes?

When source schema changes:
1. **Review**: Check `Schema_Logs` table in Metadata Warehouse for detected schema changes, or review schema evolution in the Exploratory Data Analysis report
2. **Option 1 - Allow changes**: Set `fail_on_new_schema = 'false'` in `target_details` to automatically evolve schema
3. **Option 2 - Manual fix**: Update source query or file schema definition to match expected schema
4. **Option 3 - Reload**: Trigger a full reload to rebuild the target table with new schema

See [Runtime and Workflows Guide - Schema Change Detection](MONITORING_AND_LOGGING.md#schema-logs) for details.

### Why are my values becoming NULL during file ingestion?
**Keywords:** NULL values, silent data loss, type casting, schema enforcement, CSV NULL, JSON NULL, data loss, unexpected NULL, values missing, data disappeared

**Also answers:** Why is my data disappearing? Why are CSV values NULL? Why is type casting failing silently? How do I prevent data loss during file ingestion?

This is a known behavior when using schema enforcement with CSV or JSON files:

**What's Happening:**
When you define a `schema` for CSV/JSON file ingestion and Spark cannot cast a value to the expected type (e.g., `'ABC'` to `INT`), the value silently becomes `NULL` without raising an error. This is different from Delta table schema validation which fails explicitly.

**File Format Differences:**
- **CSV/JSON**: Schema is applied DURING read - type casting failures produce NULL (silent data loss)
- **Parquet**: Schema is validated AFTER read - mismatches raise explicit exceptions (no silent data loss)

**Solutions:**
1. **Recommended - Ingest without schema**: Let CSV default to all strings, then cast in Bronze→Silver:
   ```sql
   -- Bronze ingestion: No schema (all strings)
   (45, 'source_details', 'wildcard_folder_path', 'data/*.csv')
   -- No schema specified - CSV defaults to all strings

   -- Silver transformation: Cast to desired types using change_data_types
   (46, 'data_transformation_steps', 'change_data_types', 1, 'column_name', 'amount,quantity'),
   (46, 'data_transformation_steps', 'change_data_types', 1, 'new_type', 'decimal(10,2),int')
   ```

2. **Use Parquet source files**: If possible, convert source data to Parquet format which provides explicit schema validation

3. **Accept the risk**: If you're confident source data always matches the schema, continue using schema enforcement but monitor for unexpected NULLs

See [Runtime and Workflows Guide - Schema Enforcement Behavior](DATA_INGESTION_GUIDE.md#schema-enforcement-behavior-by-file-format) for more details.

### My custom file ingestion function can't read files (FileNotFoundError or path errors)
**Keywords:** custom file ingestion, FileNotFoundError, path not found, ABFSS path, local path, mount, mounting, dbutils, custom_file_ingestion_function, file access, cannot read file, no such file, invalid path

**Also answers:** Why can't my custom function read files? How do I access files in custom ingestion? FileNotFoundError in custom notebook? How do I convert ABFSS to local path? Why does open() fail with ABFSS path?

When writing a `custom_file_ingestion_function`, the `file_paths` parameter contains **ABFSS paths** like:
```
/Volumes/{catalog}/{schema}/files/folder/file.csv
```

**The Problem:** Some Python libraries can use ABFSS paths directly, but others require local file system paths.

**What Works with ABFSS Paths (NO mounting needed):**
| Library | Example | Works? |
|---------|---------|--------|
| Spark | `spark.read.csv(abfss_path)` | ✅ Yes |
| Pandas | `pd.read_csv(abfss_path)` | ✅ Yes |
| dbutils | `dbutils.fs.head(abfss_path)` | ✅ Yes |

**What Requires Mounting (local path needed):**
| Library | Example | Works with ABFSS? |
|---------|---------|-------------------|
| Python `open()` | `open(path, 'r')` | ❌ No - mount required |
| `zipfile` | `zipfile.ZipFile(path)` | ❌ No - mount required |
| `PIL/Pillow` | `Image.open(path)` | ❌ No - mount required |
| `PyPDF2` | `PdfReader(path)` | ❌ No - mount required |
| `openpyxl` | Complex Excel parsing | ❌ No - mount required |
| `xml.etree` | Standard library XML | ❌ No - mount required |

**Solution: Use Helper Functions from helper_functions_3.py**

The accelerator provides helper functions to handle mounting automatically:

```python
def custom_file_ingestion_function(file_paths, all_metadata, spark):
    orchestration_metadata = all_metadata.get('orchestration_metadata')
    table_id = orchestration_metadata.get('Table_ID')
    
    # 1. Mount catalog using helper function
    mount_point, local_mount_path, catalog_root = _mount_catalog_for_local_access(file_paths, table_id)
    
    # 2. Convert ABFSS paths to local paths
    local_paths = _convert_abfss_paths_to_local(file_paths, local_mount_path, catalog_root)
    
    # 3. Now use standard Python file operations
    for local_path in local_paths:
        with open(local_path, 'r') as f:  # This now works!
            content = f.read()
    
    # 4. Cleanup when done
    _unmount_catalog(mount_point)
    
    return result_df
```

**Available Helper Functions:**
| Function | Purpose |
|----------|---------|
| `_mount_catalog_for_local_access(file_paths, table_id)` | Mount catalog, returns `(mount_point, local_mount_path, catalog_root)` |
| `_mount_abfss_path_for_local_access(abfss_path, table_id)` | Mount and resolve a single ABFSS path for write-oriented scenarios such as custom staging |
| `_convert_abfss_paths_to_local(file_paths, local_mount_path, catalog_root)` | Convert ABFSS paths to local paths |
| `_unmount_catalog(mount_point)` | Cleanup - unmount when done |

**Reference:** See [Custom File Ingestion Function Template](resources/Custom_File_Ingestion_Function.py) for a complete example.

**Documentation:** Databricks volume mount documentation

### What does "FIFO_Status" mean in the logs?
**Keywords:** FIFO, concurrency guard, status, processing status, concurrent run, execution status, first in first out

FIFO (First In, First Out) is a **concurrency guard** that prevents overlapping pipeline runs for the same table. When staging task starts processing a Table_ID, it calls the `FIFO_Status` stored procedure to check if another run is already in progress.

**How it works:**
- **Safe to proceed**: Returns `'Yes'` — no conflicting run exists
- **Conflict detected**: Raises an error (severity 16) with a diagnostic message showing the blocking run details
- **Time window**: Only considers runs started within the last **24 hours**

**Parameters:** `@table_id INT, @target_datastore VARCHAR(255)`

```sql
-- The pipeline calls this automatically — you don't need to run it manually
EXEC dbo.FIFO_Status @table_id = 101, @target_datastore = 'bronze'
```

> If you see FIFO errors in your logs, it means another pipeline run for the same Table_ID was still in progress or had failed within the last 24 hours. Wait for the other run to complete, or check `Data_Pipeline_Logs` for stuck/failed runs.

### How do I know if my data quality checks are working?
**Keywords:** verify DQ, test data quality, check validation, DQ testing, validation testing, quality check status

1. **Check logs**: Query `Data_Quality_Notifications` table for data quality notifications
2. **Review quarantine tables**: Check if records are being quarantined as expected
3. **Monitor report**: Use the Data Movement pre-built report to see DQ check results
4. **Test with bad data**: Intentionally introduce non-compliant data to verify checks trigger


### I'm getting "SQL pool is warming up" errors. What should I do?
**Keywords:** SQL pool warming up, warehouse warming, cold start, SQL endpoint sleep, pool warming, warehouse startup

Sometimes the serverless SQL analytics endpoint can be in sleep mode. This can lead to errors.

If this error occurs, wait a few minutes and retry. The endpoint will wake up automatically. You can also just restart the pipeline after a couple of minutes.

### I'm getting "Universal security feature is disabled for the workspace" errors. What should I do?
**Keywords:** universal security, data access control, security feature disabled, data access control, catalog security, data access security

To fix this error, the data access control feature must be enabled on each catalog using the dedicated button.

### Where can I find logs and monitoring?
**Keywords:** logs location, monitoring, where are logs, find logs, logging tables, log tables, monitoring tables

See [Runtime and Workflows Guide - Monitoring and Logging](MONITORING_AND_LOGGING.md#application-logging-framework) for:
- Application logging framework
- Data pipeline logs
- Data quality notifications
- Pre-built monitoring reports

### Where are the logging tables located?
**Keywords:** logging tables location, metadata warehouse, log tables, where are log tables, logging database, log storage

**Also answers:** Where is Data_Pipeline_Logs? Where is Activity_Run_Logs? Where is Data_Quality_Notifications? How do I query the logs? What database has the execution history?

All logging tables are stored in the **Metadata Warehouse** deployed with the solution. The actual database name varies by environment/workspace and must be resolved dynamically from workspace configuration for the active workflow. Copilot resolves it dynamically rather than assuming a fixed database name. You can access them by:

1. **Navigate to your Databricks workspace** where the solution is deployed
2. **Open the resolved metadata warehouse** for that environment/workspace
3. **Query the logging tables** using SQL queries in the warehouse SQL endpoint

**Key Logging Tables:**

1. **`Data_Pipeline_Logs`** - Pipeline execution logs
   - Tracks every data movement from start to finish
   - Contains Table_ID, source details, target details, start/end times
   - Shows ingestion status (Started/Processed/Failed)
   - Records watermark values and row counts processed
   - Includes Spark Monitor URLs for pipeline run details
   - Key columns: `Log_ID`, `Ingestion_Status`

2. **`Activity_Run_Logs`** - Structured activity execution logs
   - Stores sequenced messages for notebook-based and pipeline-based runs
   - Includes `Table_ID`, timestamp, level, step name, step number, message text, and `Source_Type` (`Notebook`, `Pipeline`). Step number maps to the notebook cell number for notebook logs and is always `1` for pipeline-originated entries
   - Uses the same `Log_ID` as `Data_Pipeline_Logs` for correlation; `Table_ID` enables independent lookup when `Data_Pipeline_Logs` has no matching row
   - Most useful when debugging `batch_processing.py` beyond the high-level pipeline status rows, or reviewing pipeline error messages
   - Key columns: `Table_ID`, `Log_ID`, `Sequence_Number`, `Step_Name`, `Message`, `Source_Type`

3. **`Data_Quality_Notifications`** - Data quality check results
   - Logs all data quality warnings and failures
   - Contains data quality category, result type (Warning/Failure)
   - Shows detailed messages about violations
   - Indicates if data was quarantined
   - Links to Log_ID for correlation with pipeline runs
   - Key columns: `Log_ID`, `Data_Quality_Message`

4. **`Schema_Logs`** - Schema change tracking
   - Stores unique schemas for all delta tables
   - Contains Table_ID, table name, and schema ID (MD5 hash)
   - Tracks schema evolution over time
   - Used for detecting schema changes that may cause issues

5. **Other Metadata Tables**
   - Review additional tables in the Metadata Warehouse as needed for extended operational insights

**Example Queries:**

```sql
-- View recent pipeline executions
SELECT TOP 100 *
FROM Data_Pipeline_Logs
ORDER BY Ingestion_Start_Time DESC;

-- Check for failed pipeline runs
SELECT *
FROM Data_Pipeline_Logs
WHERE Ingestion_Status = 'Failed'
ORDER BY Ingestion_Start_Time DESC;

-- Inspect structured activity messages for a specific run
SELECT l.Log_ID, l.Ingestion_Status, n.Table_ID, n.Sequence_Number, n.Step_Name, n.Log_Level, n.Message
FROM Data_Pipeline_Logs l
JOIN Activity_Run_Logs n
   ON l.Log_ID = n.Log_ID
WHERE l.Log_ID = '<log_id>'
ORDER BY n.Sequence_Number;

-- Review data quality issues
SELECT *
FROM Data_Quality_Notifications
WHERE Data_Quality_Result = 'Failure'
ORDER BY Log_ID DESC;

-- Track schema changes for a specific table
SELECT *
FROM Schema_Logs
WHERE Table_Name = 'silver.dbo.customers'
ORDER BY Schema_ID;
```

For detailed logging table schemas and additional monitoring capabilities, see [Runtime and Workflows Guide - Monitoring and Logging](MONITORING_AND_LOGGING.md#application-logging-framework).

### How do I write data to a different workspace?
**Keywords:** different workspace, cross-workspace write, write to another workspace, target different workspace, multi-workspace

Add the target datastore to the `Datastore_Configuration` table in your environment's datastore notebook. See [How do I add a new catalog or warehouse as a target datastore?](#how-do-i-add-a-new-catalog-or-warehouse-as-a-target-datastore) for configuration steps.

### Why is delta__created_datetime or delta__modified_datetime missing from my target table?
**Keywords:** missing timestamp, delta__created_datetime missing, delta__modified_datetime missing, system columns missing, metadata columns not appearing, timestamp not in output, join lost columns, aggregate lost timestamp, delta__raw_folderpath, delta__schema_id

**Also answers:** Why don't I have timestamp columns? Where did my system columns go? Why are delta__ columns missing after transformation? How do I get timestamp columns back? Why did join remove my timestamps? Where is delta__raw_folderpath? Why is delta__schema_id missing?

System columns (`delta__created_datetime`, `delta__modified_datetime`, `delta__raw_folderpath`, `delta__schema_id`) are added before transformations run (Step 6), but certain transformations can filter them out.

**Common causes and fixes:**
- **`join_data`** with explicit column lists: Include `delta__` columns in `left_columns`, or use `*` to keep all left-table columns
- **`aggregate_data` / `pivot_data`**: Row-level timestamps can't survive aggregation — re-add with `derived_column` using `current_timestamp()` after aggregation
- **`select_columns`**: Set `retain_metadata_columns` to `true` (the default) or explicitly include `delta__` columns
- **`union_data`**: Ensure both tables have the system columns

**Note:** `delta__raw_folderpath` only exists for file-based ingestion (Bronze from files) — it won't be present in Silver/Gold reading from Bronze Delta tables.

### Why is my Databricks compute cluster not working? (custom libraries not loading, compute settings ignored)
**Keywords:** environment not working, library not loading, custom library missing, compute settings ignored, Spark configuration not applied, environment publish, save vs publish, ENV_Default, spark_environment_id

**Also answers:** Why aren't my custom libraries available in my notebook? Why are my Spark compute settings not being applied? I configured my environment but nothing changed. My environment changes aren't taking effect.

> ⚠️ **The #1 cause: You saved the environment but didn't publish it.**

in the workspace, **saving** and **publishing** an environment are two separate steps:

| Action | What It Does |
|--------|--------------|
| **Save** | Caches your changes in the system. Does **not** apply them to Spark sessions. |
| **Publish** | Actually applies the library installations, compute settings, and Spark configurations to your environment. |

If you only save without publishing, your notebooks will continue using the previous (or default) environment configuration — custom libraries won't be available and compute settings won't take effect.

**How to publish your environment:**

1. Open your environment in the Databricks UI (e.g., `ENV_Default`)
2. Make your changes (add libraries, adjust compute settings, etc.)
3. Click **Save** to cache the changes
4. Click **Publish** on the Home tab
5. Review the pending changes on the "Pending changes" page
6. Click **Publish all** to apply
7. Wait for the publish operation to complete — a notification appears when done

> 💡 **Important notes:**
> - An environment only accepts **one Publish at a time** — you can't make changes to Libraries or Spark Compute while a publish is in progress
> - If you switch environments during an active Spark session, the new environment doesn't take effect until the **next session**
> - When attaching an environment from a different workspace, the compute configuration from that environment is **ignored** — pool and compute settings default to your current workspace

**Other common causes:**

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Library shows as installed but `import` fails | Environment not published after adding library | Publish the environment |
| Spark cluster size unchanged | Compute settings saved but not published | Publish the environment |
| Environment changes not visible in notebook | Active Spark session using old config | Restart the Spark session (stop and reopen the notebook) |
| Publishing fails with compatibility error | Library incompatible with selected runtime version | Remove the incompatible library, switch runtime, and re-publish |

📖 **Reference:** Databricks cluster policy documentation

---

## Pre-Built Reports

### What reports are included in the solution?
**Keywords:** Power BI reports, pre-built reports, monitoring reports, dashboards, reporting, analytics, visualizations, included reports

**Also answers:** What dashboards come with the accelerator? How do I monitor pipelines visually? What Power BI reports are deployed? Is there a data lineage report?

The solution includes three pre-built Power BI reports with accompanying semantic models that use **Direct Lake mode** for real-time data access:

These are **data engineering lifecycle dashboards**, so keep them in the data engineering repo and deployment flow. Use a separate reporting repo for consumer-facing semantic models and reports.

1. **Data Pipeline Monitoring Report** (`RP_Data_Pipeline_Monitoring`)
   - **Semantic Model**: `SM_Data_Pipeline_Monitoring` (Direct Lake)
   - **Purpose**: Real-time monitoring and operational dashboards for data pipeline execution
   - **Key Features**:
     - Pipeline execution status and history (live updates)
     - Success/failure rates by trigger and Table_ID
     - Watermark tracking for incremental loads
     - Data quality check results
     - Performance metrics (execution times, row counts)
     - Error analysis and troubleshooting views

2. **Exploratory Data Analysis Report** (`RP_Exploratory_Data_Analysis`)
   - **Semantic Model**: `SM_Exploratory_Data_Analysis` (Direct Lake)
   - **Purpose**: Automated data profiling and quality insights
   - **Key Features**:
     - Schema evolution tracking
     - Column-level statistics and distributions
     - Data quality summaries
     - Null analysis and completeness metrics
     - Cardinality analysis

3. **Data Lineage Report** (`RP_Data_Lineage`)
   - **Semantic Model**: `SM_Data_Lineage` (Direct Lake)
   - **Purpose**: Visualize data lineage and track source-to-target relationships across your data pipelines
   - **Key Features**:
     - **Lineage Overview**: AI-generated narrative summaries of data flow from source to target
     - **Table Overview**: Detailed view of transformations and data quality checks applied to each entity
     - Source-to-target relationship tracking across medallion layers (Bronze → Silver → Gold)
     - External source system identification (Oracle, SQL Server, files, etc.)
     - Lineage depth and path visualization
     - Impact analysis for upstream/downstream dependencies
     - Copilot-powered natural language summaries of data transformations
   - **Generation**: Run `generate_lineage.py` notebook to populate lineage data from metadata tables and execution logs

**Direct Lake Benefits**: All semantic models connect directly to Delta tables in the Metadata Warehouse using Direct Lake mode, providing the fastest query performance and eliminating the need for scheduled data refreshes. Reports automatically reflect live data updates as pipelines execute.

### How do I access the pre-built reports?
**Keywords:** open reports, view reports, access reports, find reports, where are reports, report location

After deploying the solution using the [Deployment Guide](Deployment_Guide.md):
1. Navigate to your Databricks workspace
2. Look for the reports:
   - `RP_Data_Pipeline_Monitoring`
   - `RP_Exploratory_Data_Analysis`
   - `RP_Data_Lineage`
3. Open the report directly from the workspace

The reports are automatically connected to their respective semantic models and the Metadata Warehouse.

### What data sources do the reports use?
**Keywords:** report data sources, semantic models, report connections, data source, report queries, underlying data

All three reports connect to the **Metadata Warehouse** via Direct Lake semantic models. Each report/model uses different tables:

| Report | Semantic Model | Tables Used |
|--------|---------------|-------------|
| **Data Pipeline Monitoring** | `SM_Data_Pipeline_Monitoring` | `Data_Pipeline_Logs`, `Data_Quality_Notifications`, `Date_Dimension` |
| **Exploratory Data Analysis** | `SM_Exploratory_Data_Analysis` | `Exploratory_Data_Analysis_Results`, `Schema_Changes`, `Date_Dimension` |
| **Data Lineage** | `SM_Data_Lineage` | `Data_Pipeline_Metadata_Orchestration`, `Data_Pipeline_Metadata_Primary_Configuration`, `Data_Pipeline_Metadata_Advanced_Configuration`, `Data_Pipeline_Lineage` |

### How do I troubleshoot pipeline failures using the reports?
**Keywords:** troubleshoot with reports, debug using reports, report troubleshooting, analyze failures, investigate errors

Using the **Data Pipeline Monitoring Report**:
1. Open the report in your workspace
2. Filter by:
   - **Trigger Name**: Select your specific trigger
   - **Date Range**: Narrow down to the failure period
   - **Status**: Filter to "Failed" executions
3. Review the error details page which shows:
   - Error messages and stack traces
   - Table_IDs that failed
   - Timestamp of failure
   - Number of rows processed before failure
4. Cross-reference with the quarantine tables if data quality issues are suspected

### How do I use the Exploratory Data Analysis report?
**Keywords:** EDA report, exploratory analysis, data profiling, data analysis report, profile data, data statistics

The **Exploratory Data Analysis Report** is generated by running the `Exploratory_Data_Analysis` pipeline:
1. Run the EDA pipeline on specific tables you want to analyze
2. Open the `RP_Exploratory_Data_Analysis` report
3. View automated insights including:
   - Column data types and nullability
   - Value distributions and outliers
   - Schema changes over time
   - Data quality trends
   - Cardinality analysis for potential join keys

This report helps data engineers understand new data sources and monitor data quality without manual analysis.

### How is exploratory data analysis controlled?
**Keywords:** EDA, exploratory analysis, data profiling, profiling frequency, run_exploratory_analysis, data_profiling_frequency, disable EDA, EDA scheduling, profiling schedule, run_exploratory.py_Data_Analysis, Exploratory_Data_Analysis

**Also answers:** How do I enable or disable EDA? How often does profiling run? How do I control which tables get profiled? How do I change the profiling schedule? Can I turn off data profiling?

Exploratory data analysis is controlled at **two levels**:

#### 1. Pipeline-Level: Enable/Disable EDA Entirely

The `run_exploratory_analysis` parameter on `Trigger Step Orchestrator_For_Each_Trigger_Step` controls whether the EDA pipeline runs after data movement completes:

| Value | Behavior |
|-------|----------|
| `True` (default) | Runs `Exploratory_Data_Analysis` after all data movement steps finish |
| `False` | Skips EDA entirely — useful for time-sensitive loads or dev/test runs |

You can set this per-execution when running the pipeline manually, or configure it in your trigger pipeline's invoke activity.

#### 2. Table-Level: Control Profiling Frequency

The `data_profiling_frequency` setting in `other_settings` controls how often each individual table is profiled:

```sql
(@table_id, 'other_settings', 'data_profiling_frequency', 'weekly'),
```

| Frequency | Behavior |
|-----------|----------|
| `weekly` (default) | Profiles the table if 7+ days since last profiling run |
| `daily` | Profiles the table if 1+ days since last run |
| `monthly` | Profiles the table if 30+ days since last run |
| `never` | Excludes the table from profiling entirely |

The stored procedure `[dbo].[Get_Exploratory_Analysis_Input]` checks each table's last profiling timestamp and only returns tables that are due for profiling based on their configured frequency.

#### What Gets Profiled

| Medallion Layer | Analysis Level |
|-----------------|----------------|
| **Bronze** | Minimal — row counts and column metadata only (for performance) |
| **Silver / Gold** | Full — distinct counts, null percentages, mean, std dev, min/max for numeric columns, min/max for date columns |
| **Warehouse tables** | Skipped — EDA only supports Delta tables |

> **Tip:** For large tables that don't change frequently, set `data_profiling_frequency` to `monthly` to reduce compute costs. For tables you never need profiled, set it to `never`.

### How do I use the Data Lineage report?
**Keywords:** data lineage, lineage report, source to target, data flow, impact analysis, upstream, downstream, generate_lineage.py, lineage overview

**Also answers:** How do I see source to target mapping? How do I do impact analysis? How do I trace data flow? How do I generate lineage data? Where do I see what transforms are applied?

The **Data Lineage Report** (`RP_Data_Lineage`) visualizes data flow and source-to-target relationships across your data pipelines:

1. **Generate lineage data** by running the `generate_lineage.py` notebook:
   - Open the notebook in your Databricks workspace
   - Optionally configure `trigger_name` to filter to a specific trigger
   - Ensure `persist_to_table = True` to save lineage to the `Data_Pipeline_Lineage` table
   - Run all cells

> **Important:** If a specific table shows no lineage rows, run `generate_lineage.py` to generate lineage data, then refresh the report or re-run the investigation.

2. **Open the `RP_Data_Lineage` report** and use the filters:
   - **Target Datastore**: Select the volume (Bronze, Silver, or Gold)
   - **Target Entity**: Select the specific table to analyze

3. **Explore the report pages**:
   - **Lineage Overview**: AI-generated narrative showing complete data flow path, joins, and transformations
   - **Table Overview**: Detailed breakdown of all transformations and data quality checks applied

**Key Use Cases**:
- **Impact Analysis**: Understand what downstream tables are affected when a source changes
- **Root Cause Analysis**: Trace data issues back to their source
- **Documentation**: Auto-generate data flow documentation for governance
- **Change Management**: Assess the scope of changes before modifying upstream tables

### Can I customize the pre-built reports?
**Keywords:** customize reports, modify reports, edit reports, change reports, report customization, personalize reports

Yes! The reports are standard Power BI reports that you can customize:
1. **Open in Power BI Desktop**:
   - Download the report from Databricks workspace
   - Open in Power BI Desktop
   - Modify visuals, filters, and layouts
   - Publish back to the workspace

2. **Modify the Semantic Model**:
   - Edit the semantic models (`SM_Data_Pipeline_Monitoring`, `SM_Exploratory_Data_Analysis`, or `SM_Data_Lineage`)
   - Add calculated columns, measures, or relationships
   - Update the data source connections if needed

3. **Create New Reports**:
   - Use the existing semantic models as a foundation
   - Build custom reports for specific monitoring needs

### How often is the report data refreshed?
**Keywords:** refresh, data refresh, report refresh, update frequency, real-time data, Direct Lake, refresh schedule

All semantic models use **Direct Lake mode**, which queries data directly from Delta tables without import or scheduled refreshes:

- **Data Pipeline Monitoring**: Data reflects live updates as pipelines execute. Simply refresh the report page in your browser to see the latest pipeline runs, errors, and metrics. No data refresh configuration needed.
- **Exploratory Data Analysis**: Data is generated when the `Exploratory_Data_Analysis` pipeline runs. Results are immediately available in the report due to Direct Lake's real-time connectivity. This pipeline runs automatically after `Trigger Step Orchestrator_For_Each_Trigger_Step` completes when the `run_exploratory_analysis` parameter is set to `True` (the default).
- **Data Lineage**: Data is generated when you run the `generate_lineage.py` notebook. Results are immediately available in the report. Re-run the notebook after metadata changes to refresh lineage data.
- **Performance**: Direct Lake provides sub-second query performance with automatic query caching, combining the speed of in-memory models with the freshness of live connections.

### What if I don't see any data in the reports?
**Keywords:** no data, empty reports, blank reports, reports not showing data, missing data, no visuals

Check the following:
1. **Have you run any pipelines?** The monitoring report requires pipeline execution history
2. **Is the Metadata Warehouse populated?** Verify data exists in logging tables
3. **Workspace permissions**: Ensure you have at least Viewer access to the workspace
4. **Semantic model refresh**: Check if the semantic model needs to be refreshed
5. **Connection issues**: Verify the semantic models are correctly connected to the Metadata Warehouse

### How do I monitor data quality across all pipelines?
**Keywords:** monitor DQ, data quality monitoring, track quality, DQ metrics, quality dashboard, quality tracking

Use the **Data Pipeline Monitoring Report**:
1. Navigate to the "Data Quality" page/view
2. Review the `Data_Quality_Notifications` table data which shows:
   - Number of records quarantined per Table_ID
   - Types of validation failures (pattern match, foreign key, filters)
   - Quarantine trends over time
3. Query quarantine tables directly: `{target_table_name}_quarantined`
4. Set up alerts for DQ thresholds by creating custom report pages with conditional formatting

### Where can I find the report source files?
**Keywords:** report source, report files, PBIX files, report code, report location, source files

The source files are located in the repository:
- **Reports**: `src/RP_Data_Pipeline_Monitoring.Report/`, `src/RP_Exploratory_Data_Analysis.Report/`, and `src/RP_Data_Lineage.Report/`
- **Semantic Models**: `src/SM_Data_Pipeline_Monitoring.SemanticModel/`, `src/SM_Exploratory_Data_Analysis.SemanticModel/`, and `src/SM_Data_Lineage.SemanticModel/`

These are deployed automatically during the solution setup process.

---

## Quick Reference Links

- [Metadata Reference](METADATA_REFERENCE.md)
- [Metadata Generation Guide](METADATA_GENERATION_GUIDE.md)
- [Transformation Patterns Reference](TRANSFORMATION_PATTERNS_REFERENCE.md)
- [Metadata SQL File Format](METADATA_SQL_FILE_FORMAT.md)
- [DevOps Guide](DevOps.md)
- [Deployment Guide](Deployment_Guide.md)
- [Source Code](../src/)



