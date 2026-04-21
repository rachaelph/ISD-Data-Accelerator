---
applyTo: "docs/**"
---

# Documentation Instructions

> Auto-applied when working on documentation files.

---

## How to Answer Questions Using Docs

1. **Parse** the user's question for key terms (e.g., "watermark", "incremental", "deploy", "merge type")
2. **grep_search** in `.github/contracts/knowledge-index.v1.json` with those keywords first
3. **read_file** the matched route card and use its `source.path`, `aliases`, `keywords`, `search_hints`, and `linked_docs` to choose the best document
4. If the route card has `preferred_machine_source` and the question asks for exact accepted values, defaults, or validator rules, read that contract first
5. **grep_search** inside the routed markdown document using the user terms plus the route card's `search_hints`
6. **read_file** from the matched markdown section (~50-100 lines to get the full answer)
7. **Fallback** to `grep_search` in `docs/FAQ.md` only when the knowledge index does not produce a clear route
8. **Summarize** the answer for the user

> ⚠️ Do NOT load entire documentation files or the full knowledge index. Use `grep_search` to route, then `read_file` only the relevant JSON slice and markdown section.

---

## Key Documentation Files

Use the generated knowledge-routing contract first, then grep within the routed markdown source. If the routed document is insufficient, search these docs directly:

| Topic | File |
|-------|------|
| Knowledge routing index | `.github/contracts/knowledge-index.v1.json` |
| Metadata contract and accepted values | `.github/contracts/metadata-schema.v1.json` |
| Custom function selection | `docs/CUSTOM_FUNCTION_SELECTION.md` |
| Transformation escalation | `docs/TRANSFORMATION_DECISION_TREE.md` |
| Metadata authoring workflow | `docs/METADATA_AUTHORING_WORKFLOW.md` |
| Ingestion pattern selection | `docs/INGESTION_PATTERNS_REFERENCE.md` |
| Metadata explanation, overview, and validation | `docs/METADATA_REFERENCE.md` |
| Data ingestion scenarios | `docs/DATA_INGESTION_GUIDE.md` |
| Data transformation scenarios | `docs/DATA_TRANSFORMATION_GUIDE.md` |
| Data modeling (dim/fact) | `docs/DATA_MODELING_GUIDE.md` |
| Core components (pipelines, notebooks, SPs) | `docs/CORE_COMPONENTS_REFERENCE.md` |
| Monitoring, logging, reports | `docs/MONITORING_AND_LOGGING.md` |
| Metadata Generation | `docs/METADATA_GENERATION_GUIDE.md` |
| Legacy Code Conversion | `docs/LEGACY_CODE_CONVERSION_GUIDE.md` |
| Transformation Patterns | `docs/TRANSFORMATION_PATTERNS_REFERENCE.md` |
| Metadata SQL File Format | `docs/METADATA_SQL_FILE_FORMAT.md` |
| DevOps/CI-CD | `docs/DevOps.md` |
| Generalizing CI/CD to multiple workspaces | `docs/Generalizing_Workspace_CICD.md` |
| Workspace CI/CD setup & ID replacement | `docs/Workspace_CICD_Guide.md` |
| Architecture patterns | `docs/Medallion_Architecture_Best_Practices.md` |
| Extending the solution | `docs/Extending_the_Accelerator.md` |
| Trigger design, parallelism & dependencies | `docs/Trigger_Design_and_Dependency_Management.md` |
| Deployment to Databricks | `docs/Deployment_Guide.md` |
| Getting started / onboarding | `docs/New_User_Onboarding.md` |
| Security review guidance | `docs/Security_Guidance_for_Reviewers.md` |
| How AI is used in the accelerator | `docs/How_AI_Is_Embedded.md` |
| Time savings / value proposition | `docs/Time_Savings_Analysis.md` |

---

## Response Guidelines

| Do This | Not This |
|---------|----------|
| grep_search the knowledge index first, then grep inside the routed doc | Maintain hardcoded lookup tables |
| Read and summarize documentation | Tell users to "review the documentation" |
| Provide actionable guidance | Give vague pointers |
