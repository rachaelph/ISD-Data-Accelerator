from __future__ import annotations

import re
from typing import Any


WORKFLOW_CAPABILITIES_VERSION = 1


STAGE_CAPABILITIES: dict[str, dict[str, Any]] = {
    "author": {
        "prompt_command": "/fdp-03-author",
        "prompt_file": ".github/prompts/fdp-03-author.prompt.md",
        "summary": "Create or edit metadata SQL, DQ rules, transformations, and custom notebooks.",
        "side_effects": "writes files",
        "auto_proceed": True,
    },
    "convert": {
        "prompt_command": "/fdp-03-convert",
        "prompt_file": ".github/prompts/fdp-03-convert.prompt.md",
        "summary": "Convert legacy SQL, Python, or ETL logic into accelerator metadata.",
        "side_effects": "writes files",
        "auto_proceed": True,
    },
    "validate_metadata": {
        "prompt_command": None,
        "prompt_file": ".github/prompts/fdp-03-author.prompt.md",
        "summary": "Run metadata validation until the edited SQL passes.",
        "runner": "python .github/skills/metadata-validation/validate_metadata_sql.py <file>",
        "side_effects": "none",
        "auto_proceed": True,
    },
    "commit": {
        "prompt_command": "/fdp-04-commit",
        "prompt_file": ".github/prompts/fdp-04-commit.prompt.md",
        "summary": "Commit, sync, and deploy metadata changes.",
        "runner": "python automation_scripts/agents/commit_pipeline.py ...",
        "side_effects": "git push and Fabric sync",
        "auto_proceed": True,
        "approval_policy": "confirm_if_no_feature_overrides",
    },
    "run": {
        "prompt_command": "/fdp-05-run",
        "prompt_file": ".github/prompts/fdp-05-run.prompt.md",
        "summary": "Execute a single table or trigger exactly once.",
        "runner": "python automation_scripts/agents/run_pipeline.py ...",
        "side_effects": "starts Fabric execution",
        "auto_proceed": True,
        "approval_policy": "confirm_if_cancelled_or_target_ambiguous",
    },
    "investigate": {
        "prompt_command": "/fdp-06-investigate",
        "prompt_file": ".github/prompts/fdp-06-investigate.prompt.md",
        "summary": "Inspect run outcomes, DQ, schema, lineage, profiling, and live data.",
        "side_effects": "read-only unless operation query uses --resolve",
        "auto_proceed": True,
    },
    "troubleshoot": {
        "prompt_command": "/fdp-06-troubleshoot",
        "prompt_file": ".github/prompts/fdp-06-troubleshoot.prompt.md",
        "summary": "Drill into failed runs and notebook errors before suggesting fixes.",
        "side_effects": "read-only",
        "auto_proceed": True,
    },
}


COMMAND_CAPABILITIES: list[dict[str, Any]] = [
    {
        "id": "fdp-00-end-to-end",
        "command": "/fdp-00-end-to-end",
        "prompt_file": ".github/prompts/fdp-00-end-to-end.prompt.md",
        "category": "planner",
        "summary": "Plan and execute an end-to-end workflow from one intent.",
        "intent_keywords": ["end to end", "plan", "execute", "verify it worked", "show me it worked"],
    },
    {
        "id": "fdp-01-onboarding",
        "command": "/fdp-01-onboarding",
        "prompt_file": ".github/prompts/fdp-01-onboarding.prompt.md",
        "category": "entrypoint",
        "summary": "Get started with the accelerator.",
        "intent_keywords": ["get started", "new user", "onboarding"],
    },
    {
        "id": "fdp-02-deploy",
        "command": "/fdp-02-deploy",
        "prompt_file": ".github/prompts/fdp-02-deploy.prompt.md",
        "category": "deployment",
        "summary": "Deploy the accelerator into a Fabric workspace.",
        "intent_keywords": ["deploy accelerator", "initial setup"],
    },
    {
        "id": "fdp-02-feature",
        "command": "/fdp-02-feature",
        "prompt_file": ".github/prompts/fdp-02-feature.prompt.md",
        "category": "workspace_setup",
        "summary": "Set up a feature branch and workspace.",
        "intent_keywords": ["feature workspace", "isolated workspace"],
    },
    {
        "id": "fdp-03-author",
        "command": "/fdp-03-author",
        "prompt_file": ".github/prompts/fdp-03-author.prompt.md",
        "category": "authoring",
        "summary": "Create or modify metadata, DQ rules, transformations, and custom notebooks.",
        "intent_keywords": ["metadata", "dq rule", "transformation", "custom notebook", "author"],
    },
    {
        "id": "fdp-03-convert",
        "command": "/fdp-03-convert",
        "prompt_file": ".github/prompts/fdp-03-convert.prompt.md",
        "category": "authoring",
        "summary": "Convert existing SQL, Python, or ETL into accelerator metadata.",
        "intent_keywords": ["convert", "legacy sql", "ssis", "informatica", "adf"],
    },
    {
        "id": "fdp-04-commit",
        "command": "/fdp-04-commit",
        "prompt_file": ".github/prompts/fdp-04-commit.prompt.md",
        "category": "deployment",
        "summary": "Commit, sync workspace, and deploy metadata.",
        "intent_keywords": ["commit", "deploy metadata", "sync workspace"],
    },
    {
        "id": "fdp-05-run",
        "command": "/fdp-05-run",
        "prompt_file": ".github/prompts/fdp-05-run.prompt.md",
        "category": "execution",
        "summary": "Execute a pipeline or notebook run.",
        "intent_keywords": ["run", "rerun", "execute pipeline", "execute table"],
    },
    {
        "id": "fdp-06-investigate",
        "command": "/fdp-06-investigate",
        "prompt_file": ".github/prompts/fdp-06-investigate.prompt.md",
        "category": "observability",
        "summary": "Investigate run health, DQ, schema, lineage, profiling, and live data.",
        "intent_keywords": ["investigate", "dq", "schema", "lineage", "profile"],
    },
    {
        "id": "fdp-06-troubleshoot",
        "command": "/fdp-06-troubleshoot",
        "prompt_file": ".github/prompts/fdp-06-troubleshoot.prompt.md",
        "category": "observability",
        "summary": "Troubleshoot failed pipelines and notebooks.",
        "intent_keywords": ["failed", "error", "why did", "troubleshoot"],
    },
    {
        "id": "fdp-99-maintain",
        "command": "/fdp-99-maintain",
        "prompt_file": ".github/prompts/fdp-99-maintain.prompt.md",
        "category": "maintenance",
        "summary": "Maintain core accelerator internals.",
        "intent_keywords": ["maintain", "core internals"],
    },
    {
        "id": "fdp-99-audit",
        "command": "/fdp-99-audit",
        "prompt_file": ".github/prompts/fdp-99-audit.prompt.md",
        "category": "maintenance",
        "summary": "Audit docs for gaps and stale guidance.",
        "intent_keywords": ["audit docs", "documentation gaps"],
    },
    {
        "id": "fdp-99-changelog",
        "command": "/fdp-99-changelog",
        "prompt_file": ".github/prompts/fdp-99-changelog.prompt.md",
        "category": "maintenance",
        "summary": "Generate a changelog from the net diff.",
        "intent_keywords": ["changelog", "release notes"],
    },
    {
        "id": "fdp-99-prompt",
        "command": "/fdp-99-prompt",
        "prompt_file": ".github/prompts/fdp-99-prompt.prompt.md",
        "category": "maintenance",
        "summary": "Maintain prompt and customization files.",
        "intent_keywords": ["prompt", "customization", "copilot instructions"],
    },
]


PLANNER_RECIPES: list[dict[str, Any]] = [
    {
        "id": "convert_validate_commit_run_investigate",
        "summary": "Convert legacy logic, validate the metadata, deploy it, run it, and inspect the outcome.",
        "stages": ["convert", "validate_metadata", "commit", "run", "investigate"],
        "required_inputs": ["target_table_or_trigger"],
        "keyword_groups_all": [
            ["convert", "migration", "migrate", "legacy", "ssis", "informatica", "adf"],
            ["run", "execute", "verify", "prove", "show me it worked", "test"],
        ],
        "optional_keywords_any": ["metadata", "pipeline", "table", "trigger", "worked", "result"],
        "investigation_strategy": "post_run_validation",
        "priority": 100,
    },
    {
        "id": "author_validate_commit_run_investigate",
        "summary": "Author or edit metadata, validate it, deploy it, run it, and inspect the outcome.",
        "stages": ["author", "validate_metadata", "commit", "run", "investigate"],
        "required_inputs": ["target_table_or_trigger"],
        "keyword_groups_all": [
            ["add", "create", "edit", "update", "change", "author"],
            ["metadata", "dq", "rule", "freshness", "transformation", "entity", "pipeline", "notebook"],
            ["run", "execute", "verify", "prove", "show me it worked", "show me it works", "test", "end to end"],
        ],
        "optional_keywords_any": ["deploy", "commit", "investigate", "worked", "outcome", "result"],
        "investigation_strategy": "post_run_validation",
        "priority": 90,
    },
    {
        "id": "author_validate_commit",
        "summary": "Author or edit metadata, validate it, and deploy it.",
        "stages": ["author", "validate_metadata", "commit"],
        "required_inputs": [],
        "keyword_groups_all": [
            ["add", "create", "edit", "update", "change", "author"],
            ["metadata", "dq", "rule", "freshness", "transformation", "entity", "pipeline", "notebook"],
            ["deploy", "commit", "sync", "push"],
        ],
        "optional_keywords_any": ["workspace", "metadata deploy"],
        "investigation_strategy": None,
        "priority": 80,
    },
    {
        "id": "author_validate_only",
        "summary": "Author or edit metadata and validate it before stopping.",
        "stages": ["author", "validate_metadata"],
        "required_inputs": [],
        "keyword_groups_all": [
            ["add", "create", "edit", "update", "change", "author"],
            ["metadata", "dq", "rule", "freshness", "transformation", "entity", "pipeline", "notebook"],
        ],
        "optional_keywords_any": ["validate", "check", "fix"],
        "investigation_strategy": None,
        "priority": 60,
    },
    {
        "id": "run_investigate",
        "summary": "Execute an existing pipeline or notebook run and inspect the outcome.",
        "stages": ["run", "investigate"],
        "required_inputs": ["target_table_or_trigger"],
        "keyword_groups_all": [
            ["run", "rerun", "execute"],
            ["investigate", "inspect", "verify", "show", "outcome", "worked", "result"],
        ],
        "optional_keywords_any": ["table", "trigger", "pipeline", "notebook"],
        "investigation_strategy": "post_run_validation",
        "priority": 70,
    },
    {
        "id": "investigate_only",
        "summary": "Investigate current platform state without making changes.",
        "stages": ["investigate"],
        "required_inputs": [],
        "keyword_groups_all": [
            ["investigate", "check", "inspect", "show", "look at"],
            ["dq", "schema", "lineage", "freshness", "profile", "run history", "health", "live data"],
        ],
        "optional_keywords_any": ["table", "trigger", "quarantine"],
        "investigation_strategy": "direct_investigation",
        "priority": 50,
    },
    {
        "id": "troubleshoot_only",
        "summary": "Troubleshoot a failed run instead of chaining the success path.",
        "stages": ["troubleshoot"],
        "required_inputs": [],
        "keyword_groups_all": [
            ["fail", "failed", "error", "broken", "stuck", "why did"],
            ["pipeline", "notebook", "run", "execution", "load"],
        ],
        "optional_keywords_any": ["debug", "troubleshoot", "root cause"],
        "investigation_strategy": "failure_investigation",
        "priority": 85,
    },
]


APPROVAL_BOUNDARIES: list[dict[str, str]] = [
    {
        "stage": "commit",
        "policy_id": "feature_workspace_guard",
        "condition": "HasOverrides == false",
        "reason": "Commit/deploy can target the shared dev workspace when feature overrides are inactive.",
    },
    {
        "stage": "run",
        "policy_id": "cancelled_or_ambiguous_run_guard",
        "condition": "execution_cancelled_previously || target_unresolved",
        "reason": "A prior run may still be active or the run target may still be ambiguous.",
    },
]


def _normalize_intent(intent: str) -> str:
    normalized = intent.casefold()
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def _score_recipe(normalized_intent: str, recipe: dict[str, Any]) -> int:
    score = 0
    for keyword_group in recipe.get("keyword_groups_all", []):
        if not any(keyword in normalized_intent for keyword in keyword_group):
            return -1
        score += 3

    for keyword in recipe.get("optional_keywords_any", []):
        if keyword in normalized_intent:
            score += 1

    return score + int(recipe.get("priority", 0))


def _build_stage_plan(stage_id: str) -> dict[str, Any]:
    stage = dict(STAGE_CAPABILITIES[stage_id])
    stage["stage_id"] = stage_id
    return stage


def resolve_planner_recipe(intent: str) -> dict[str, Any]:
    normalized_intent = _normalize_intent(intent)
    scored_recipes = [
        (recipe, _score_recipe(normalized_intent, recipe))
        for recipe in PLANNER_RECIPES
    ]
    matched_recipes = [entry for entry in scored_recipes if entry[1] >= 0]
    if not matched_recipes:
        fallback_recipe = next(recipe for recipe in PLANNER_RECIPES if recipe["id"] == "investigate_only")
        return {
            "recipe": fallback_recipe,
            "score": 0,
            "normalized_intent": normalized_intent,
            "matched": False,
        }

    recipe, score = max(matched_recipes, key=lambda item: item[1])
    return {
        "recipe": recipe,
        "score": score,
        "normalized_intent": normalized_intent,
        "matched": True,
    }


def build_execution_plan(intent: str) -> dict[str, Any]:
    resolution = resolve_planner_recipe(intent)
    recipe = resolution["recipe"]
    stage_plans = [_build_stage_plan(stage_id) for stage_id in recipe["stages"]]
    matched_commands = [
        stage_plan["prompt_command"]
        for stage_plan in stage_plans
        if stage_plan.get("prompt_command")
    ]
    approval_boundaries = [
        boundary
        for boundary in APPROVAL_BOUNDARIES
        if boundary["stage"] in recipe["stages"]
    ]

    return {
        "version": WORKFLOW_CAPABILITIES_VERSION,
        "intent": intent,
        "normalized_intent": resolution["normalized_intent"],
        "matchedRecipeId": recipe["id"],
        "matched": resolution["matched"],
        "confidence": resolution["score"],
        "summary": recipe["summary"],
        "stages": stage_plans,
        "matchedCommands": matched_commands,
        "requiredInputs": recipe.get("required_inputs", []),
        "approvalBoundaries": approval_boundaries,
        "investigationStrategy": recipe.get("investigation_strategy"),
    }