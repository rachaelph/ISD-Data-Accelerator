#!/usr/bin/env python3
"""Commit workspace changes to Git, sync the Databricks Repo, and deploy metadata SQL.

Workflow:
1. Authenticate via Databricks SDK
2. Read workspace config (with overrides)
3. Detect local Git changes (metadata SQL files, deleted triggers)
4. Commit and push to remote
5. Sync Databricks Repo to latest branch
6. Deploy changed metadata SQL via Statement Execution API
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Allow direct script execution to find utils/databricks_agent_utils.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "utils"))

from databricks_agent_utils import (
    AgentError,
    execute_sql_batch,
    get_workspace_client,
    load_datastore_config,
    resolve_engine_folder,
    resolve_execution_context,
    sync_datastore_config_to_delta,
)

LOCAL_NOTEBOOK_PATTERN = re.compile(r"([^/\\]+)\.Notebook(?:[/\\]|$)")
METADATA_NOTEBOOK_PREFIX = "metadata_"


@dataclass
class SqlFile:
    name: str
    full_path: Path
    kind: str


def normalize_notebook_name(notebook_name: str) -> str:
    return notebook_name.removesuffix(".Notebook")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Commit workspace changes to Git, sync the Databricks Repo, and deploy metadata SQL."
    )
    parser.add_argument("--repo-id", help="Databricks Repos ID to sync after push")
    parser.add_argument("--git-folder-name")
    parser.add_argument("--source-directory", type=Path, default=Path("."))
    parser.add_argument("--commit-comment", default="")
    parser.add_argument("--skip-commit", action="store_true")
    parser.add_argument("--skip-sync", action="store_true")
    parser.add_argument("--skip-datastore-sync", action="store_true")
    parser.add_argument("--skip-metadata-deploy", action="store_true")
    parser.add_argument(
        "--environment",
        default="DEV",
        help="Environment name used to locate datastore_<ENV>.json for the Datastore_Configuration sync.",
    )
    parser.add_argument(
        "--prune-datastore-config",
        action="store_true",
        help="Also delete Datastore_Configuration rows not present in the JSON config.",
    )
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def log(message: str = "", *, error: bool = False) -> None:
    stream = sys.stderr if error else sys.stdout
    print(message, file=stream, flush=True)


def run_command(
    command: list[str],
    cwd: Path,
    error_prefix: str,
    env_overrides: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)
    try:
        completed = subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=False, env=env)
    except OSError as exc:
        raise AgentError(f"{error_prefix}: {exc}") from exc
    if completed.returncode != 0:
        message = (completed.stderr or completed.stdout).strip()
        raise AgentError(f"{error_prefix}: {message}")
    return completed


# ---------------------------------------------------------------------------
# Notebook / trigger name helpers
# ---------------------------------------------------------------------------
def parse_notebook_names(changed_paths: list[str]) -> list[str]:
    names: set[str] = set()
    for change in changed_paths:
        match = LOCAL_NOTEBOOK_PATTERN.search(change)
        if match:
            names.add(normalize_notebook_name(match.group(1)))
    return sorted(names)


def parse_sql_file_names(change_lines: list[str]) -> list[str]:
    names: set[str] = set()
    for change_line in change_lines:
        for path in extract_git_status_paths(change_line):
            if path.lower().endswith(".sql"):
                names.add(Path(path).stem)
    return sorted(names)


def metadata_trigger_from_notebook_name(notebook_name: str) -> str | None:
    normalized_name = normalize_notebook_name(notebook_name)
    if not normalized_name.startswith(METADATA_NOTEBOOK_PREFIX):
        return None
    trigger_name = normalized_name[len(METADATA_NOTEBOOK_PREFIX):]
    return trigger_name or None


def extract_git_status_paths(change_line: str) -> list[str]:
    payload = change_line[3:].strip()
    if not payload:
        return []
    if " -> " in payload:
        old_path, new_path = payload.split(" -> ", 1)
        return [old_path.strip(), new_path.strip()]
    return [payload]


def parse_deleted_metadata_triggers_from_git_status(change_lines: list[str]) -> list[str]:
    trigger_names: set[str] = set()
    for change_line in change_lines:
        status_code = change_line[:2]
        paths = extract_git_status_paths(change_line)
        if "D" in status_code:
            candidate_paths = paths
        elif "R" in status_code and len(paths) == 2:
            candidate_paths = [paths[0]]
        else:
            candidate_paths = []

        for path in candidate_paths:
            match = LOCAL_NOTEBOOK_PATTERN.search(path)
            if not match:
                continue
            trigger_name = metadata_trigger_from_notebook_name(match.group(1))
            if trigger_name:
                trigger_names.add(trigger_name)
    return sorted(trigger_names)


def get_committed_change_lines(source_directory: Path, git_folder_name: str | None) -> list[str]:
    cmd = ["git", "diff-tree", "--no-commit-id", "--name-status", "-r", "HEAD"]
    if git_folder_name:
        cmd += ["--", f"{git_folder_name}/"]
    committed = run_command(
        cmd,
        cwd=source_directory,
        error_prefix="git diff-tree failed",
    )
    return [line.strip() for line in committed.stdout.splitlines() if line.strip()]


def get_committed_paths(source_directory: Path, git_folder_name: str | None) -> list[str]:
    committed_change_lines = get_committed_change_lines(source_directory, git_folder_name)
    committed_paths: list[str] = []
    for change_line in committed_change_lines:
        parts = change_line.split("\t")
        if len(parts) < 2:
            continue
        committed_paths.extend(path.strip() for path in parts[1:] if path.strip())
    return committed_paths


def get_local_change_lines(source_directory: Path, git_folder_name: str | None) -> list[str]:
    status_cmd = ["git", "status", "--porcelain", "--untracked-files=all"]
    if git_folder_name:
        status_cmd += ["--", f"{git_folder_name}/"]
    local_status = run_command(
        status_cmd,
        cwd=source_directory,
        error_prefix="git status failed",
    )
    return [line for line in local_status.stdout.splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# SQL file discovery
# ---------------------------------------------------------------------------
def discover_sql_files(source_directory: Path, engine_folder_override: str | None = None) -> list[SqlFile]:
    sql_files: list[SqlFile] = []
    engine = resolve_engine_folder(source_directory, engine_folder_override)
    if engine.metadata_dir.exists():
        for sql_path in sorted(engine.metadata_dir.rglob("*.sql")):
            if sql_path.is_file():
                sql_files.append(SqlFile(name=sql_path.stem, full_path=sql_path, kind="Metadata"))
    return sql_files


def build_metadata_delete_sql(trigger_name: str) -> str:
    escaped_trigger_name = trigger_name.replace("'", "''")
    return f"""DELETE FROM dbo.Data_Pipeline_Metadata_Advanced_Configuration
WHERE Table_ID IN (SELECT Table_ID FROM dbo.Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = '{escaped_trigger_name}');

DELETE FROM dbo.Data_Pipeline_Metadata_Primary_Configuration
WHERE Table_ID IN (SELECT Table_ID FROM dbo.Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = '{escaped_trigger_name}');

DELETE FROM dbo.Data_Pipeline_Metadata_Orchestration
WHERE Trigger_Name = '{escaped_trigger_name}';
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    args = parse_args()
    started_at = time.time()
    result: dict[str, Any] = {
        "localGitStatus": "Skipped",
        "syncStatus": "Skipped",
        "datastoreConfigSyncStatus": "Skipped",
        "datastoreConfigRowsUpserted": 0,
        "datastoreConfigTable": None,
        "metadataDeployStatus": "Skipped",
        "metadataFilesExecuted": 0,
        "metadataTriggersDeleted": 0,
        "branchName": None,
        "hasFeatureOverrides": None,
        "errorMessage": None,
        "durationSeconds": 0,
    }

    changed_notebook_names: set[str] = set()
    changed_sql_file_names: set[str] = set()
    deleted_metadata_trigger_names: set[str] = set()
    has_local_git_folder_changes = False

    try:
        log("")
        log("=========================================")
        log("[START] Commit Pipeline")
        log("=========================================")
        log(f"   Repo ID        : {args.repo_id or '(not specified)'}")
        log(f"   Git Folder     : {args.git_folder_name or '(auto-detect)'}")
        log(f"   Skip Commit    : {args.skip_commit}")
        log(f"   Skip Sync      : {args.skip_sync}")
        log(f"   Skip DS Sync   : {args.skip_datastore_sync}")
        log(f"   Skip MetaDeploy: {args.skip_metadata_deploy}")
        log(f"   Environment    : {args.environment}")
        log("")

        log("Step 1: Authenticating...")
        client = get_workspace_client()
        log(f"   [OK] Authentication successful ({client.config.host})")
        log("")

        log("Step 2: Reading workspace config (with overrides)...")
        execution_context = resolve_execution_context(
            source_directory=args.source_directory,
            git_folder_name=args.git_folder_name,
            required_variables=["sql_warehouse_id", "metadata_catalog", "metadata_schema"],
            environment=args.environment,
        )
        git_folder_name = execution_context["GitFolderName"]
        variables = execution_context["Variables"]
        sql_warehouse_id = variables["sql_warehouse_id"]
        catalog_name = variables["metadata_catalog"]
        schema_name = variables["metadata_schema"]
        metadata_database = f"{catalog_name}.{schema_name}"
        result["branchName"] = execution_context["BranchName"]
        result["hasFeatureOverrides"] = execution_context["HasOverrides"]
        if execution_context["HasOverrides"]:
            log(f"   [OK] {execution_context['OverrideStatusMessage']}")
        else:
            log(f"   [WARN] {execution_context['OverrideStatusMessage']}")
        if git_folder_name:
            log(f"   [OK] Resolved git folder: {git_folder_name}")
        else:
            log("   [OK] No git folder scope — committing from repo root")
        log(f"   [OK] Branch: {execution_context['BranchName']}")
        log("")

        source_directory = args.source_directory.resolve()
        scope_label = f"'{git_folder_name}/'" if git_folder_name else "the repository"

        log("Step 3: Scanning local Git changes for deploy determinism...")
        local_changes = get_local_change_lines(source_directory, git_folder_name)
        if not local_changes:
            log(f"   [INFO] No local file changes in {scope_label}")
        else:
            has_local_git_folder_changes = True
            log(f"   Found {len(local_changes)} local change(s):")
            for change in local_changes:
                log(f"     [CHG] {change}")

            local_notebook_names = parse_notebook_names(local_changes)
            if local_notebook_names:
                changed_notebook_names.update(local_notebook_names)
                log(f"   Local notebook changes added to deploy filter: {', '.join(local_notebook_names)}")

            local_sql_file_names = parse_sql_file_names(local_changes)
            if local_sql_file_names:
                changed_sql_file_names.update(local_sql_file_names)
                log(f"   Local SQL file changes added to deploy filter: {', '.join(local_sql_file_names)}")

            local_deleted_trigger_names = parse_deleted_metadata_triggers_from_git_status(local_changes)
            if local_deleted_trigger_names:
                deleted_metadata_trigger_names.update(local_deleted_trigger_names)
                log(f"   Local deleted metadata triggers: {', '.join(local_deleted_trigger_names)}")
        log("")

        if not args.skip_commit:
            log("Step 4: Checking whether local Git changes need commit/push...")

            if not local_changes:
                log(f"   [INFO] No local file changes in {scope_label} -- skipping local commit")
                result["localGitStatus"] = "NoChanges"
            else:
                commit_comment = args.commit_comment or "chore: deploy workspace changes"
                if git_folder_name:
                    log(f"   Staging all changes under '{git_folder_name}/'...")
                    run_command(["git", "add", "--", f"{git_folder_name}/"], cwd=source_directory, error_prefix="git add failed")
                else:
                    log("   Staging all changes under the repository root...")
                    run_command(["git", "add", "--all"], cwd=source_directory, error_prefix="git add failed")

                log(f"   Committing: {commit_comment}")
                commit_output = run_command(
                    ["git", "commit", "-m", commit_comment],
                    cwd=source_directory,
                    error_prefix="git commit failed",
                )
                first_commit_line = next((line for line in commit_output.stdout.splitlines() if line.strip()), "")
                if first_commit_line:
                    log(f"   {first_commit_line}")

                committed_paths = get_committed_paths(source_directory, git_folder_name)
                committed_notebook_names = parse_notebook_names(committed_paths)
                if committed_notebook_names:
                    changed_notebook_names.update(committed_notebook_names)
                    log(f"   Committed notebook changes added to deploy filter: {', '.join(committed_notebook_names)}")

                committed_sql_file_names = parse_sql_file_names(get_committed_change_lines(source_directory, git_folder_name))
                if committed_sql_file_names:
                    changed_sql_file_names.update(committed_sql_file_names)
                    log(f"   Committed SQL file changes added to deploy filter: {', '.join(committed_sql_file_names)}")

                committed_deleted_trigger_names = parse_deleted_metadata_triggers_from_git_status(
                    get_committed_change_lines(source_directory, git_folder_name)
                )
                if committed_deleted_trigger_names:
                    deleted_metadata_trigger_names.update(committed_deleted_trigger_names)
                    log(f"   Committed deleted metadata triggers: {', '.join(committed_deleted_trigger_names)}")

                log("   Pushing to remote...")
                try:
                    run_command(
                        ["git", "push"],
                        cwd=source_directory,
                        error_prefix="git push failed",
                        env_overrides={"GIT_TERMINAL_PROMPT": "0"},
                    )
                except AgentError as exc:
                    err = str(exc)
                    if any(fragment in err for fragment in [
                        "terminal prompts disabled",
                        "could not read Username",
                        "Authentication failed",
                    ]):
                        raise AgentError(
                            "git push failed: Git credentials are not cached. Run 'git config --global credential.helper manager' "
                            "and push manually once to cache credentials, then re-run."
                        ) from exc
                    raise

                log("   [OK] Local changes committed and pushed to remote")
                result["localGitStatus"] = "Pushed"
        else:
            log("Step 4: Skipping local Git commit (--skip-commit)")
            if local_changes:
                result["localGitStatus"] = "SkippedWithLocalChanges"
                log("   [WARN] Deploy will use local file contents for changed metadata/datastore SQL, but those changes are not committed or synced to the Databricks Repo")
            else:
                result["localGitStatus"] = "Skipped"
        log("")

        if not args.skip_sync:
            repo_id = args.repo_id or execution_context.get("Variables", {}).get("repo_id")
            if repo_id:
                log("Step 5: Syncing Databricks Repo from Git...")
                branch = execution_context["BranchName"]
                client.repos.update(repo_id=int(repo_id), branch=branch)
                log(f"   [OK] Repo {repo_id} synced to branch '{branch}'")
                result["syncStatus"] = "Synced"
            else:
                log("Step 5: Skipping Databricks Repo sync (no --repo-id provided or configured)")
                result["syncStatus"] = "Skipped"
        else:
            log("Step 5: Skipping Databricks Repo sync (--skip-sync)")
        log("")

        if not args.skip_datastore_sync:
            log("Step 6: Syncing datastore JSON config to Datastore_Configuration Delta table...")
            try:
                datastore_config = load_datastore_config(
                    source_directory=args.source_directory,
                    environment=args.environment,
                    branch=execution_context["BranchName"],
                )
                sync_result = sync_datastore_config_to_delta(
                    datastore_config,
                    prune=args.prune_datastore_config,
                )
            except AgentError as exc:
                result["datastoreConfigSyncStatus"] = "Failed"
                raise AgentError(f"Datastore config sync failed: {exc}") from exc
            result["datastoreConfigSyncStatus"] = "Synced"
            result["datastoreConfigRowsUpserted"] = sync_result["rowsUpserted"]
            result["datastoreConfigTable"] = sync_result["tableFullyQualifiedName"]
            log(f"   [OK] Upserted {sync_result['rowsUpserted']} row(s) into {sync_result['tableFullyQualifiedName']}")
            if sync_result["activeBranchOverride"]:
                log(f"   [OK] Branch override active: {sync_result['activeBranchOverride']}")
            if sync_result["pruned"]:
                log("   [OK] Pruned rows not present in JSON config")
        else:
            log("Step 6: Skipping datastore config sync (--skip-datastore-sync)")
        log("")

        if not args.skip_metadata_deploy:
            log("Step 7: Deploying metadata SQL via Statement Execution API...")
            log(f"   SQL Warehouse ID : {sql_warehouse_id}")
            log(f"   Database         : {metadata_database}")
            log("")

            sql_files = discover_sql_files(source_directory)
            available_sql_notebook_names = {normalize_notebook_name(sql_file.name) for sql_file in sql_files}
            changed_sql_notebook_names = sorted(
                name for name in (changed_notebook_names | changed_sql_file_names) if name in available_sql_notebook_names
            )
            deleted_metadata_trigger_names_sorted = sorted(deleted_metadata_trigger_names)

            if changed_sql_notebook_names:
                original_count = len(sql_files)
                sql_files = [
                    sql_file
                    for sql_file in sql_files
                    if normalize_notebook_name(sql_file.name) in changed_sql_notebook_names
                ]
                if len(sql_files) < original_count:
                    log(f"   Filtered to {len(sql_files)} changed file(s) out of {original_count} total")
            elif has_local_git_folder_changes and not deleted_metadata_trigger_names_sorted:
                scope_label = f"'{git_folder_name}/'" if git_folder_name else "the repository"
                log(
                    f"   [INFO] Local changes were detected in {scope_label} but none map to metadata/datastore SQL files -- skipping metadata SQL deployment"
                )
                result["metadataDeployStatus"] = "NoChanges"
            else:
                log("   [INFO] No workspace changes detected - executing all SQL files")

            if result["metadataDeployStatus"] == "NoChanges":
                log("   [INFO] No changed metadata or datastore SQL files to execute - skipping")
            else:
                deleted_trigger_count = 0
                if deleted_metadata_trigger_names_sorted:
                    log(f"   Removing metadata for {len(deleted_metadata_trigger_names_sorted)} deleted trigger(s):")
                    for trigger_name in deleted_metadata_trigger_names_sorted:
                        log(f"      - {trigger_name}")
                    log("")

                    for trigger_name in deleted_metadata_trigger_names_sorted:
                        log(f"   Deleting metadata rows for trigger: {trigger_name}")
                        try:
                            execute_sql_batch(
                                sql_warehouse_id,
                                build_metadata_delete_sql(trigger_name),
                                catalog=catalog_name,
                                schema=schema_name,
                            )
                        except Exception as exc:
                            raise AgentError(
                                f"Metadata cleanup failed for deleted trigger '{trigger_name}': {exc}"
                            ) from exc
                        log("      Success")
                        deleted_trigger_count += 1

                if not sql_files:
                    if deleted_trigger_count:
                        result["metadataDeployStatus"] = "Deployed"
                        result["metadataTriggersDeleted"] = deleted_trigger_count
                        log("   [INFO] No changed metadata or datastore SQL files to execute after cleanup")
                    else:
                        log("   [INFO] No changed metadata or datastore SQL files to execute - skipping")
                        result["metadataDeployStatus"] = "NoChanges"
                else:
                    log(f"   Executing {len(sql_files)} SQL file(s):")
                    for sql_file in sql_files:
                        log(f"      - [{sql_file.kind}] {sql_file.name}")
                    log("")

                    executed_count = 0
                    for sql_file in sql_files:
                        log(f"   Executing: [{sql_file.kind}] {sql_file.name}")
                        sql_text = sql_file.full_path.read_text(encoding="utf-8")
                        try:
                            execute_sql_batch(
                                sql_warehouse_id,
                                sql_text,
                                catalog=catalog_name,
                                schema=schema_name,
                            )
                        except Exception as exc:
                            raise AgentError(f"Metadata SQL execution failed for '{sql_file.name}': {exc}") from exc
                        log("      Success")
                        executed_count += 1

                    result["metadataDeployStatus"] = "Deployed"
                    result["metadataFilesExecuted"] = executed_count
                    result["metadataTriggersDeleted"] = deleted_trigger_count
                    log("")
                    if deleted_trigger_count:
                        log(
                            f"   Success: executed {executed_count} SQL file(s) and cleaned up {deleted_trigger_count} deleted trigger(s)"
                        )
                    else:
                        log(f"   Success: all {executed_count} SQL file(s) executed successfully")
        else:
            log("Step 7: Skipping metadata SQL deployment (--skip-metadata-deploy)")
        log("")

    except AgentError as exc:
        if result["localGitStatus"] == "Skipped" and not args.skip_commit:
            result["localGitStatus"] = "Failed"
        if result["datastoreConfigSyncStatus"] == "Skipped" and not args.skip_datastore_sync:
            result["datastoreConfigSyncStatus"] = "Failed"
        if result["metadataDeployStatus"] == "Skipped" and not args.skip_metadata_deploy:
            result["metadataDeployStatus"] = "Failed"
        result["errorMessage"] = str(exc)
        log(f"[ERROR] {exc}", error=True)
    except Exception as exc:
        result["errorMessage"] = f"Unexpected error: {exc}"
        log(f"[ERROR] Unexpected: {exc}", error=True)
    finally:
        result["durationSeconds"] = int(time.time() - started_at)

    log("=========================================")
    log("[END] Commit Pipeline")
    log("=========================================")
    log("")

    if args.pretty:
        print(json.dumps(result, indent=2))
    else:
        print(json.dumps(result, separators=(",", ":")))
    return 0 if result["errorMessage"] is None else 1


if __name__ == "__main__":
    raise SystemExit(main())