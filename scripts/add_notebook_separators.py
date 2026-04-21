"""
Convert helper_functions_*.py to proper Databricks notebook format:
1. Remove __file__/package-style imports (replace with comment)
2. Replace sc._jvm logger with Python logging
3. Replace lazy 'from databricks_batch_engine...' imports with direct calls
4. Insert '# COMMAND ----------' cell separators at section boundaries

Section boundaries are detected by looking for banner-comment TRIPLETS:
    # ===...===
    # SECTION TITLE
    # ===...===
A separator is inserted BEFORE the first '# ===...===' of each triplet.
A separator is also inserted after the top-level import block.
"""

import re
from pathlib import Path

COMMAND_SEP = "# COMMAND ----------"
BANNER_RE = re.compile(r"^# ={10,}\s*$")

REPO = Path(r"c:\Users\rphillips\OneDrive - Microsoft\Repos\ISD-Data-AI-Platform-Accelerator\databricks_batch_engine")


def find_banner_triplets(lines: list[str]) -> set[int]:
    """
    Find lines where a banner-pair section starts.
    Pattern: line i = '# ===', line i+1 = '# TITLE', line i+2 = '# ==='
    Returns the set of line indices for 'line i' (the first banner of each triplet).
    """
    boundaries = set()
    i = 0
    while i < len(lines) - 2:
        if (
            BANNER_RE.match(lines[i].strip())
            and lines[i + 1].strip().startswith("#")
            and not BANNER_RE.match(lines[i + 1].strip())
            and BANNER_RE.match(lines[i + 2].strip())
        ):
            boundaries.add(i)
            i += 3  # Skip past this triplet
        else:
            i += 1
    return boundaries


def find_import_block_end(lines: list[str]) -> int:
    """Find the line index AFTER the last top-level import statement."""
    paren_depth = 0
    last_import_end = -1
    past_header = False

    for i, line in enumerate(lines):
        stripped = line.strip()
        paren_depth += stripped.count("(") - stripped.count(")")

        if not past_header:
            if stripped == "" or stripped.startswith("#"):
                continue
            past_header = True

        if past_header and paren_depth == 0:
            if stripped.startswith("import ") or stripped.startswith("from "):
                last_import_end = i
            elif stripped == "" or stripped.startswith("# ---"):
                continue
            else:
                break

    return last_import_end + 1 if last_import_end >= 0 else -1


def insert_separators(lines: list[str]) -> tuple[list[str], int]:
    """Insert COMMAND separators at section boundaries."""
    import_end = find_import_block_end(lines)
    banner_starts = find_banner_triplets(lines)

    # Combine all boundary points
    boundaries = set(banner_starts)
    if import_end > 0:
        boundaries.add(import_end)

    result = []
    count = 0
    for i, line in enumerate(lines):
        if i in boundaries:
            # Add separator before this line
            # Ensure blank line before separator
            if result and result[-1].strip() not in ("", COMMAND_SEP):
                result.append("")
            if not result or result[-1].strip() != COMMAND_SEP:
                result.append(COMMAND_SEP)
                result.append("")
                count += 1
        result.append(line)

    return result, count


# -- File-specific content patches --

def patch_helper_1(text: str) -> str:
    """Apply Databricks fixes to helper_functions_1.py."""
    # 1. Remove __file__/package imports
    text = text.replace(
        "# --- Databricks Runtime Bootstrap ---\n"
        "import sys as _sys\n"
        "from pathlib import Path as _Path\n"
        "\n"
        "if __package__ in (None, \"\"):\n"
        "    _sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))\n"
        "\n"
        "from databricks_batch_engine.runtime import (\n"
        "    delta_sql_identifier,\n"
        "    delta_target_exists,\n"
        "    describe_history_target,\n"
        "    get_delta_table_for_target,\n"
        "    dbutils,\n"
        "    read_delta_target,\n"
        "    sc,\n"
        "    show_tblproperties_target,\n"
        "    spark,\n"
        "    unset_tblproperty_target,\n"
        "    vacuum_target,\n"
        "    write_delta_target,\n"
        ")",
        "# --- Databricks Runtime Bootstrap ---\n"
        "# When run via %run in a Databricks notebook, runtime symbols (spark, dbutils, etc.)\n"
        "# are already in the shared namespace. No package imports needed."
    )

    # 2. Replace sc._jvm logger
    text = text.replace(
        "    # https://learn.microsoft.com/azure/databricks/admin/account-settings/audit-log-delivery\n"
        "    logger = sc._jvm.org.apache.log4j.LogManager.getLogger(\"spark_application_logger\")",
        "    # Use Python logging instead of sc._jvm (not supported on shared clusters)\n"
        "    import logging\n"
        "    logger = logging.getLogger(\"spark_application_logger\")\n"
        "    if not logger.handlers:\n"
        "        logger.setLevel(logging.DEBUG)\n"
        "        logger.addHandler(logging.StreamHandler())"
    )

    # 3. Fix logger.warn -> logger.warning
    text = text.replace(
        "        logger.warn(message)",
        "        logger.warning(message)"
    )

    # 4. Fix instantiate_notebook lazy import
    text = text.replace(
        "def instantiate_notebook(notebook_name: str, max_retries: int = 3) -> None:\n"
        "    from databricks_batch_engine.helper_functions_3 import load_custom_module as _load_custom_module\n"
        "\n"
        "    _load_custom_module(module_name=notebook_name, max_retries=max_retries)",
        "def instantiate_notebook(notebook_name: str, max_retries: int = 3) -> None:\n"
        "    # load_custom_module is in the shared namespace via %run ./helper_functions_3\n"
        "    load_custom_module(module_name=notebook_name, max_retries=max_retries)"
    )

    return text


def patch_helper_2(text: str) -> str:
    """Apply Databricks fixes to helper_functions_2.py."""
    text = text.replace(
        "import sys as _sys\n"
        "from pathlib import Path as _Path\n"
        "\n"
        "if __package__ in (None, \"\"):\n"
        "    _sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))\n"
        "\n"
        "from databricks_batch_engine.helper_functions_1 import *",
        "# When run via %run in a Databricks notebook, helper_functions_1 symbols\n"
        "# are already in the shared namespace. No package imports needed."
    )

    text = text.replace(
        "def instantiate_notebook(notebook_name: str, max_retries: int = 3) -> None:\n"
        "    from databricks_batch_engine.helper_functions_3 import load_custom_module as _load_custom_module\n"
        "\n"
        "    _load_custom_module(module_name=notebook_name, max_retries=max_retries)",
        "def instantiate_notebook(notebook_name: str, max_retries: int = 3) -> None:\n"
        "    # load_custom_module is in the shared namespace via %run ./helper_functions_3\n"
        "    load_custom_module(module_name=notebook_name, max_retries=max_retries)"
    )

    return text


def patch_helper_3(text: str) -> str:
    """Apply Databricks fixes to helper_functions_3.py."""
    text = text.replace(
        "import sys as _sys\n"
        "from pathlib import Path as _Path\n"
        "\n"
        "if __package__ in (None, \"\"):\n"
        "    _sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))\n"
        "\n"
        "from databricks_batch_engine.helper_functions_1 import *\n"
        "from databricks_batch_engine.runtime import (\n"
        "    DatabricksSqlConnection,\n"
        "    build_databricks_run_url,\n"
        "    delta_target_exists,\n"
        "    load_workspace_module_into_globals,\n"
        "    dbutils,\n"
        "    read_delta_target,\n"
        "    spark,\n"
        "    vacuum_target,\n"
        ")",
        "# When run via %run in a Databricks notebook, runtime and helper_functions_1 symbols\n"
        "# are already in the shared namespace. No package imports needed."
    )

    return text


def process_file(filepath: Path, patch_fn) -> None:
    text = filepath.read_text(encoding="utf-8")

    # Apply content patches first
    text = patch_fn(text)

    # Then insert cell separators
    lines = text.splitlines()
    lines, sep_count = insert_separators(lines)

    filepath.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"{filepath.name}: patched imports + inserted {sep_count} COMMAND separators")


def main():
    process_file(REPO / "helper_functions_1.py", patch_helper_1)
    process_file(REPO / "helper_functions_2.py", patch_helper_2)
    process_file(REPO / "helper_functions_3.py", patch_helper_3)


if __name__ == "__main__":
    main()
