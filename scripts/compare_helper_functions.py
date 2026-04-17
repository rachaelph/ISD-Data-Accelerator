#!/usr/bin/env python3
"""Compare helper notebook functions vs Data_Engineer.md documentation.

Usage:
    python compare_helper_functions.py [--repo-root <path>]

When no root is provided, the script assumes it is located inside the
ISD-Data-AI-Platform-Accelerator repo (docs/resources). It prints a JSON summary
for each NB_Helper_Functions_* notebook, showing any documented-but-missing or
implemented-but-undocumented functions.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare helper notebooks against documentation.")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="Path to the repository root (defaults to project root).",
    )
    return parser.parse_args()


def load_documented_functions(repo_root: Path) -> dict[str, set[str]]:
    doc_path = repo_root / "docs" / "Data_Engineer.md"
    doc_text = doc_path.read_text(encoding="utf-8")
    pattern = re.compile(r"\|\s*`([^`]+)`\s*\|\s*(NB_Helper_Functions_[123])", re.IGNORECASE)
    documented: dict[str, set[str]] = {f"NB_Helper_Functions_{i}": set() for i in range(1, 4)}
    for fn, notebook in pattern.findall(doc_text):
        documented[notebook].add(fn)
    return documented


def load_notebook_functions(repo_root: Path) -> dict[str, set[str]]:
    results: dict[str, set[str]] = {}
    for i in range(1, 4):
        notebook_name = f"NB_Helper_Functions_{i}"
        nb_path = repo_root / "src" / f"{notebook_name}.Notebook" / "notebook-content.py"
        text = nb_path.read_text(encoding="utf-8")
        functions = set(re.findall(r"^def\s+([A-Za-z_][A-Za-z0-9_]*)\(", text, flags=re.MULTILINE))
        results[notebook_name] = functions
    return results


def compare_functions(documented: dict[str, set[str]], actual: dict[str, set[str]]) -> dict[str, dict[str, object]]:
    comparison: dict[str, dict[str, object]] = {}
    for notebook, doc_names in documented.items():
        actual_names = actual.get(notebook, set())
        missing = sorted(doc_names - actual_names)
        undocumented = sorted(actual_names - doc_names)
        comparison[notebook] = {
            "documented": len(doc_names),
            "implemented": len(actual_names),
            "missing": missing,
            "undocumented": undocumented,
        }
    return comparison


def main() -> None:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    documented = load_documented_functions(repo_root)
    actual = load_notebook_functions(repo_root)
    comparison = compare_functions(documented, actual)
    print(json.dumps(comparison, indent=2))


if __name__ == "__main__":
    main()
