#!/usr/bin/env python3
"""Analyze helper notebook functions for direct or indirect test references."""

from __future__ import annotations

import argparse
import ast
import io
import json
import re
import tokenize
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set, Tuple

DEFAULT_HELPER_PATHS = [
    "src/NB_Helper_Functions_1.Notebook/notebook-content.py",
    "src/NB_Helper_Functions_2.Notebook/notebook-content.py",
    "src/NB_Helper_Functions_3.Notebook/notebook-content.py",
]
STRING_HINT_KEYS = {
    "functions_to_execute",
    "custom_file_ingestion_function",
    "custom_table_ingestion_function",
    "custom_transformation_function",
    "notebooks_to_run",
    "function_name",
    "function_names",
}


@dataclass
class TestFileSignals:
    token_names: Set[str]
    call_names: Set[str]
    attr_names: Set[str]
    heuristic_strings: Set[str]


def read_file_text(path: Path) -> str:
    """Read text with utf-8 fallback."""
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8", errors="ignore")


def extract_helper_functions(helper_paths: Sequence[Path]) -> Dict[Path, List[str]]:
    pattern = re.compile(r"^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", re.MULTILINE)
    helpers: Dict[Path, List[str]] = {}
    for helper in helper_paths:
        text = read_file_text(helper)
        helpers[helper] = pattern.findall(text)
    return helpers


def build_call_graph(
    helper_paths: Sequence[Path],
    helpers: Dict[Path, List[str]],
) -> Dict[Tuple[Path, str], Set[Tuple[Path, str]]]:
    """Capture helper-to-helper calls for indirect coverage propagation."""

    name_to_defs: Dict[str, List[Tuple[Path, str]]] = defaultdict(list)
    for helper_path, names in helpers.items():
        for name in names:
            name_to_defs[name].append((helper_path, name))

    call_graph: Dict[Tuple[Path, str], Set[Tuple[Path, str]]] = defaultdict(set)

    for helper_path in helper_paths:
        text = read_file_text(helper_path)
        try:
            tree = ast.parse(text, filename=str(helper_path))
        except SyntaxError:
            continue

        helper_names = set(helpers.get(helper_path, []))

        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name in helper_names:
                caller_key = (helper_path, node.name)
                call_graph.setdefault(caller_key, set())

                class CallCollector(ast.NodeVisitor):
                    def __init__(self) -> None:
                        self.calls: Set[str] = set()

                    def visit_Call(self, call_node: ast.Call) -> None:  # type: ignore[override]
                        target = call_node.func
                        name: str | None = None
                        if isinstance(target, ast.Name):
                            name = target.id
                        elif isinstance(target, ast.Attribute):
                            name = target.attr
                        if name:
                            self.calls.add(name)
                        self.generic_visit(call_node)

                collector = CallCollector()
                collector.visit(node)
                for call_name in collector.calls:
                    for callee_key in name_to_defs.get(call_name, []):
                        call_graph[caller_key].add(callee_key)

    # Ensure every helper appears in the graph even if it has no outgoing edges
    for helper_path, names in helpers.items():
        for name in names:
            call_graph.setdefault((helper_path, name), set())

    return call_graph


def tokenize_names(text: str) -> Set[str]:
    names: Set[str] = set()
    buff = io.StringIO(text)
    try:
        for tok in tokenize.generate_tokens(buff.readline):
            if tok.type == tokenize.NAME:
                names.add(tok.string)
    except tokenize.TokenError:
        pass
    return names


def flatten_attribute(node: ast.AST) -> str:
    parts: List[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def collect_strings(node: ast.AST) -> List[str]:
    strings: List[str] = []
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        strings.append(node.value)
    elif isinstance(node, ast.List):
        for elt in node.elts:
            strings.extend(collect_strings(elt))
    elif isinstance(node, ast.Tuple):
        for elt in node.elts:
            strings.extend(collect_strings(elt))
    elif isinstance(node, ast.Set):
        for elt in node.elts:
            strings.extend(collect_strings(elt))
    elif isinstance(node, ast.JoinedStr):
        literal = "".join(
            value.value if isinstance(value, ast.Constant) and isinstance(value.value, str) else ""
            for value in node.values
        )
        if literal:
            strings.append(literal)
    return strings


def analyze_test_file(path: Path) -> TestFileSignals:
    text = read_file_text(path)
    token_names = tokenize_names(text)
    try:
        tree = ast.parse(text, filename=str(path))
    except SyntaxError:
        return TestFileSignals(token_names, set(), set(), set())

    call_names: Set[str] = set()
    attr_names: Set[str] = set()
    heuristic_strings: Set[str] = set()

    class CoverageVisitor(ast.NodeVisitor):
        def visit_Call(self, node: ast.Call) -> None:  # type: ignore[override]
            target = node.func
            if isinstance(target, ast.Name):
                call_names.add(target.id)
            elif isinstance(target, ast.Attribute):
                attr_names.add(flatten_attribute(target))
                call_names.add(target.attr)
            self.generic_visit(node)

        def visit_Dict(self, node: ast.Dict) -> None:  # type: ignore[override]
            for key, value in zip(node.keys, node.values):
                if isinstance(key, ast.Constant) and isinstance(key.value, str):
                    key_text = key.value
                    if key_text in STRING_HINT_KEYS:
                        for literal in collect_strings(value):
                            heuristic_strings.update(split_candidates(literal))
            self.generic_visit(node)

    CoverageVisitor().visit(tree)
    return TestFileSignals(token_names, call_names, attr_names, heuristic_strings)


def split_candidates(value: str) -> Set[str]:
    parts = re.split(r"[\s,]+", value.strip())
    return {part for part in parts if part}


def aggregate_signals(test_files: Iterable[Path]) -> Dict[str, TestFileSignals]:
    signals: Dict[str, TestFileSignals] = {}
    for path in test_files:
        signals[str(path)] = analyze_test_file(path)
    return signals


def classify_helpers(
    helpers: Dict[Path, List[str]],
    test_signals: Dict[str, TestFileSignals],
) -> Dict[Path, Dict[str, Dict[str, Set[str]]]]:
    result: Dict[Path, Dict[str, Dict[str, Set[str]]]] = {}
    for helper_path, names in helpers.items():
        helper_entry: Dict[str, Dict[str, Set[str]]] = {}
        for name in names:
            helper_entry[name] = {
                "direct": set(),
                "call": set(),
                "heuristic": set(),
                "graph": set(),
            }
        result[helper_path] = helper_entry

    for test_path, signals in test_signals.items():
        for helper_path, names in helpers.items():
            entry = result[helper_path]
            for name in names:
                if name in signals.token_names:
                    entry[name]["direct"].add(test_path)
                elif name in signals.call_names or any(
                    name == attr.split(".")[-1] for attr in signals.attr_names
                ):
                    entry[name]["call"].add(test_path)
                elif name in signals.heuristic_strings:
                    entry[name]["heuristic"].add(test_path)
    return result


def apply_call_graph_propagation(
    classification: Dict[Path, Dict[str, Dict[str, Set[str]]]],
    call_graph: Dict[Tuple[Path, str], Set[Tuple[Path, str]]],
) -> None:
    """Mark helpers as indirectly covered when invoked by tested helpers."""

    def entry_for(key: Tuple[Path, str]) -> Dict[str, Set[str]]:
        helper_path, func_name = key
        return classification[helper_path][func_name]

    def has_test_coverage(entry: Dict[str, Set[str]]) -> bool:
        return bool(entry["direct"] or entry["call"] or entry["heuristic"])

    covered_keys: Set[Tuple[Path, str]] = set()
    queue: deque[Tuple[Path, str]] = deque()

    for key in call_graph:
        entry = entry_for(key)
        if has_test_coverage(entry):
            covered_keys.add(key)
            queue.append(key)

    while queue:
        caller = queue.popleft()
        caller_path, caller_name = caller
        for callee in call_graph.get(caller, set()):
            callee_entry = entry_for(callee)
            evidence_label = f"{caller_name}@{caller_path.name}"
            callee_entry["graph"].add(evidence_label)
            if callee not in covered_keys:
                covered_keys.add(callee)
                queue.append(callee)


def summarize(classification: Dict[Path, Dict[str, Dict[str, Set[str]]]]) -> Dict[str, object]:
    overall_direct = 0
    overall_indirect = 0
    overall_total = 0
    details: Dict[str, Dict[str, int]] = {}
    function_details: List[Dict[str, object]] = []

    for helper_path, functions in classification.items():
        helper_stats = {"total": 0, "direct": 0, "indirect": 0, "unreferenced": 0}
        for name, buckets in functions.items():
            helper_stats["total"] += 1
            overall_total += 1
            status = "unreferenced"
            evidence = {k: sorted(v) for k, v in buckets.items() if v}
            if buckets["direct"]:
                status = "direct"
                helper_stats["direct"] += 1
                overall_direct += 1
            elif buckets["call"] or buckets["heuristic"] or buckets["graph"]:
                status = "indirect"
                helper_stats["indirect"] += 1
                overall_indirect += 1
            else:
                helper_stats["unreferenced"] += 1
            function_details.append(
                {
                    "notebook": str(helper_path),
                    "function": name,
                    "status": status,
                    "evidence": evidence,
                }
            )
        details[str(helper_path)] = helper_stats

    summary = {
        "total_functions": overall_total,
        "directly_referenced": overall_direct,
        "indirectly_referenced": overall_indirect,
        "unreferenced": overall_total - (overall_direct + overall_indirect),
    }
    return {
        "summary": summary,
        "notebooks": details,
        "functions": function_details,
    }


def find_test_files(tests_dir: Path) -> List[Path]:
    return sorted(tests_dir.rglob("*.py"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tests-dir",
        default=os.path.join("integration_tests", "notebooks"),
        help="Directory containing test files (default: %(default)s)",
    )
    parser.add_argument(
        "--helper",
        dest="helpers",
        action="append",
        help="Path to a helper notebook .py file (default: standard helper notebooks)",
    )
    parser.add_argument(
        "--output",
        choices=["json", "pretty"],
        default="json",
        help="Output format (default: %(default)s)",
    )
    args = parser.parse_args()

    helper_paths = args.helpers if args.helpers else DEFAULT_HELPER_PATHS
    helper_path_objs = [Path(path) for path in helper_paths]

    tests_dir = Path(args.tests_dir)
    test_files = find_test_files(tests_dir)
    if not test_files:
        raise SystemExit(f"No test files found under {tests_dir}")

    helpers = extract_helper_functions(helper_path_objs)
    call_graph = build_call_graph(helper_path_objs, helpers)
    test_signals = aggregate_signals(test_files)
    classification = classify_helpers(helpers, test_signals)
    apply_call_graph_propagation(classification, call_graph)
    data = summarize(classification)

    if args.output == "json":
        print(json.dumps(data, indent=2))
    else:
        pretty_print(data)


def pretty_print(report: Dict[str, object]) -> None:
    summary = report["summary"]
    print("Summary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")
    print("\nDetails by notebook:")
    for notebook, stats in report["notebooks"].items():
        print(f"- {notebook}")
        for key, value in stats.items():
            print(f"    {key}: {value}")


if __name__ == "__main__":
    main()
