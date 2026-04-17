"""Scan helper function notebooks for unused variables and parameters."""
import re
import sys

files = [
    "src/NB_Helper_Functions_1.Notebook/notebook-content.py",
    "src/NB_Helper_Functions_2.Notebook/notebook-content.py",
    "src/NB_Helper_Functions_3.Notebook/notebook-content.py",
]

for fpath in files:
    with open(fpath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    print(f"=== {fpath} ===")

    # Find function boundaries
    func_starts = []
    for i, line in enumerate(lines):
        m = re.match(r"^def (\w+)\(", line)
        if m:
            func_starts.append((i, m.group(1)))

    for idx, (start, fname) in enumerate(func_starts):
        end = func_starts[idx + 1][0] if idx + 1 < len(func_starts) else len(lines)

        # --- Check for unused parameters ---
        # Collect full signature
        sig_lines = []
        paren_depth = 0
        for sl in range(start, min(start + 30, end)):
            sig_lines.append(lines[sl])
            paren_depth += lines[sl].count("(") - lines[sl].count(")")
            if paren_depth <= 0:
                break
        signature = " ".join(l.strip() for l in sig_lines)

        # Extract param names from signature
        sig_match = re.search(r"\((.+?)\)\s*(?:->|:)", signature, re.DOTALL)
        if sig_match:
            params_str = sig_match.group(1)
            param_names = re.findall(r"(\w+)\s*(?::|,|=|\))", params_str)
            # Remove type hints that got captured
            param_names = [
                p
                for p in param_names
                if p
                not in (
                    "str",
                    "int",
                    "float",
                    "bool",
                    "list",
                    "dict",
                    "tuple",
                    "set",
                    "Any",
                    "None",
                    "DataFrame",
                    "Union",
                    "Optional",
                    "Column",
                )
            ]

            body_text = "".join(lines[start + 1 : end])
            for pname in param_names:
                if pname in ("self", "cls", "args", "kwargs"):
                    continue
                if not re.search(r"\b" + re.escape(pname) + r"\b", body_text):
                    print(f"  L{start+1}: {fname}() -> UNUSED PARAM \"{pname}\"")

        # --- Check for unused local variables ---
        for j in range(start + 1, end):
            line = lines[j]
            stripped = line.strip()
            if stripped.startswith("#") or stripped.startswith("@"):
                continue
            if stripped.startswith("def "):
                continue

            m2 = re.match(r"^\s+(\w+)\s*=\s*.+", line)
            if not m2:
                continue
            varname = m2.group(1)
            if varname.startswith("_") and len(varname) <= 2:
                continue
            if varname in ("self", "cls"):
                continue

            remaining = "".join(lines[j + 1 : end])
            if not re.search(r"\b" + re.escape(varname) + r"\b", remaining):
                print(f"  L{j+1}: {fname}() -> UNUSED VAR \"{varname}\"")

    print()
