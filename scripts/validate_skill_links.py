"""
Validate all links in Copilot customization files.

Scans:
  - .github/copilot-instructions.md (global rules)
  - .github/instructions/*.instructions.md (auto-applied context)
  - .github/prompts/*.prompt.md (slash-command workflows)
  - .github/skills/*/SKILL.md (executable skill descriptors)

Checks:
  1. Markdown links [text](path) - target file exists
  2. Backtick/quoted file path references - target file exists
  3. Search Pattern table entries - pattern actually matches in the target doc file
"""
import re
import os
import glob

base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(base)

skill_files = []
skill_files += sorted(glob.glob(".github/instructions/*.instructions.md"))
skill_files += sorted(glob.glob(".github/prompts/*.prompt.md"))
skill_files += sorted(glob.glob(".github/skills/*/SKILL.md"))
skill_files.append(".github/copilot-instructions.md")

# Known directory prefixes for backtick/quoted path references
PATH_PREFIXES = (
    ".github/", "docs/", "src/", "deployment/", "scripts/",
    "automation_scripts/", "data_pipelines/", "integration_tests/",
    "workspace_cicd/", "workspaces/", "ado_pipelines/",
)

results = {
    "file_links_valid": 0,
    "file_links_broken": [],
    "search_patterns_valid": 0,
    "search_patterns_broken": [],
}


def strip_backticks(s):
    """Remove surrounding backticks (single or double) from a string."""
    s = s.strip()
    # Handle double backticks first: ``value``
    if s.startswith("``") and s.endswith("``"):
        return s[2:-2].strip()
    if s.startswith("`") and s.endswith("`"):
        return s[1:-1].strip()
    return s


def find_pattern_in_file(filepath, pattern):
    """Check if a search pattern exists in a file. Returns True/False."""
    full = os.path.join(base, filepath)
    if not os.path.exists(full):
        return None  # file doesn't exist
    with open(full, "r", encoding="utf-8") as f:
        content = f.read()
    return pattern in content


def parse_search_pattern_tables(skill_file, content):
    """
    Parse markdown tables that contain Search Pattern columns.
    
    Handles multiple table formats:
      - | Topic | File | Search Pattern |  (3-col with explicit file)
      - | Transformation | Search Pattern | (2-col with implicit file stated above)
      - | Type | Search Pattern | Use Case |  (3-col with implicit file, pattern in col 2)
    
    Also handles inline references like:
      `docs/FAQ.md` → `### heading`
    """
    lines = content.split("\n")
    
    # Track "All use: `file`" context for 2-column tables
    implicit_file = None
    
    for i, line in enumerate(lines):
        # Detect "All use: `file`" lines
        all_use_match = re.search(r'All use:\s*`([^`]+)`', line)
        if all_use_match:
            implicit_file = all_use_match.group(1)
            continue
        
        # Detect table header rows with "Search Pattern"
        if "Search Pattern" in line and "|" in line:
            cols = [c.strip() for c in line.strip().strip("|").split("|")]
            # Find column indices
            sp_idx = None
            file_idx = None
            for ci, col in enumerate(cols):
                if "Search Pattern" in col:
                    sp_idx = ci
                if col.strip().lower() == "file":
                    file_idx = ci
            
            if sp_idx is None:
                continue
            
            # Skip separator row (next line should be |---|---|)
            j = i + 1
            if j < len(lines) and re.match(r'\s*\|[\s\-:|]+\|', lines[j]):
                j += 1
            
            # Parse data rows
            while j < len(lines):
                row = lines[j].strip()
                if not row.startswith("|"):
                    break
                
                cells = [c.strip() for c in row.strip().strip("|").split("|")]
                
                if len(cells) <= sp_idx:
                    j += 1
                    continue
                
                pattern_cell = strip_backticks(cells[sp_idx])
                
                if not pattern_cell or pattern_cell.startswith("---"):
                    j += 1
                    continue
                
                # Determine target file
                target_file = None
                if file_idx is not None and file_idx < len(cells):
                    target_file = strip_backticks(cells[file_idx])
                
                if not target_file:
                    target_file = implicit_file
                
                # Handle inline "file → pattern" format in pattern cell
                # Only split on → if the pattern looks like a file path followed by a heading
                # Don't split if the → is part of an actual heading (e.g., "Silver → Gold")
                arrow_match = re.match(
                    r'((?:docs|src|deployment|scripts|automation_scripts)/[^→]+)→\s*`?([^`]+)`?',
                    pattern_cell,
                )
                if arrow_match:
                    target_file = arrow_match.group(1).strip().strip("`")
                    pattern_cell = arrow_match.group(2).strip().strip("`")
                
                if target_file and pattern_cell:
                    yield (j + 1, target_file, pattern_cell)
                
                j += 1
            continue
        
        # Handle standalone inline references: `docs/FAQ.md` → `### heading`
        arrow_ref = re.search(
            r'`(docs/[^`]+)`\s*→\s*`([^`]+)`', line
        )
        if arrow_ref:
            yield (i + 1, arrow_ref.group(1), arrow_ref.group(2))


for sf in skill_files:
    full_path = os.path.join(base, sf)
    if not os.path.exists(full_path):
        print(f"SKIP: {sf} not found")
        continue
    with open(full_path, "r", encoding="utf-8") as f:
        content = f.read()

    checked_files = set()

    # === CHECK 1: Markdown links [text](path) ===
    for text, url in re.findall(r"\[([^\]]*)\]\(([^)]+)\)", content):
        if url.startswith(("http://", "https://", "mailto:", "#")):
            continue
        clean = url.split("#")[0]
        if not clean or clean in checked_files:
            continue
        checked_files.add(clean)
        if os.path.exists(os.path.join(base, clean)):
            results["file_links_valid"] += 1
        else:
            results["file_links_broken"].append((sf, f"[{text}]({url})", clean))

    # === CHECK 2: Backtick/quoted file path references ===
    for match in re.finditer(r"[`\"](([^`\"]+))[`\"]", content):
        path = match.group(1)
        # Skip glob patterns (contain *)
        if "*" in path:
            continue
        if not any(path.startswith(p) for p in PATH_PREFIXES):
            continue
        clean = path.split("#")[0]
        if clean in checked_files:
            continue
        checked_files.add(clean)
        if os.path.exists(os.path.join(base, clean)):
            results["file_links_valid"] += 1
        else:
            results["file_links_broken"].append(
                (sf, f"backtick ref: `{path}`", clean)
            )

    # === CHECK 3: Search Pattern table entries ===
    checked_patterns = set()
    for line_num, target_file, pattern in parse_search_pattern_tables(sf, content):
        key = (target_file, pattern)
        if key in checked_patterns:
            continue
        checked_patterns.add(key)

        # First check file exists
        file_full = os.path.join(base, target_file)
        if not os.path.exists(file_full):
            results["search_patterns_broken"].append(
                (sf, line_num, target_file, pattern, "FILE NOT FOUND")
            )
            continue

        # Then check pattern exists in file
        found = find_pattern_in_file(target_file, pattern)
        if found:
            results["search_patterns_valid"] += 1
        else:
            results["search_patterns_broken"].append(
                (sf, line_num, target_file, pattern, "PATTERN NOT FOUND IN FILE")
            )


# === PRINT RESULTS ===
print()
print("=" * 60)
print("  COPILOT CUSTOMIZATION FILE LINK & SEARCH PATTERN VALIDATION")
print("=" * 60)
print(f"  Files scanned: {len(skill_files)}")
print()

# File links
total_file = results["file_links_valid"] + len(results["file_links_broken"])
print(f"  FILE LINKS: {results['file_links_valid']}/{total_file} valid")
if results["file_links_broken"]:
    for src, ref, target in results["file_links_broken"]:
        print(f"    ❌ {src}")
        print(f"       {ref}")
        print(f"       Missing: {target}")
else:
    print(f"    ✅ All file links valid")

print()

# Search patterns
total_sp = results["search_patterns_valid"] + len(results["search_patterns_broken"])
print(f"  SEARCH PATTERNS: {results['search_patterns_valid']}/{total_sp} valid")
if results["search_patterns_broken"]:
    for src, line_num, target_file, pattern, reason in results["search_patterns_broken"]:
        print(f"    ❌ {src} (line {line_num})")
        print(f"       File: {target_file}")
        print(f"       Pattern: {pattern}")
        print(f"       Reason: {reason}")
else:
    print(f"    ✅ All search patterns valid")

print()
total_broken = len(results["file_links_broken"]) + len(results["search_patterns_broken"])
if total_broken == 0:
    print("  ✅ ALL CHECKS PASSED!")
else:
    print(f"  ⚠️  {total_broken} BROKEN REFERENCE(S) FOUND")
print("=" * 60)
