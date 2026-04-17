"""
Count unit and integration tests in the accelerator.

Usage:
    python scripts/count_tests.py
"""

import re
from pathlib import Path


def count_tests(test_dir: Path) -> dict:
    """Count test functions in all Python test files."""
    results = {}
    total = 0
    
    for test_file in sorted(test_dir.glob("test_*.py")):
        content = test_file.read_text(encoding="utf-8")
        matches = re.findall(r"def test_", content)
        count = len(matches)
        results[test_file.name] = count
        total += count
    
    return {"files": results, "total": total}


def main():
    # Find the test directory relative to this script
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent
    test_dir = repo_root / "integration_tests" / "notebooks"
    
    if not test_dir.exists():
        print(f"Error: Test directory not found: {test_dir}")
        return
    
    results = count_tests(test_dir)
    
    print("=" * 50)
    print("Test Count Summary")
    print("=" * 50)
    print()
    
    for filename, count in results["files"].items():
        print(f"  {filename}: {count} tests")
    
    print()
    print("-" * 50)
    print(f"  Total test files: {len(results['files'])}")
    print(f"  Total tests: {results['total']}")
    print("=" * 50)


if __name__ == "__main__":
    main()
