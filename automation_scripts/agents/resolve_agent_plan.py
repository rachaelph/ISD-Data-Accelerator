from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from workflow_capabilities import build_execution_plan


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve a deterministic end-to-end workflow plan from a user intent.")
    parser.add_argument("--intent", required=True, help="Natural-language description of what the user wants to accomplish.")
    parser.add_argument("--pretty", action="store_true", help="Emit indented JSON output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    plan = build_execution_plan(args.intent)
    if args.pretty:
        print(json.dumps(plan, indent=2))
    else:
        print(json.dumps(plan, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())