#!/usr/bin/env python3
"""Render Mermaid and Graphviz call graphs from dynamic dependency outputs."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.call_graph import discover_dynamic_dependency_files, export_call_graph_from_file


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate call graph files from dynamic_dependencies.json results."
    )
    parser.add_argument(
        "path",
        nargs="?",
        default="contracts",
        help="Path to a dynamic_dependencies.json file or a directory to scan (default: contracts).",
    )
    parser.add_argument(
        "--base-name",
        default="dynamic_call_graph",
        help="Output filename prefix (default: dynamic_call_graph).",
    )
    args = parser.parse_args()

    source_path = Path(args.path).resolve()
    try:
        files = discover_dynamic_dependency_files(source_path)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if not files:
        print(f"No dynamic_dependencies.json files found under {source_path}")
        return 0

    for dynamic_path in files:
        try:
            mermaid_path, dot_path, html_path = export_call_graph_from_file(
                dynamic_path, base_name=args.base_name
            )
            print(f"{dynamic_path}:")
            print(f"  - {mermaid_path}")
            print(f"  - {dot_path}")
            print(f"  - {html_path}")
        except Exception as exc:  # pragma: no cover - defensive path for malformed data
            print(f"{dynamic_path}: failed ({exc})", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
