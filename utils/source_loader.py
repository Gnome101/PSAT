"""Shared helpers for loading contract sources and Slither findings."""

import json
from pathlib import Path


def load_sources(project_dir: Path) -> str:
    """Concatenate all .sol files in the project's src/ directory."""
    parts = []
    for sol_file in sorted(project_dir.rglob("src/**/*.sol")):
        rel = sol_file.relative_to(project_dir)
        content = sol_file.read_text()
        parts.append(f"// === {rel} ===\n{content}")
    return "\n\n".join(parts)


def load_slither_findings(project_dir: Path) -> str:
    """Load slither results as a summary string."""
    json_path = project_dir / "slither_results.json"
    if not json_path.exists():
        return "No Slither results available."

    data = json.loads(json_path.read_text())
    detectors = data.get("results", {}).get("detectors", [])

    if not detectors:
        return "Slither found no issues."

    lines = []
    for d in detectors:
        impact = d.get("impact", "?")
        check = d.get("check", "?")
        desc = d.get("description", "").strip().split("\n")[0]
        lines.append(f"- [{impact}] {check}: {desc}")

    return "\n".join(lines)
