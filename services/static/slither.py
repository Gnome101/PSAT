"""Run Slither analysis on a Foundry project and produce a report."""

import json
import subprocess
from pathlib import Path

SEVERITY_ORDER = {"High": 0, "Medium": 1, "Low": 2, "Informational": 3, "Optimization": 4}


def run_slither(project_dir: Path) -> dict:
    """Run slither on a project directory and return the JSON output."""
    commands = [
        ["slither", ".", "--json", "-"],
        ["slither", ".", "--json", "-", "--print", "human-summary"],
    ]
    last_error = None

    for command in commands:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            cwd=project_dir,
        )
        if not result.stdout.strip():
            last_error = RuntimeError(
                f"Slither produced no JSON output (exit {result.returncode}). stderr:\n{result.stderr}"
            )
            continue

        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            last_error = RuntimeError(
                "Failed to parse Slither output. "
                f"exit={result.returncode} stderr:\n{result.stderr}\nstdout:\n{result.stdout[:2000]}"
            )

    if last_error is not None:
        raise last_error
    raise RuntimeError("Failed to run Slither")


def format_report(slither_output: dict, contract_name: str, address: str) -> str:
    """Format slither JSON into a readable text report."""
    lines = []
    lines.append("=" * 70)
    lines.append("  SECURITY ANALYSIS REPORT")
    lines.append(f"  Contract: {contract_name}")
    lines.append(f"  Address:  {address}")
    lines.append("=" * 70)

    detectors = slither_output.get("results", {}).get("detectors", [])

    if not detectors:
        lines.append("\nNo issues found.")
        return "\n".join(lines)

    by_impact = {}
    for d in detectors:
        impact = d.get("impact", "Unknown")
        by_impact.setdefault(impact, []).append(d)

    # summary
    lines.append(f"\nSummary: {len(detectors)} finding(s)")
    for impact in sorted(by_impact.keys(), key=lambda x: SEVERITY_ORDER.get(x, 99)):
        lines.append(f"  {impact}: {len(by_impact[impact])}")

    # details
    for impact in sorted(by_impact.keys(), key=lambda x: SEVERITY_ORDER.get(x, 99)):
        lines.append(f"\n{'─' * 70}")
        lines.append(f"  [{impact.upper()}]")
        lines.append(f"{'─' * 70}")

        for i, finding in enumerate(by_impact[impact], 1):
            check = finding.get("check", "unknown")
            desc = finding.get("description", "").strip()
            confidence = finding.get("confidence", "?")

            lines.append(f"\n  {i}. {check} (confidence: {confidence})")
            for desc_line in desc.split("\n"):
                lines.append(f"     {desc_line}")

    lines.append(f"\n{'=' * 70}")
    return "\n".join(lines)


def analyze(project_dir: Path, contract_name: str, address: str) -> Path:
    """Run full analysis and save reports. Returns path to the text report."""
    slither_output = run_slither(project_dir)

    report = format_report(slither_output, contract_name, address)

    report_path = project_dir / "analysis_report.txt"
    report_path.write_text(report + "\n")

    json_path = project_dir / "slither_results.json"
    json_path.write_text(json.dumps(slither_output, indent=2) + "\n")

    return report_path
