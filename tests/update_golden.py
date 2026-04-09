"""
Golden Snapshot Updater — Sprint 97
Captures current migration output as golden reference snapshots.

Usage:
    python tests/update_golden.py                  # Snapshot all current output
    python tests/update_golden.py --approve         # Approve & overwrite golden
    python tests/update_golden.py --diff            # Show diff against golden
"""

import argparse
import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent.parent
OUTPUT_DIR = WORKSPACE / "output"
GOLDEN_DIR = WORKSPACE / "tests" / "golden"

# Directories to snapshot (relative to output/)
SNAPSHOT_DIRS = [
    "notebooks",
    "pipelines",
    "sql",
    "schema",
    "dbt",
    "inventory",
    "validation",
]


def _file_hash(path):
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def snapshot_current_output(golden_dir=None):
    """Capture current output/ as golden reference.

    Returns:
        dict with snapshot metadata.
    """
    golden_dir = Path(golden_dir) if golden_dir else GOLDEN_DIR
    golden_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "created": datetime.now(timezone.utc).isoformat(),
        "files": [],
        "directories": [],
    }

    for subdir in SNAPSHOT_DIRS:
        src = OUTPUT_DIR / subdir
        dst = golden_dir / subdir
        if not src.exists():
            continue

        dst.mkdir(parents=True, exist_ok=True)
        metadata["directories"].append(subdir)

        for file_path in sorted(src.rglob("*")):
            if file_path.is_file():
                rel = file_path.relative_to(OUTPUT_DIR)
                dst_file = golden_dir / rel
                dst_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(file_path, dst_file)
                metadata["files"].append({
                    "path": str(rel),
                    "hash": _file_hash(file_path),
                    "size": file_path.stat().st_size,
                })

    # Write metadata
    meta_path = golden_dir / "golden_metadata.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    return metadata


def compare_with_golden(golden_dir=None):
    """Compare current output against golden snapshots.

    Returns:
        dict with comparison results: added, removed, changed, unchanged files.
    """
    golden_dir = Path(golden_dir) if golden_dir else GOLDEN_DIR
    meta_path = golden_dir / "golden_metadata.json"

    if not meta_path.exists():
        return {"error": "No golden snapshot found. Run: python tests/update_golden.py --approve"}

    with open(meta_path, encoding="utf-8") as f:
        golden_meta = json.load(f)

    golden_files = {f["path"]: f["hash"] for f in golden_meta.get("files", [])}
    current_files = {}

    for subdir in SNAPSHOT_DIRS:
        src = OUTPUT_DIR / subdir
        if not src.exists():
            continue
        for file_path in sorted(src.rglob("*")):
            if file_path.is_file():
                rel = str(file_path.relative_to(OUTPUT_DIR))
                current_files[rel] = _file_hash(file_path)

    added = [f for f in current_files if f not in golden_files]
    removed = [f for f in golden_files if f not in current_files]
    changed = [f for f in current_files if f in golden_files and current_files[f] != golden_files[f]]
    unchanged = [f for f in current_files if f in golden_files and current_files[f] == golden_files[f]]

    return {
        "added": added,
        "removed": removed,
        "changed": changed,
        "unchanged": unchanged,
        "total_golden": len(golden_files),
        "total_current": len(current_files),
        "has_drift": len(added) > 0 or len(removed) > 0 or len(changed) > 0,
    }


def generate_diff_report(comparison, golden_dir=None):
    """Generate a human-readable diff report.

    Returns:
        str: Markdown diff report content.
    """
    golden_dir = Path(golden_dir) if golden_dir else GOLDEN_DIR

    lines = [
        "# Golden Snapshot Diff Report",
        "",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]

    if comparison.get("error"):
        lines.append(f"**Error:** {comparison['error']}")
        return "\n".join(lines)

    if not comparison.get("has_drift"):
        lines.append("No drift detected. All outputs match golden snapshot.")
        return "\n".join(lines)

    lines.extend([
        f"**Golden files:** {comparison['total_golden']}",
        f"**Current files:** {comparison['total_current']}",
        "",
    ])

    if comparison["changed"]:
        lines.extend(["## Changed Files", ""])
        for f in comparison["changed"]:
            lines.append(f"- `{f}`")
        lines.append("")

    if comparison["added"]:
        lines.extend(["## New Files (not in golden)", ""])
        for f in comparison["added"]:
            lines.append(f"- `{f}`")
        lines.append("")

    if comparison["removed"]:
        lines.extend(["## Removed Files (in golden but not in current output)", ""])
        for f in comparison["removed"]:
            lines.append(f"- `{f}`")
        lines.append("")

    # File-level diffs for changed files
    if comparison["changed"]:
        lines.extend(["## Detailed Diffs", ""])
        for f in comparison["changed"][:10]:  # Limit to first 10
            golden_path = golden_dir / f
            current_path = OUTPUT_DIR / f
            if golden_path.exists() and current_path.exists():
                try:
                    golden_text = golden_path.read_text(encoding="utf-8").splitlines()
                    current_text = current_path.read_text(encoding="utf-8").splitlines()
                    lines.append(f"### `{f}`")
                    lines.append("```diff")
                    # Simple line-by-line diff
                    max_lines = max(len(golden_text), len(current_text))
                    for i in range(min(max_lines, 50)):  # Limit lines shown
                        g_line = golden_text[i] if i < len(golden_text) else ""
                        c_line = current_text[i] if i < len(current_text) else ""
                        if g_line != c_line:
                            if g_line:
                                lines.append(f"- {g_line}")
                            if c_line:
                                lines.append(f"+ {c_line}")
                    if max_lines > 50:
                        lines.append(f"... ({max_lines - 50} more lines)")
                    lines.append("```")
                    lines.append("")
                except (UnicodeDecodeError, OSError):
                    lines.append(f"### `{f}` (binary — diff skipped)")
                    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Golden Snapshot Manager (Sprint 97)")
    parser.add_argument("--approve", action="store_true",
                        help="Capture current output as new golden reference")
    parser.add_argument("--diff", action="store_true",
                        help="Show diff between current output and golden")
    parser.add_argument("--golden-dir", type=str, default=None)
    args = parser.parse_args()

    golden = Path(args.golden_dir) if args.golden_dir else GOLDEN_DIR

    if args.approve:
        meta = snapshot_current_output(golden)
        print(f"\n  Golden Snapshot Approved")
        print(f"  ──────────────────────")
        print(f"  Files:       {len(meta['files'])}")
        print(f"  Directories: {len(meta['directories'])}")
        print(f"  Location:    {golden}")
        print()
        return

    if args.diff:
        comparison = compare_with_golden(golden)
        report = generate_diff_report(comparison, golden)
        print(report)

        # Also write to file
        report_path = golden / "diff_report.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report, encoding="utf-8")
        print(f"\n  Report written to: {report_path}")
        return

    # Default: show comparison stats
    comparison = compare_with_golden(golden)
    if comparison.get("error"):
        print(f"  {comparison['error']}")
    elif comparison.get("has_drift"):
        print(f"  Drift detected!")
        print(f"    Changed: {len(comparison['changed'])}")
        print(f"    Added:   {len(comparison['added'])}")
        print(f"    Removed: {len(comparison['removed'])}")
        print(f"  Run with --diff for details, or --approve to update golden.")
    else:
        print(f"  No drift. {comparison['total_current']} files match golden snapshot.")


if __name__ == "__main__":
    main()
