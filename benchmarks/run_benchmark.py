"""
Benchmark Harness — Sprint 95 & 96
Times each migration phase at configurable scale, tracks memory usage,
and generates scalability report.

Usage:
    python benchmarks/run_benchmark.py --scale 10
    python benchmarks/run_benchmark.py --scale 100 --profile
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(WORKSPACE))

OUTPUT_DIR = WORKSPACE / "benchmarks" / "results"


def _get_memory_mb():
    """Return current process memory in MB (best-effort)."""
    try:
        import psutil
        return round(psutil.Process().memory_info().rss / (1024 * 1024), 1)
    except ImportError:
        pass
    try:
        import resource
        import platform as _plat
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if _plat.system() == "Darwin":
            return round(usage / (1024 * 1024), 1)
        return round(usage / 1024, 1)
    except ImportError:
        return 0.0


def _generate_synthetic(count, output_dir):
    """Generate synthetic mappings for benchmarking."""
    from benchmarks.generate_mappings import generate_benchmark_mappings
    return generate_benchmark_mappings(count, output_dir=output_dir)


def _run_phase(phase_name, func, *args, profile=False, **kwargs):
    """Run a phase function with timing and optional memory profiling.

    Returns:
        dict with phase metrics.
    """
    mem_before = _get_memory_mb() if profile else 0
    t0 = time.time()
    try:
        result = func(*args, **kwargs)
        elapsed = time.time() - t0
        mem_after = _get_memory_mb() if profile else 0
        return {
            "phase": phase_name,
            "status": "ok",
            "duration_seconds": round(elapsed, 3),
            "memory_before_mb": mem_before,
            "memory_after_mb": mem_after,
            "memory_peak_mb": max(mem_before, mem_after),
            "result": result,
        }
    except Exception as e:
        elapsed = time.time() - t0
        return {
            "phase": phase_name,
            "status": "error",
            "duration_seconds": round(elapsed, 3),
            "memory_before_mb": mem_before,
            "memory_after_mb": 0,
            "memory_peak_mb": mem_before,
            "error": str(e)[:200],
        }


def run_benchmark(scale=10, profile=False, output_dir=None):
    """Run full benchmark at the given scale.

    Args:
        scale: Number of synthetic mappings to generate and process.
        profile: Enable memory profiling.
        output_dir: Output directory for results.

    Returns:
        dict with benchmark results.
    """
    output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # Synthetic input directory
    synth_dir = WORKSPACE / "benchmarks" / "synthetic_input" / "mappings"

    results = {
        "scale": scale,
        "profile": profile,
        "started": datetime.now(timezone.utc).isoformat(),
        "phases": [],
        "throughput": {},
    }

    # Phase 0: Generate synthetic data
    gen_result = _run_phase(
        "generate_synthetic",
        _generate_synthetic, scale, str(synth_dir),
        profile=profile,
    )
    results["phases"].append(gen_result)

    # Phase 1: Assessment
    def _assess():
        import importlib
        os.environ["INFORMATICA_INPUT_DIR"] = str(synth_dir.parent)
        mod = importlib.import_module("run_assessment")
        importlib.reload(mod)
        mod.INPUT_DIR = synth_dir.parent
        mod.main()

    assess_result = _run_phase("assessment", _assess, profile=profile)
    results["phases"].append(assess_result)

    # Phase 2: SQL Migration
    def _sql():
        import importlib
        mod = importlib.import_module("run_sql_migration")
        importlib.reload(mod)
        mod.main()

    sql_result = _run_phase("sql_migration", _sql, profile=profile)
    results["phases"].append(sql_result)

    # Phase 3: Notebook Migration
    def _notebooks():
        import importlib
        mod = importlib.import_module("run_notebook_migration")
        importlib.reload(mod)
        mod.main()

    nb_result = _run_phase("notebook_migration", _notebooks, profile=profile)
    results["phases"].append(nb_result)

    # Throughput metrics
    total_duration = sum(p["duration_seconds"] for p in results["phases"])
    results["throughput"] = {
        "mappings_per_second": round(scale / max(total_duration, 0.001), 2),
        "total_duration_seconds": round(total_duration, 3),
    }

    # Peak memory
    if profile:
        peak = max(p.get("memory_peak_mb", 0) for p in results["phases"])
        results["peak_memory_mb"] = peak
        results["memory_under_4gb"] = peak < 4096

    results["completed"] = datetime.now(timezone.utc).isoformat()

    # Write results
    result_path = output_dir / f"benchmark_scale_{scale}.json"
    with open(result_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    # Write CSV summary
    csv_path = output_dir / "benchmark_summary.csv"
    write_header = not csv_path.exists()
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["scale", "total_seconds", "mappings_per_sec", "peak_memory_mb", "timestamp"])
        writer.writerow([
            scale,
            results["throughput"]["total_duration_seconds"],
            results["throughput"]["mappings_per_second"],
            results.get("peak_memory_mb", 0),
            results["completed"],
        ])

    return results


def generate_scalability_report(results_dir=None):
    """Generate benchmarks/SCALABILITY.md from benchmark results.

    Returns:
        str: Path to the generated report.
    """
    results_dir = Path(results_dir) if results_dir else OUTPUT_DIR

    json_files = sorted(results_dir.glob("benchmark_scale_*.json"))
    if not json_files:
        return None

    runs = []
    for f in json_files:
        with open(f, encoding="utf-8") as fh:
            runs.append(json.load(fh))

    lines = [
        "# Scalability Report",
        "",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "## Benchmark Results",
        "",
        "| Scale | Total (s) | Mappings/sec | Peak Memory (MB) | Status |",
        "|------:|----------:|-------------:|------------------:|--------|",
    ]

    for r in runs:
        tp = r.get("throughput", {})
        mem = r.get("peak_memory_mb", 0)
        ok = all(p.get("status") == "ok" for p in r.get("phases", []))
        lines.append(
            f"| {r['scale']} | {tp.get('total_duration_seconds', 0):.1f} | "
            f"{tp.get('mappings_per_second', 0):.1f} | {mem:.0f} | "
            f"{'Pass' if ok else 'Fail'} |"
        )

    lines.extend([
        "",
        "## Phase Breakdown (Latest Run)",
        "",
        "| Phase | Duration (s) | Memory Delta (MB) | Status |",
        "|-------|-------------:|------------------:|--------|",
    ])

    latest = runs[-1] if runs else {}
    for p in latest.get("phases", []):
        delta = p.get("memory_after_mb", 0) - p.get("memory_before_mb", 0)
        lines.append(
            f"| {p['phase']} | {p['duration_seconds']:.3f} | "
            f"{delta:+.1f} | {p['status']} |"
        )

    lines.extend([
        "",
        "## Recommendations",
        "",
        "- For 100+ mappings: use `--parallel-waves 4` for 3x+ speedup",
        "- For 500+ mappings: ensure 8GB+ RAM, consider streaming XML parser",
        "- For 1000+ mappings: use container deployment with 16GB RAM, SSD storage",
        "",
    ])

    report_path = WORKSPACE / "benchmarks" / "SCALABILITY.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return str(report_path)


def main():
    parser = argparse.ArgumentParser(description="Benchmark Harness")
    parser.add_argument("--scale", type=int, default=10, help="Number of mappings")
    parser.add_argument("--profile", action="store_true", help="Enable memory profiling")
    parser.add_argument("--report", action="store_true", help="Generate scalability report")
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    if args.report:
        report = generate_scalability_report(args.output_dir)
        if report:
            print(f"  Report generated: {report}")
        else:
            print("  No benchmark results found")
        return

    results = run_benchmark(args.scale, args.profile, args.output_dir)

    print(f"\n  Benchmark Results (scale={args.scale})")
    print(f"  ─────────────────────────────────")
    tp = results["throughput"]
    print(f"  Total duration: {tp['total_duration_seconds']:.1f}s")
    print(f"  Throughput:     {tp['mappings_per_second']:.1f} mappings/sec")
    if args.profile:
        print(f"  Peak memory:    {results.get('peak_memory_mb', 0):.0f} MB")
        print(f"  Under 4GB:      {'Yes' if results.get('memory_under_4gb') else 'No'}")
    print()


if __name__ == "__main__":
    main()
