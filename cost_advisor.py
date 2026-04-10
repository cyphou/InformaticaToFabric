"""
Phase 17 — Cost Optimization Advisor (Sprint 99)

Facade module that re-exports cost functions from ml_pipeline.py for backward
compatibility with the development plan's file naming.

All cost logic lives in ml_pipeline.py alongside ML pipeline generation.

Usage:
    python cost_advisor.py
    python cost_advisor.py --inventory path/to/inventory.json
"""

from ml_pipeline import (
    estimate_mapping_cost,
    generate_tco_comparison,
    generate_reserved_capacity_plan,
    detect_idle_resources,
    generate_cost_tags,
    generate_cost_dashboard_html,
)

__all__ = [
    "estimate_mapping_cost",
    "generate_tco_comparison",
    "generate_reserved_capacity_plan",
    "detect_idle_resources",
    "generate_cost_tags",
    "generate_cost_dashboard_html",
]


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Cost Optimization Advisor (Sprint 99)")
    parser.add_argument("--inventory", default=None, help="Path to inventory.json")
    args = parser.parse_args()

    tco = generate_tco_comparison(args.inventory)
    reserved = generate_reserved_capacity_plan(args.inventory)
    idle = detect_idle_resources(args.inventory)

    print("=" * 60)
    print("  Cost Optimization Advisor")
    print("=" * 60)
    print(f"\n💰 TCO Comparison:")
    print(f"   Fabric:      ${tco['fabric_total']:.2f}/day")
    print(f"   Databricks:  ${tco['databricks_total']:.2f}/day")
    print(f"   {tco['recommendation']}")
    print(f"\n📊 Reserved Capacity:")
    print(f"   Recommendation: {reserved['recommendation']}")
    print(f"   Pay-as-you-go: ${reserved['pay_as_you_go']['yearly']:.2f}/year")
    print(f"   1-Year Reserved: ${reserved['reserved_1yr']['yearly']:.2f}/year ({reserved['reserved_1yr']['savings_vs_paygo']} savings)")
    print(f"   3-Year Reserved: ${reserved['reserved_3yr']['yearly']:.2f}/year ({reserved['reserved_3yr']['savings_vs_paygo']} savings)")
    print(f"\n🔧 Optimization Potential: ~{idle['potential_savings_pct']}%")
    for r in idle["recommendations"]:
        print(f"   - {r['description']}")


if __name__ == "__main__":
    main()
