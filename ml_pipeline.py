"""
Phase 17 — ML Pipeline Template Generation (Sprint 98)

Detects feature engineering mappings and generates ML pipeline templates:
  - Databricks Feature Store notebooks
  - MLflow experiment tracking templates
  - Batch scoring pipeline jobs

Usage:
    python ml_pipeline.py
    python ml_pipeline.py --inventory path/to/inventory.json
"""

import json
import re
import textwrap
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output" / "ml"
INVENTORY_PATH = WORKSPACE / "output" / "inventory" / "inventory.json"


# ─────────────────────────────────────────────
#  Feature Engineering Detection (Sprint 98)
# ─────────────────────────────────────────────

FEATURE_PATTERNS = [
    r"FEAT_", r"FEATURE_", r"ML_", r"FE_",
    r"_FEATURES$", r"_FEATURE_STORE$",
]

SCORING_PATTERNS = [
    r"SCORE_", r"PREDICT_", r"MODEL_",
    r"_PREDICTIONS$", r"_SCORED$",
]


def detect_feature_mappings(inventory_path=None):
    """Detect mappings that produce feature tables for ML.

    Checks target table names against known feature/scoring patterns.
    Returns categorized list of ML-relevant mappings.
    """
    inv_path = Path(inventory_path or INVENTORY_PATH)
    if not inv_path.exists():
        return {"feature_engineering": [], "scoring": [], "summary": {
            "fe_count": 0, "scoring_count": 0, "total": 0,
        }}

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    fe_mappings = []
    scoring_mappings = []

    for mapping in inventory.get("mappings", []):
        targets = mapping.get("targets", [])
        name = mapping.get("name", "")
        all_names = targets + [name]

        is_fe = any(re.search(p, n, re.IGNORECASE) for n in all_names for p in FEATURE_PATTERNS)
        is_scoring = any(re.search(p, n, re.IGNORECASE) for n in all_names for p in SCORING_PATTERNS)

        # Also check transforms for aggregation/window patterns that suggest feature engineering
        transforms = mapping.get("transformations", [])
        has_agg = any(t.get("type") in ("Aggregator", "Expression", "Rank", "Sorter")
                      for t in transforms)

        if is_fe or (has_agg and len(transforms) >= 3):
            fe_mappings.append({
                "name": name,
                "sources": mapping.get("sources", []),
                "targets": targets,
                "complexity": mapping.get("complexity", "Simple"),
                "transforms": [t.get("type", "") for t in transforms],
                "detection_reason": "name_pattern" if is_fe else "aggregation_pattern",
            })
        elif is_scoring:
            scoring_mappings.append({
                "name": name,
                "sources": mapping.get("sources", []),
                "targets": targets,
                "complexity": mapping.get("complexity", "Simple"),
            })

    return {
        "feature_engineering": fe_mappings,
        "scoring": scoring_mappings,
        "summary": {
            "fe_count": len(fe_mappings),
            "scoring_count": len(scoring_mappings),
            "total": len(fe_mappings) + len(scoring_mappings),
        },
    }


# ─────────────────────────────────────────────
#  Feature Store Notebook Generator
# ─────────────────────────────────────────────

def generate_feature_store_notebook(mapping, catalog="main", schema="feature_store"):
    """Generate a Databricks Feature Store notebook.

    Creates:
    - Feature table definition
    - Feature computation PySpark
    - Feature Store write
    """
    name = mapping["name"]
    sources = mapping.get("sources", [])
    targets = mapping.get("targets", [])
    target_table = targets[0] if targets else f"feat_{name.lower()}"

    nb = textwrap.dedent(f"""\
    # Databricks notebook source
    # Feature Store — {name}
    # Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
    # Source: Informatica mapping {name}

    # COMMAND ----------

    from databricks.feature_engineering import FeatureEngineeringClient
    from pyspark.sql import functions as F

    fe = FeatureEngineeringClient()

    # COMMAND ----------

    # Read source data
    """)

    for src in sources:
        nb += f'df_{src.lower()} = spark.read.table("{catalog}.{schema}.{src.lower()}")\n'

    nb += textwrap.dedent(f"""
    # COMMAND ----------

    # Feature transformations
    features_df = df_{sources[0].lower() if sources else 'source'}
    # TODO: Add feature engineering transformations from mapping {name}
    #   Example: .withColumn("feature_1", F.col("col_a") / F.col("col_b"))

    # COMMAND ----------

    # Create or update feature table
    fe.create_table(
        name="{catalog}.{schema}.{target_table.lower()}",
        primary_keys=["id"],  # TODO: Set actual primary key
        df=features_df,
        description="Feature table from Informatica mapping {name}",
    )

    print("✅ Feature table {target_table.lower()} updated")
    """)

    return nb


# ─────────────────────────────────────────────
#  MLflow Experiment Template Generator
# ─────────────────────────────────────────────

def generate_mlflow_template(mapping, experiment_name=None):
    """Generate MLflow experiment tracking template."""
    name = mapping["name"]
    exp = experiment_name or f"/Workspace/experiments/{name}"

    return textwrap.dedent(f"""\
    # Databricks notebook source
    # MLflow Experiment — {name}
    # Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}

    # COMMAND ----------

    import mlflow
    import mlflow.spark
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.classification import RandomForestClassifier
    from pyspark.ml.evaluation import BinaryClassificationEvaluator

    mlflow.set_experiment("{exp}")

    # COMMAND ----------

    # Load feature data
    features_df = spark.read.table("main.feature_store.{name.lower()}")

    # Train/test split
    train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)

    # COMMAND ----------

    with mlflow.start_run(run_name="{name}_run") as run:
        # Log parameters
        mlflow.log_param("model_type", "RandomForest")
        mlflow.log_param("source_mapping", "{name}")
        mlflow.log_param("train_size", train_df.count())

        # Feature assembly
        feature_cols = [c for c in train_df.columns if c not in ["id", "label"]]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        train_assembled = assembler.transform(train_df)

        # Train model
        rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100)
        model = rf.fit(train_assembled)

        # Evaluate
        test_assembled = assembler.transform(test_df)
        predictions = model.transform(test_assembled)
        evaluator = BinaryClassificationEvaluator(labelCol="label")
        auc = evaluator.evaluate(predictions)

        # Log metrics
        mlflow.log_metric("auc", auc)
        mlflow.log_metric("test_size", test_df.count())

        # Log model
        mlflow.spark.log_model(model, "model")

        print(f"✅ Run {{run.info.run_id}} — AUC: {{auc:.4f}}")

    # COMMAND ----------

    # Register best model
    # mlflow.register_model(f"runs:/{{run.info.run_id}}/model", "{name}_model")
    """)


# ─────────────────────────────────────────────
#  Batch Scoring Pipeline Generator
# ─────────────────────────────────────────────

def generate_scoring_pipeline(mapping, model_name=None, catalog="main", schema="predictions"):
    """Generate a batch scoring pipeline notebook."""
    name = mapping["name"]
    sources = mapping.get("sources", [])
    m_name = model_name or f"{name}_model"

    return textwrap.dedent(f"""\
    # Databricks notebook source
    # Batch Scoring Pipeline — {name}
    # Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}

    # COMMAND ----------

    import mlflow
    from pyspark.sql import functions as F

    # COMMAND ----------

    # Load latest production model
    model_uri = "models:/{m_name}/Production"
    model = mlflow.spark.load_model(model_uri)

    # COMMAND ----------

    # Load input data
    input_df = spark.read.table("{catalog}.{schema}.{sources[0].lower() if sources else 'input_table'}")

    # COMMAND ----------

    # Score
    predictions_df = model.transform(input_df)

    # Add metadata
    predictions_df = (predictions_df
        .withColumn("scored_at", F.current_timestamp())
        .withColumn("model_name", F.lit("{m_name}"))
    )

    # COMMAND ----------

    # Write predictions to Delta
    predictions_df.write.format("delta").mode("overwrite").saveAsTable(
        "{catalog}.{schema}.{name.lower()}_predictions"
    )

    print("✅ Scored {{}} rows → {name.lower()}_predictions".format(predictions_df.count()))
    """)


# ─────────────────────────────────────────────
#  Cost Optimization Advisor (Sprint 99)
# ─────────────────────────────────────────────

# Cost rates (approximate public pricing)
FABRIC_CU_RATE_PER_HOUR = 0.36      # F2 capacity per hour
DATABRICKS_DBU_RATE_STANDARD = 0.22  # Standard compute DBU/hour
DATABRICKS_DBU_RATE_PREMIUM = 0.55   # Premium compute DBU/hour


def estimate_mapping_cost(mapping, target="fabric"):
    """Estimate compute cost for a single mapping execution.

    Heuristic based on complexity and estimated runtime.
    """
    complexity = mapping.get("complexity", "Simple")
    runtime_minutes = {"Simple": 5, "Medium": 15, "Complex": 45, "Custom": 60}.get(complexity, 10)
    cu_needed = {"Simple": 1, "Medium": 2, "Complex": 4, "Custom": 6}.get(complexity, 1)

    if target == "fabric":
        cost = (runtime_minutes / 60) * cu_needed * FABRIC_CU_RATE_PER_HOUR
    else:
        dbu_rate = DATABRICKS_DBU_RATE_STANDARD
        cost = (runtime_minutes / 60) * cu_needed * dbu_rate

    return {
        "mapping": mapping.get("name", ""),
        "complexity": complexity,
        "estimated_runtime_min": runtime_minutes,
        "compute_units": cu_needed,
        "cost_usd": round(cost, 4),
        "target": target,
    }


def generate_tco_comparison(inventory_path=None):
    """Generate TCO comparison between Fabric and Databricks.

    Returns per-mapping and total cost comparison.
    """
    inv_path = Path(inventory_path or INVENTORY_PATH)
    if not inv_path.exists():
        return {
            "fabric_total": 0, "databricks_total": 0, "savings_pct": 0,
            "recommendation": "No inventory available", "mappings": [],
        }

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    results = []
    fabric_total = 0
    db_total = 0

    for mapping in inventory.get("mappings", []):
        fabric_est = estimate_mapping_cost(mapping, "fabric")
        db_est = estimate_mapping_cost(mapping, "databricks")
        cheaper = "fabric" if fabric_est["cost_usd"] <= db_est["cost_usd"] else "databricks"

        results.append({
            "name": mapping["name"],
            "complexity": mapping.get("complexity", "Simple"),
            "fabric_cost": fabric_est["cost_usd"],
            "databricks_cost": db_est["cost_usd"],
            "cheaper": cheaper,
            "savings": round(abs(fabric_est["cost_usd"] - db_est["cost_usd"]), 4),
        })
        fabric_total += fabric_est["cost_usd"]
        db_total += db_est["cost_usd"]

    # Determine recommendation
    if fabric_total <= db_total:
        savings_pct = round((1 - fabric_total / db_total) * 100, 1) if db_total > 0 else 0
        recommendation = f"Fabric is {savings_pct}% cheaper overall"
    else:
        savings_pct = round((1 - db_total / fabric_total) * 100, 1) if fabric_total > 0 else 0
        recommendation = f"Databricks is {savings_pct}% cheaper overall"

    return {
        "fabric_total": round(fabric_total, 2),
        "databricks_total": round(db_total, 2),
        "savings_pct": savings_pct,
        "recommendation": recommendation,
        "mappings": results,
    }


def generate_reserved_capacity_plan(inventory_path=None):
    """Calculate break-even point for reserved capacity vs. pay-as-you-go.

    Compares 1-year and 3-year reserved pricing.
    """
    tco = generate_tco_comparison(inventory_path)
    daily_cost = tco["fabric_total"]  # assume 1 run/day

    paygo_monthly = daily_cost * 30
    paygo_yearly = daily_cost * 365

    # Reserved pricing discounts (approximate)
    reserved_1yr_discount = 0.35  # 35% discount
    reserved_3yr_discount = 0.55  # 55% discount

    reserved_1yr_monthly = paygo_monthly * (1 - reserved_1yr_discount)
    reserved_3yr_monthly = paygo_monthly * (1 - reserved_3yr_discount)

    return {
        "pay_as_you_go": {
            "monthly": round(paygo_monthly, 2),
            "yearly": round(paygo_yearly, 2),
        },
        "reserved_1yr": {
            "monthly": round(reserved_1yr_monthly, 2),
            "yearly": round(reserved_1yr_monthly * 12, 2),
            "savings_vs_paygo": f"{reserved_1yr_discount * 100:.0f}%",
        },
        "reserved_3yr": {
            "monthly": round(reserved_3yr_monthly, 2),
            "yearly": round(reserved_3yr_monthly * 12, 2),
            "savings_vs_paygo": f"{reserved_3yr_discount * 100:.0f}%",
        },
        "recommendation": (
            "3-year reserved capacity" if paygo_yearly > 1000
            else "1-year reserved capacity" if paygo_yearly > 200
            else "Pay-as-you-go (low volume)"
        ),
        "daily_runs": 1,
        "breakeven_months_1yr": round(1 / reserved_1yr_discount, 1) if reserved_1yr_discount else 0,
    }


def detect_idle_resources(inventory_path=None):
    """Detect potentially over-provisioned or idle resources.

    Returns optimization recommendations.
    """
    inv_path = Path(inventory_path or INVENTORY_PATH)
    recommendations = []

    if not inv_path.exists():
        return {"recommendations": recommendations, "potential_savings_pct": 0}

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    mappings = inventory.get("mappings", [])

    # Detect simple mappings that could use smaller compute
    simple_count = sum(1 for m in mappings if m.get("complexity") == "Simple")
    if simple_count > len(mappings) * 0.5:
        recommendations.append({
            "type": "right_size",
            "description": f"{simple_count} of {len(mappings)} mappings are Simple — consider smaller compute SKU",
            "potential_savings_pct": 20,
        })

    # Detect mappings with no downstream dependencies (potential cleanup)
    dag = inventory.get("dependency_dag", {})
    leaf_mappings = [m["name"] for m in mappings
                     if not dag.get(m["name"], {}).get("downstream")]
    if leaf_mappings and len(leaf_mappings) > len(mappings) * 0.3:
        recommendations.append({
            "type": "unused_tables",
            "description": f"{len(leaf_mappings)} mappings have no downstream consumers — review if still needed",
            "potential_savings_pct": 10,
            "mappings": leaf_mappings[:10],
        })

    total_savings = sum(r.get("potential_savings_pct", 0) for r in recommendations)
    return {
        "recommendations": recommendations,
        "potential_savings_pct": min(total_savings, 50),
    }


def generate_cost_tags(inventory_path=None, department="data_engineering",
                       project="informatica_migration"):
    """Generate cost allocation tags per mapping for cloud billing."""
    inv_path = Path(inventory_path or INVENTORY_PATH)
    if not inv_path.exists():
        return {"tags": [], "department": department, "project": project}

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    tags = []
    for mapping in inventory.get("mappings", []):
        tags.append({
            "resource": mapping["name"],
            "tags": {
                "department": department,
                "project": project,
                "cost_center": f"cc-{department}",
                "complexity": mapping.get("complexity", "Simple"),
                "migration_wave": mapping.get("wave", "1"),
                "source_system": "informatica",
            },
        })

    return {"tags": tags, "department": department, "project": project}


def generate_cost_dashboard_html(inventory_path=None):
    """Generate HTML cost dashboard."""
    import html as html_module

    tco = generate_tco_comparison(inventory_path)
    reserved = generate_reserved_capacity_plan(inventory_path)
    idle = detect_idle_resources(inventory_path)

    rows = ""
    for m in tco["mappings"]:
        winner = "✅" if m["cheaper"] == "fabric" else ""
        rows += (f'<tr><td>{html_module.escape(m["name"])}</td>'
                 f'<td>{m["complexity"]}</td>'
                 f'<td>{winner} ${m["fabric_cost"]:.4f}</td>'
                 f'<td>{"✅" if m["cheaper"] == "databricks" else ""} ${m["databricks_cost"]:.4f}</td>'
                 f'<td>${m["savings"]:.4f}</td></tr>')

    recs_html = ""
    for r in idle["recommendations"]:
        recs_html += f'<li><strong>{r["type"]}</strong>: {html_module.escape(r["description"])} (~{r["potential_savings_pct"]}% savings)</li>'

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Cost Dashboard</title>
<style>
body {{ font-family: -apple-system, sans-serif; margin: 20px; background: #f5f5f5; }}
.card {{ background: white; border-radius: 8px; padding: 20px; margin-bottom: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
.stats {{ display: flex; gap: 24px; }}
.stat {{ text-align: center; }}
.stat-value {{ font-size: 28px; font-weight: bold; }}
table {{ width: 100%; border-collapse: collapse; }}
th {{ background: #0078D4; color: white; padding: 10px; text-align: left; }}
td {{ padding: 8px; border-bottom: 1px solid #eee; }}
</style></head><body>
<h1>💰 Cost Optimization Dashboard</h1>
<div class="card"><h2>TCO Comparison</h2>
<div class="stats">
<div class="stat"><div class="stat-value" style="color:#0078D4">${tco['fabric_total']:.2f}</div><div>Fabric (daily)</div></div>
<div class="stat"><div class="stat-value" style="color:#FF3621">${tco['databricks_total']:.2f}</div><div>Databricks (daily)</div></div>
<div class="stat"><div class="stat-value" style="color:#27AE60">{tco['savings_pct']}%</div><div>Savings</div></div>
</div>
<p><strong>Recommendation:</strong> {html_module.escape(tco['recommendation'])}</p></div>
<div class="card"><h2>Reserved Capacity Plan</h2>
<table><tr><th>Plan</th><th>Monthly</th><th>Yearly</th><th>Savings</th></tr>
<tr><td>Pay-as-you-go</td><td>${reserved['pay_as_you_go']['monthly']:.2f}</td><td>${reserved['pay_as_you_go']['yearly']:.2f}</td><td>—</td></tr>
<tr><td>1-Year Reserved</td><td>${reserved['reserved_1yr']['monthly']:.2f}</td><td>${reserved['reserved_1yr']['yearly']:.2f}</td><td>{reserved['reserved_1yr']['savings_vs_paygo']}</td></tr>
<tr><td>3-Year Reserved</td><td>${reserved['reserved_3yr']['monthly']:.2f}</td><td>${reserved['reserved_3yr']['yearly']:.2f}</td><td>{reserved['reserved_3yr']['savings_vs_paygo']}</td></tr>
</table>
<p><strong>Recommendation:</strong> {html_module.escape(reserved['recommendation'])}</p></div>
<div class="card"><h2>Per-Mapping Cost</h2>
<table><tr><th>Mapping</th><th>Complexity</th><th>Fabric</th><th>Databricks</th><th>Savings</th></tr>
{rows}</table></div>
<div class="card"><h2>Optimization Recommendations</h2>
<ul>{recs_html if recs_html else '<li>No additional recommendations</li>'}</ul>
<p>Potential savings: ~{idle['potential_savings_pct']}%</p></div>
</body></html>"""


# ─────────────────────────────────────────────
#  Orchestrator
# ─────────────────────────────────────────────

def generate_ml_artifacts(inventory_path=None):
    """Generate all ML pipeline artifacts.

    Returns summary of generated files.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    generated = []

    detection = detect_feature_mappings(inventory_path)
    print(f"\n🔬 ML Pipeline Detection:")
    print(f"   Feature engineering: {detection['summary']['fe_count']}")
    print(f"   Scoring pipelines: {detection['summary']['scoring_count']}")

    # Feature Store notebooks
    for mapping in detection["feature_engineering"]:
        nb = generate_feature_store_notebook(mapping)
        path = OUTPUT_DIR / f"FS_{mapping['name']}.py"
        path.write_text(nb, encoding="utf-8")
        generated.append(path.name)

    # MLflow templates
    for mapping in detection["feature_engineering"]:
        nb = generate_mlflow_template(mapping)
        path = OUTPUT_DIR / f"MLFLOW_{mapping['name']}.py"
        path.write_text(nb, encoding="utf-8")
        generated.append(path.name)

    # Scoring pipelines
    for mapping in detection["scoring"]:
        nb = generate_scoring_pipeline(mapping)
        path = OUTPUT_DIR / f"SCORE_{mapping['name']}.py"
        path.write_text(nb, encoding="utf-8")
        generated.append(path.name)

    # Cost dashboard
    cost_html = generate_cost_dashboard_html(inventory_path)
    cost_path = OUTPUT_DIR / "cost_dashboard.html"
    cost_path.write_text(cost_html, encoding="utf-8")
    generated.append("cost_dashboard.html")

    # TCO report
    tco = generate_tco_comparison(inventory_path)
    tco_path = OUTPUT_DIR / "tco_comparison.json"
    with open(tco_path, "w", encoding="utf-8") as f:
        json.dump(tco, f, indent=2)
    generated.append("tco_comparison.json")

    # Detection report
    det_path = OUTPUT_DIR / "ml_detection.json"
    with open(det_path, "w", encoding="utf-8") as f:
        json.dump(detection, f, indent=2)
    generated.append("ml_detection.json")

    print(f"\n✅ Generated {len(generated)} ML artifacts → {OUTPUT_DIR}")
    return {"generated": generated, "detection": detection, "tco": tco}


def main():
    import argparse
    parser = argparse.ArgumentParser(description="ML Pipeline & Cost Advisor (Phase 17)")
    parser.add_argument("--inventory", default=None, help="Path to inventory.json")
    args = parser.parse_args()
    generate_ml_artifacts(args.inventory)


if __name__ == "__main__":
    main()
