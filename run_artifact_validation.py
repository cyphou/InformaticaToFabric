"""
Artifact Validation — validates generated migration outputs for structural correctness.

Three validators:
  1. Pipeline JSON schema validator  — ensures Fabric/Databricks pipeline JSON is structurally valid
  2. DBT SQL syntax validator        — ensures generated SQL models have valid SQL/Jinja syntax
  3. Notebook structure validator     — ensures generated PySpark notebooks have correct cell structure

Usage:
    python run_artifact_validation.py                    # validate all artifacts
    python run_artifact_validation.py --pipelines        # validate pipelines only
    python run_artifact_validation.py --dbt              # validate DBT models only
    python run_artifact_validation.py --notebooks        # validate notebooks only
"""

import json
import re
import sys
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output"


# ═══════════════════════════════════════════════════════════════════════════
#  1. Pipeline JSON Schema Validator
# ═══════════════════════════════════════════════════════════════════════════

# Fabric pipeline JSON schema (structural rules, not full ADF schema)
FABRIC_PIPELINE_SCHEMA = {
    "required_root": ["name", "properties"],
    "required_properties": ["activities"],
    "valid_activity_types": {
        "TridentNotebook", "IfCondition", "SetVariable", "ForEach",
        "Until", "Wait", "WebActivity", "Copy", "ExecutePipeline",
        "Lookup", "GetMetadata", "Filter", "Switch", "AppendVariable",
        "SparkJob", "Script", "SynapseNotebook", "Delete", "Fail",
    },
    "required_activity_fields": ["name", "type"],
    "required_notebook_fields": ["notebook"],
    "dependency_conditions": {"Succeeded", "Failed", "Completed", "Skipped"},
}

# Databricks workflow schema (Jobs API)
DATABRICKS_WORKFLOW_SCHEMA = {
    "required_root": ["name", "tasks"],
    "valid_task_types": {"notebook_task", "spark_jar_task", "spark_python_task",
                         "spark_submit_task", "pipeline_task", "python_wheel_task",
                         "sql_task", "dbt_task", "run_job_task", "condition_task"},
    "required_task_fields": ["task_key"],
}


def validate_fabric_pipeline(pipeline_json):
    """Validate a Fabric Data Pipeline JSON structure.

    Returns a list of issues found. Empty list = valid.
    """
    issues = []

    # Root-level checks
    for field in FABRIC_PIPELINE_SCHEMA["required_root"]:
        if field not in pipeline_json:
            issues.append(f"Missing required root field: '{field}'")

    if "properties" not in pipeline_json:
        return issues

    props = pipeline_json["properties"]
    for field in FABRIC_PIPELINE_SCHEMA["required_properties"]:
        if field not in props:
            issues.append(f"Missing required properties field: '{field}'")

    if "activities" not in props:
        return issues

    activities = props["activities"]
    if not isinstance(activities, list):
        issues.append("'activities' must be an array")
        return issues

    valid_types = FABRIC_PIPELINE_SCHEMA["valid_activity_types"]
    valid_dep_conds = FABRIC_PIPELINE_SCHEMA["dependency_conditions"]
    activity_names = set()

    for i, act in enumerate(activities):
        prefix = f"Activity [{i}]"

        # Required fields
        for field in FABRIC_PIPELINE_SCHEMA["required_activity_fields"]:
            if field not in act:
                issues.append(f"{prefix}: Missing required field '{field}'")

        name = act.get("name", f"<unnamed-{i}>")
        prefix = f"Activity '{name}'"

        # Duplicate name check
        if name in activity_names:
            issues.append(f"{prefix}: Duplicate activity name")
        activity_names.add(name)

        # Type validation
        act_type = act.get("type", "")
        if act_type and act_type not in valid_types:
            issues.append(f"{prefix}: Unknown activity type '{act_type}'")

        # Dependency validation
        deps = act.get("dependsOn", [])
        for dep in deps:
            dep_activity = dep.get("activity", "")
            if not dep_activity:
                issues.append(f"{prefix}: dependsOn entry missing 'activity' field")
            dep_conditions = dep.get("dependencyConditions", [])
            for cond in dep_conditions:
                if cond not in valid_dep_conds:
                    issues.append(f"{prefix}: Invalid dependency condition '{cond}'")

        # Notebook activity checks
        if act_type == "TridentNotebook":
            tp = act.get("typeProperties", {})
            notebook = tp.get("notebook", {})
            if not notebook.get("referenceName"):
                issues.append(f"{prefix}: TridentNotebook missing notebook.referenceName")

        # IfCondition checks
        if act_type == "IfCondition":
            tp = act.get("typeProperties", {})
            if "expression" not in tp:
                issues.append(f"{prefix}: IfCondition missing expression")
            # Recursively validate nested activities
            for branch_key in ("ifTrueActivities", "ifFalseActivities"):
                branch = tp.get(branch_key, [])
                for j, nested in enumerate(branch):
                    for field in FABRIC_PIPELINE_SCHEMA["required_activity_fields"]:
                        if field not in nested:
                            issues.append(
                                f"{prefix}.{branch_key}[{j}]: Missing '{field}'")

        # Policy validation
        policy = act.get("policy", {})
        if policy:
            timeout = policy.get("timeout", "")
            if timeout and not re.match(r"^\d+\.\d{2}:\d{2}:\d{2}$", timeout):
                issues.append(
                    f"{prefix}: Invalid timeout format '{timeout}' "
                    f"(expected 'D.HH:MM:SS')")
            retry = policy.get("retry")
            if retry is not None and (not isinstance(retry, int) or retry < 0):
                issues.append(f"{prefix}: Invalid retry value '{retry}'")

    # Circular dependency check
    dep_map = {}
    for act in activities:
        name = act.get("name", "")
        dep_map[name] = [d.get("activity", "") for d in act.get("dependsOn", [])]
    issues.extend(_check_circular_deps(dep_map))

    return issues


def validate_databricks_workflow(workflow_json):
    """Validate a Databricks Workflow (Jobs API) JSON structure.

    Returns a list of issues found. Empty list = valid.
    """
    issues = []

    for field in DATABRICKS_WORKFLOW_SCHEMA["required_root"]:
        if field not in workflow_json:
            issues.append(f"Missing required root field: '{field}'")

    tasks = workflow_json.get("tasks", [])
    if not isinstance(tasks, list):
        issues.append("'tasks' must be an array")
        return issues

    valid_task_types = DATABRICKS_WORKFLOW_SCHEMA["valid_task_types"]
    task_keys = set()

    for i, task in enumerate(tasks):
        prefix = f"Task [{i}]"

        for field in DATABRICKS_WORKFLOW_SCHEMA["required_task_fields"]:
            if field not in task:
                issues.append(f"{prefix}: Missing required field '{field}'")

        key = task.get("task_key", f"<unnamed-{i}>")
        prefix = f"Task '{key}'"

        if key in task_keys:
            issues.append(f"{prefix}: Duplicate task_key")
        task_keys.add(key)

        # Must have exactly one task type
        found_types = [t for t in valid_task_types if t in task]
        if len(found_types) == 0:
            issues.append(f"{prefix}: No task type specified (need one of {valid_task_types})")
        elif len(found_types) > 1:
            issues.append(f"{prefix}: Multiple task types: {found_types}")

        # Validate notebook_task
        if "notebook_task" in task:
            nt = task["notebook_task"]
            if not nt.get("notebook_path"):
                issues.append(f"{prefix}: notebook_task missing 'notebook_path'")

        # Validate dbt_task
        if "dbt_task" in task:
            dt = task["dbt_task"]
            if not dt.get("commands"):
                issues.append(f"{prefix}: dbt_task missing 'commands'")

        # Validate depends_on references
        deps = task.get("depends_on", [])
        for dep in deps:
            ref = dep.get("task_key", "")
            if not ref:
                issues.append(f"{prefix}: depends_on entry missing 'task_key'")

        # Max retries sanity
        max_retries = task.get("max_retries")
        if max_retries is not None and (not isinstance(max_retries, int) or max_retries < 0):
            issues.append(f"{prefix}: Invalid max_retries '{max_retries}'")

    # Check depends_on references exist
    for task in tasks:
        key = task.get("task_key", "")
        for dep in task.get("depends_on", []):
            ref = dep.get("task_key", "")
            if ref and ref not in task_keys:
                issues.append(f"Task '{key}': depends_on references unknown task '{ref}'")

    # Circular dependency check
    dep_map = {}
    for task in tasks:
        key = task.get("task_key", "")
        dep_map[key] = [d.get("task_key", "") for d in task.get("depends_on", [])]
    issues.extend(_check_circular_deps(dep_map))

    return issues


def _check_circular_deps(dep_map):
    """Detect circular dependencies in a dependency graph. Returns list of issues."""
    issues = []
    visited = set()
    rec_stack = set()

    def _dfs(node, path):
        if node in rec_stack:
            cycle = path[path.index(node):]
            issues.append(f"Circular dependency detected: {' → '.join(cycle + [node])}")
            return
        if node in visited:
            return
        visited.add(node)
        rec_stack.add(node)
        for dep in dep_map.get(node, []):
            if dep:
                _dfs(dep, path + [node])
        rec_stack.discard(node)

    for node in dep_map:
        if node not in visited:
            _dfs(node, [])
    return issues


# ═══════════════════════════════════════════════════════════════════════════
#  2. DBT SQL Syntax Validator
# ═══════════════════════════════════════════════════════════════════════════

# Jinja block patterns
_JINJA_BLOCK = re.compile(r"\{[{%#].*?[}%#]\}", re.DOTALL)

# SQL keywords that indicate a valid model
_SQL_REQUIRED_PATTERNS = [
    re.compile(r"\bSELECT\b", re.IGNORECASE),
]

# Common SQL syntax errors
_SQL_ERROR_PATTERNS = [
    (re.compile(r",\s*\bFROM\b", re.IGNORECASE), "Trailing comma before FROM"),
    (re.compile(r",\s*\bWHERE\b", re.IGNORECASE), "Trailing comma before WHERE"),
    (re.compile(r",\s*\bGROUP\s+BY\b", re.IGNORECASE), "Trailing comma before GROUP BY"),
    (re.compile(r",\s*\bORDER\s+BY\b", re.IGNORECASE), "Trailing comma before ORDER BY"),
    (re.compile(r",\s*\bHAVING\b", re.IGNORECASE), "Trailing comma before HAVING"),
    (re.compile(r",\s*\)"), "Trailing comma before closing parenthesis"),
]


def validate_dbt_model(sql_content, filename=""):
    """Validate a DBT SQL model file for common syntax issues.

    Returns a list of issues found. Empty list = valid.
    """
    issues = []
    prefix = f"[{filename}] " if filename else ""

    if not sql_content.strip():
        issues.append(f"{prefix}Empty model file")
        return issues

    # Strip Jinja blocks for SQL analysis
    sql_plain = _JINJA_BLOCK.sub(" __JINJA__ ", sql_content)

    # Check for required SQL patterns
    for pattern in _SQL_REQUIRED_PATTERNS:
        if not pattern.search(sql_plain):
            issues.append(f"{prefix}Missing SELECT statement")

    # Check for Jinja config block
    if "{{ config(" not in sql_content and "{%- config(" not in sql_content:
        # Config is optional but recommended
        pass  # Just a warning, not blocking

    # Check {{ ref() }} and {{ source() }} syntax
    ref_pattern = re.compile(r"\{\{\s*ref\(\s*'([^']+)'\s*\)\s*\}\}")
    source_pattern = re.compile(r"\{\{\s*source\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)\s*\}\}")

    # Check for malformed Jinja
    open_braces = sql_content.count("{{")
    close_braces = sql_content.count("}}")
    if open_braces != close_braces:
        issues.append(f"{prefix}Unmatched Jinja braces: {open_braces} '{{{{' vs {close_braces} '}}}}'")

    open_blocks = sql_content.count("{%")
    close_blocks = sql_content.count("%}")
    if open_blocks != close_blocks:
        issues.append(f"{prefix}Unmatched Jinja blocks: {open_blocks} '{{%' vs {close_blocks} '%}}'")

    # Check for common SQL errors in the stripped SQL
    for pattern, msg in _SQL_ERROR_PATTERNS:
        if pattern.search(sql_plain):
            issues.append(f"{prefix}{msg}")

    # Balanced parentheses (after stripping Jinja)
    paren_depth = 0
    for ch in sql_plain:
        if ch == "(":
            paren_depth += 1
        elif ch == ")":
            paren_depth -= 1
        if paren_depth < 0:
            issues.append(f"{prefix}Unmatched closing parenthesis")
            break
    if paren_depth > 0:
        issues.append(f"{prefix}Unmatched opening parenthesis ({paren_depth} unclosed)")

    # CTE validation: WITH ... AS (...) should have matching names
    cte_pattern = re.compile(r"\bWITH\b|\b(\w+)\s+AS\s*\(", re.IGNORECASE)
    cte_names = []
    for m in cte_pattern.finditer(sql_plain):
        if m.group(1):
            cte_names.append(m.group(1))

    # Check duplicate CTE names
    seen_ctes = set()
    for cte in cte_names:
        if cte.lower() in seen_ctes:
            issues.append(f"{prefix}Duplicate CTE name: '{cte}'")
        seen_ctes.add(cte.lower())

    return issues


def validate_dbt_yaml(yaml_content, filename=""):
    """Validate a DBT YAML file (schema.yml, sources.yml) for common issues.

    Returns a list of issues found. Empty list = valid.
    """
    issues = []
    prefix = f"[{filename}] " if filename else ""

    if not yaml_content.strip():
        issues.append(f"{prefix}Empty YAML file")
        return issues

    # Check for tab characters (YAML doesn't allow tabs)
    if "\t" in yaml_content:
        lines_with_tabs = [i + 1 for i, line in enumerate(yaml_content.split("\n"))
                          if "\t" in line]
        issues.append(f"{prefix}Tab characters found on lines: {lines_with_tabs[:5]}")

    # Check indentation consistency
    indents = set()
    for line in yaml_content.split("\n"):
        if line.strip() and line != line.lstrip():
            indent = len(line) - len(line.lstrip())
            indents.add(indent)
    if indents:
        # Check if all indents are multiples of the smallest
        min_indent = min(indents)
        if min_indent > 0:
            bad_indents = [i for i in indents if i % min_indent != 0]
            if bad_indents:
                issues.append(f"{prefix}Inconsistent YAML indentation: {sorted(indents)}")

    # Check for required top-level keys (schema.yml and sources.yml need version)
    basename = Path(filename).name if filename else ""
    if basename in ("schema.yml", "sources.yml") and "version:" not in yaml_content:
        issues.append(f"{prefix}Missing 'version:' key")

    return issues


# ═══════════════════════════════════════════════════════════════════════════
#  3. Notebook Structure Validator
# ═══════════════════════════════════════════════════════════════════════════

def validate_notebook(content, filename="", target="fabric"):
    """Validate a generated PySpark notebook structure.

    Returns a list of issues found. Empty list = valid.
    """
    issues = []
    prefix = f"[{filename}] " if filename else ""

    if not content.strip():
        issues.append(f"{prefix}Empty notebook file")
        return issues

    lines = content.strip().split("\n")

    # Header check
    if target == "fabric":
        if not lines[0].startswith("# Fabric notebook"):
            issues.append(f"{prefix}Missing Fabric notebook header")
    elif target == "databricks":
        if not lines[0].startswith("# Databricks notebook"):
            issues.append(f"{prefix}Missing Databricks notebook header")

    # Cell separator check
    cell_separator = "# COMMAND ----------"
    cell_count = content.count(cell_separator)
    if cell_count < 2:
        issues.append(f"{prefix}Too few cells ({cell_count} separators, expected ≥2)")

    # Import check
    if "from pyspark.sql" not in content:
        issues.append(f"{prefix}Missing PySpark imports")

    # Source read check
    if "spark.table(" not in content and "spark.read" not in content:
        issues.append(f"{prefix}Missing source read (spark.table or spark.read)")

    # Target write check
    write_patterns = [
        "write.mode(", "write.format(", ".save(", "merge(", ".insertInto(",
        "createOrReplaceTempView(",
    ]
    if not any(p in content for p in write_patterns):
        issues.append(f"{prefix}Missing target write operation")

    # Check for common Python syntax issues
    # Unclosed string literals
    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        if stripped.startswith("#") or not stripped:
            continue
        # Simple check: odd number of unescaped quotes
        single_quotes = stripped.count("'") - stripped.count("\\'")
        double_quotes = stripped.count('"') - stripped.count('\\"')
        # Skip multi-line strings
        if '"""' in stripped or "'''" in stripped:
            continue
        if single_quotes % 2 != 0 and "'''" not in stripped:
            # Could be multi-line, skip if ends with continuation
            if not stripped.endswith("\\"):
                pass  # Multi-line strings make this unreliable

    # Validate no hardcoded credentials
    cred_patterns = [
        re.compile(r"password\s*=\s*['\"][^'\"]+['\"]", re.IGNORECASE),
        re.compile(r"secret\s*=\s*['\"][^'\"]+['\"]", re.IGNORECASE),
        re.compile(r"token\s*=\s*['\"][A-Za-z0-9+/=]{20,}['\"]", re.IGNORECASE),
    ]
    for pat in cred_patterns:
        if pat.search(content):
            issues.append(f"{prefix}Possible hardcoded credential detected")

    return issues


# ═══════════════════════════════════════════════════════════════════════════
#  4. Batch Validation — scan output/ directory
# ═══════════════════════════════════════════════════════════════════════════

def validate_all_pipelines(output_dir=None):
    """Validate all pipeline JSON files in the output directory."""
    output_dir = output_dir or OUTPUT_DIR
    pipeline_dir = Path(output_dir) / "pipelines"
    results = {"valid": 0, "invalid": 0, "issues": {}}

    if not pipeline_dir.exists():
        return results

    for f in sorted(pipeline_dir.glob("PL_*.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            results["invalid"] += 1
            results["issues"][f.name] = [f"Invalid JSON: {e}"]
            continue

        # Detect Fabric vs Databricks format
        if "properties" in data:
            issues = validate_fabric_pipeline(data)
        elif "tasks" in data:
            issues = validate_databricks_workflow(data)
        else:
            issues = ["Unknown pipeline format (neither Fabric nor Databricks)"]

        if issues:
            results["invalid"] += 1
            results["issues"][f.name] = issues
        else:
            results["valid"] += 1

    return results


def validate_all_dbt_models(output_dir=None):
    """Validate all DBT SQL models in the output directory."""
    output_dir = output_dir or OUTPUT_DIR
    dbt_dir = Path(output_dir) / "dbt"
    results = {"valid": 0, "invalid": 0, "issues": {}}

    if not dbt_dir.exists():
        return results

    # SQL models (skip macros & snapshots — they use Jinja functions, not plain SQL)
    for f in sorted(dbt_dir.rglob("*.sql")):
        content = f.read_text(encoding="utf-8")
        rel_path = str(f.relative_to(dbt_dir))
        # Macros are Jinja functions, not SQL models
        if rel_path.replace("\\", "/").startswith("macros/"):
            results["valid"] += 1
            continue
        issues = validate_dbt_model(content, rel_path)
        if issues:
            results["invalid"] += 1
            results["issues"][rel_path] = issues
        else:
            results["valid"] += 1

    # YAML files
    for f in sorted(dbt_dir.rglob("*.yml")):
        content = f.read_text(encoding="utf-8")
        rel_path = str(f.relative_to(dbt_dir))
        issues = validate_dbt_yaml(content, rel_path)
        if issues:
            results["invalid"] += 1
            results["issues"][rel_path] = issues
        else:
            results["valid"] += 1

    return results


def validate_all_notebooks(output_dir=None, target="fabric"):
    """Validate all notebook files in the output directory."""
    output_dir = output_dir or OUTPUT_DIR
    nb_dir = Path(output_dir) / "notebooks"
    results = {"valid": 0, "invalid": 0, "issues": {}}

    if not nb_dir.exists():
        return results

    for f in sorted(nb_dir.glob("NB_*.py")):
        content = f.read_text(encoding="utf-8")
        issues = validate_notebook(content, f.name, target)
        if issues:
            results["invalid"] += 1
            results["issues"][f.name] = issues
        else:
            results["valid"] += 1

    return results


def validate_all(output_dir=None, target="fabric"):
    """Run all validators and return a combined report."""
    report = {
        "pipelines": validate_all_pipelines(output_dir),
        "dbt_models": validate_all_dbt_models(output_dir),
        "notebooks": validate_all_notebooks(output_dir, target),
    }

    total_valid = sum(r["valid"] for r in report.values())
    total_invalid = sum(r["invalid"] for r in report.values())
    report["summary"] = {
        "total_valid": total_valid,
        "total_invalid": total_invalid,
        "total_artifacts": total_valid + total_invalid,
        "pass_rate": round(total_valid / max(total_valid + total_invalid, 1) * 100, 1),
    }
    return report


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════

def main():
    """Run artifact validation and print results."""
    args = set(sys.argv[1:])
    run_all = not args or "--all" in args

    target = "fabric"
    if "--databricks" in args:
        target = "databricks"

    print("=" * 60)
    print("  Artifact Validation Report")
    print("=" * 60)

    if run_all or "--pipelines" in args:
        result = validate_all_pipelines()
        print(f"\n📦 Pipelines: {result['valid']} valid, {result['invalid']} invalid")
        for name, issues in result["issues"].items():
            for issue in issues:
                print(f"   ❌ {name}: {issue}")

    if run_all or "--dbt" in args:
        result = validate_all_dbt_models()
        print(f"\n🏗️  DBT Models: {result['valid']} valid, {result['invalid']} invalid")
        for name, issues in result["issues"].items():
            for issue in issues:
                print(f"   ❌ {name}: {issue}")

    if run_all or "--notebooks" in args:
        result = validate_all_notebooks(target=target)
        print(f"\n📓 Notebooks: {result['valid']} valid, {result['invalid']} invalid")
        for name, issues in result["issues"].items():
            for issue in issues:
                print(f"   ❌ {name}: {issue}")

    if run_all:
        report = validate_all(target=target)
        print(f"\n{'=' * 60}")
        s = report["summary"]
        print(f"  Total: {s['total_artifacts']} artifacts, {s['total_valid']} valid, "
              f"{s['total_invalid']} invalid ({s['pass_rate']}% pass rate)")
        print("=" * 60)


if __name__ == "__main__":
    main()
