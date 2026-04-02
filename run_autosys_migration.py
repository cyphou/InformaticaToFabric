"""
AutoSys JIL Migration — Sprint 61 (Phase 5)
Parses AutoSys JIL (Job Information Language) files,
builds a dependency graph, converts schedules to cron,
links jobs to Informatica workflows (via pmcmd commands),
and generates Fabric Data Pipeline / Databricks Workflow definitions.

Flow:
  1. Parse JIL files → structured job list
  2. Classify jobs (BOX, CMD, FW, FT)
  3. Build dependency DAG from `condition` attributes
  4. Resolve schedules → cron expressions
  5. Link pmcmd commands → Informatica workflow names
  6. Generate pipeline/workflow JSON for each BOX / standalone job chain
  7. Write output to output/autosys/

Usage:
    python run_autosys_migration.py
    python run_autosys_migration.py path/to/autosys_dir
"""

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
INPUT_DIR = WORKSPACE / "input" / "autosys"
OUTPUT_DIR = WORKSPACE / "output" / "autosys"
INVENTORY_PATH = WORKSPACE / "output" / "inventory" / "inventory.json"


# ─────────────────────────────────────────────
#  JIL Parser
# ─────────────────────────────────────────────

# Attributes that are multi-word strings (keep as-is, don't split)
_STRING_ATTRS = {
    "command", "description", "notification_msg", "watch_file",
    "std_out_file", "std_err_file", "profile", "machine", "owner",
}

# Days-of-week mapping: AutoSys → cron
_DOW_MAP = {
    "su": "0", "mo": "1", "tu": "2", "we": "3",
    "th": "4", "fr": "5", "sa": "6", "all": "*",
}


def parse_jil(text):
    """Parse JIL text into a list of job dicts and a list of calendar dicts.

    Returns (jobs, calendars) where each job is a dict of attributes
    and each calendar is {"name": ..., "description": ...}.
    """
    jobs = []
    calendars = []
    current = None
    in_block_comment = False

    for raw_line in text.splitlines():
        # Handle multi-line block comments (/* ... */)
        if in_block_comment:
            if "*/" in raw_line:
                # End of block comment — take text after */
                raw_line = raw_line[raw_line.index("*/") + 2:]
                in_block_comment = False
            else:
                continue

        # Strip inline comments (/* ... */ on a single line)
        line = re.sub(r'/\*.*?\*/', '', raw_line).strip()

        # Check if a standalone block comment opens (not inside a path/glob).
        # Only treat /* as a comment start when preceded by whitespace or at line start.
        comment_match = re.search(r'(?:^|(?<=\s))/\*', line)
        if comment_match:
            line = line[:comment_match.start()].strip()
            in_block_comment = True

        if not line:
            continue

        # Calendar definition
        cal_match = re.match(r'^insert_calendar:\s*(.+)', line, re.IGNORECASE)
        if cal_match:
            if current:
                jobs.append(current)
                current = None
            calendars.append({"name": cal_match.group(1).strip(), "description": ""})
            continue

        # Job definition start
        job_match = re.match(r'^insert_job:\s*(.+)', line, re.IGNORECASE)
        if job_match:
            if current:
                jobs.append(current)
            current = {"name": job_match.group(1).strip()}
            continue

        # Calendar attribute
        if calendars and not current:
            attr_match = re.match(r'^(\w+):\s*(.*)', line)
            if attr_match:
                calendars[-1][attr_match.group(1)] = attr_match.group(2).strip().strip('"')
            continue

        # Job attributes
        if current:
            attr_match = re.match(r'^(\w+):\s*(.*)', line)
            if attr_match:
                key = attr_match.group(1)
                value = attr_match.group(2).strip().strip('"')
                current[key] = value

    if current:
        jobs.append(current)

    return jobs, calendars


def classify_job(job):
    """Classify a job into CMD, BOX, FW, or FT."""
    jt = job.get("job_type", "CMD").upper()
    if jt == "BOX":
        return "BOX"
    elif jt in ("FW",):
        return "FW"
    elif jt in ("FT",):
        return "FT"
    return "CMD"


def parse_condition(condition_str):
    """Parse an AutoSys condition string into structured dependencies.

    Handles: s(job), f(job), n(job), d(job), AND/OR operators.
    Returns a list of {"job": name, "status": "success|failure|notrunning|done"}.
    """
    if not condition_str:
        return []

    status_map = {"s": "success", "f": "failure", "n": "notrunning", "d": "done"}
    deps = []
    for match in re.finditer(r'([sfnd])\(([^)]+)\)', condition_str, re.IGNORECASE):
        status_code = match.group(1).lower()
        job_name = match.group(2).strip()
        deps.append({
            "job": job_name,
            "status": status_map.get(status_code, "success"),
        })

    return deps


def build_dependency_graph(jobs):
    """Build a dependency graph from parsed jobs.

    Returns dict: {job_name: [{"job": dep_name, "status": ...}, ...]}.
    Also adds implicit box membership as a dependency.
    """
    graph = {}
    for job in jobs:
        name = job["name"]
        deps = parse_condition(job.get("condition", ""))

        # If job is in a box, add implicit dependency on box
        box = job.get("box_name")
        if box:
            # Don't add box as explicit dependency — it's a container
            pass

        graph[name] = deps

    return graph


def resolve_schedule(job):
    """Convert AutoSys schedule attributes to a cron expression.

    Handles: days_of_week, start_times, start_mins, date_conditions.
    Returns dict with {cron, note, original} or {} if no schedule.
    """
    if not job.get("date_conditions") and not job.get("start_times"):
        return {}

    # Days of week
    dow_raw = job.get("days_of_week", "all").lower()
    if dow_raw == "all":
        dow = "*"
    else:
        parts = [d.strip() for d in dow_raw.split(",")]
        dow = ",".join(_DOW_MAP.get(d, d) for d in parts)

    # Start times (HH:MM)
    start_times = job.get("start_times", "00:00").strip('"').strip()
    time_match = re.match(r'(\d{1,2}):(\d{2})', start_times)
    if time_match:
        hour = time_match.group(1)
        minute = time_match.group(2)
    else:
        hour, minute = "0", "0"

    # Start mins (repeating within the hour)
    start_mins = job.get("start_mins", "")
    if start_mins:
        minute = start_mins.replace(" ", "")

    # Build cron: minute hour day-of-month month day-of-week
    cron = f"{minute} {hour} * * {dow}"

    # Calendar reference
    calendar = job.get("run_calendar", "")
    note = f"AutoSys schedule: {start_times} {dow_raw}"
    if calendar:
        note += f" (calendar: {calendar})"

    return {
        "cron": cron,
        "note": note,
        "original": {
            "days_of_week": dow_raw,
            "start_times": start_times,
            "start_mins": start_mins,
            "run_calendar": calendar,
        }
    }


# ─────────────────────────────────────────────
#  Informatica Linkage
# ─────────────────────────────────────────────

_PMCMD_PATTERN = re.compile(
    r'pmcmd\s+startworkflow\b.*?-w\s+(\S+)', re.IGNORECASE
)

_PMCMD_FOLDER_PATTERN = re.compile(
    r'pmcmd\s+startworkflow\b.*?-f\s+(\S+)', re.IGNORECASE
)


def extract_informatica_workflow(command):
    """Extract Informatica workflow name from a pmcmd command.

    Returns {"workflow": name, "folder": folder_or_None} or None.
    """
    if not command:
        return None

    wf_match = _PMCMD_PATTERN.search(command)
    if not wf_match:
        return None

    workflow = wf_match.group(1)
    # Strip any trailing flags that got captured
    workflow = re.sub(r'\s+-.*', '', workflow)

    folder_match = _PMCMD_FOLDER_PATTERN.search(command)
    folder = folder_match.group(1) if folder_match else None

    return {"workflow": workflow, "folder": folder}


def link_jobs_to_workflows(jobs, inventory_workflows=None):
    """Link AutoSys CMD jobs to Informatica workflows.

    Returns dict: {job_name: {"workflow": wf_name, "folder": ...}}.
    """
    wf_names = set()
    if inventory_workflows:
        wf_names = {wf["name"] for wf in inventory_workflows}

    linkage = {}
    for job in jobs:
        if classify_job(job) != "CMD":
            continue
        info = extract_informatica_workflow(job.get("command", ""))
        if info:
            info["linked"] = info["workflow"] in wf_names if wf_names else None
            linkage[job["name"]] = info

    return linkage


# ─────────────────────────────────────────────
#  Pipeline / Workflow Generation
# ─────────────────────────────────────────────

def _get_target():
    """Return the target platform ('fabric' or 'databricks')."""
    return os.environ.get("INFORMATICA_MIGRATION_TARGET", "fabric")


def _condition_to_depends_on(deps, target="fabric"):
    """Convert parsed conditions to dependsOn / depends_on structure."""
    depends = []
    for dep in deps:
        status = dep["status"]
        if target == "databricks":
            # Databricks: task dependencies
            depends.append({"task_key": dep["job"], "outcome": status})
        else:
            # Fabric: dependsOn with conditions
            condition_map = {
                "success": "Succeeded",
                "failure": "Failed",
                "done": "Completed",
                "notrunning": "Skipped",
            }
            depends.append({
                "activity": dep["job"],
                "dependencyConditions": [condition_map.get(status, "Succeeded")]
            })
    return depends


def generate_fabric_pipeline(box_name, child_jobs, dep_graph, linkage, schedule_info):
    """Generate a Fabric Data Pipeline JSON for a BOX (or standalone chain)."""
    activities = []
    for job in child_jobs:
        name = job["name"]
        jtype = classify_job(job)
        deps = dep_graph.get(name, [])
        depends_on = _condition_to_depends_on(deps, "fabric")

        wf_link = linkage.get(name)
        description = job.get("description", "")

        if jtype == "FW":
            # File watcher → GetMetadata + Until loop annotation
            activities.append({
                "name": name,
                "type": "GetMetadata",
                "dependsOn": depends_on,
                "description": f"AutoSys FW: {description}. Watch: {job.get('watch_file', '')}",
                "typeProperties": {
                    "fieldList": ["exists", "lastModified"],
                    "dataset": {"type": "DatasetReference", "referenceName": "DS_" + name},
                },
                "annotations": [f"AutoSysFileWatcher:{job.get('watch_file', '')}"],
            })
        elif wf_link:
            # pmcmd → Notebook activity
            nb_name = f"NB_{wf_link['workflow']}"
            activities.append({
                "name": name,
                "type": "Notebook",
                "dependsOn": depends_on,
                "description": f"AutoSys CMD → Informatica {wf_link['workflow']}. {description}",
                "typeProperties": {
                    "notebook": {"referenceName": nb_name, "type": "NotebookReference"},
                    "parameters": {}
                },
                "annotations": [
                    f"AutoSysJob:{name}",
                    f"InformaticaWorkflow:{wf_link['workflow']}",
                ],
            })
        else:
            # Generic command → Script activity
            activities.append({
                "name": name,
                "type": "Script",
                "dependsOn": depends_on,
                "description": f"AutoSys CMD: {description}. Command: {job.get('command', '')}",
                "typeProperties": {
                    "content": f"-- TODO: Convert AutoSys command to Fabric activity\n-- Original: {job.get('command', '')}",
                },
                "annotations": [f"AutoSysJob:{name}", "ManualReviewRequired"],
            })

        # Add alarm annotation
        if job.get("alarm_if_fail") == "1":
            activities[-1].setdefault("annotations", []).append("AlarmIfFail")

    # Build pipeline
    annotations = [
        "MigratedFromAutoSys",
        f"OriginalBox:{box_name}",
    ]

    pipeline = {
        "name": f"PL_AUTOSYS_{box_name}",
        "properties": {
            "description": f"Migrated from AutoSys BOX/chain {box_name}.",
            "activities": activities,
            "parameters": {
                "load_date": {"type": "string", "defaultValue": "@utcnow('yyyy-MM-dd')"},
            },
            "variables": {},
            "annotations": annotations,
        }
    }

    # Schedule trigger
    if schedule_info and schedule_info.get("cron"):
        pipeline["properties"]["trigger"] = {
            "type": "ScheduleTrigger",
            "description": schedule_info.get("note", ""),
            "typeProperties": {
                "recurrence": {
                    "cron": schedule_info["cron"],
                    "timeZone": "UTC",
                }
            }
        }

    return pipeline


def generate_databricks_workflow(box_name, child_jobs, dep_graph, linkage, schedule_info):
    """Generate a Databricks Workflow (Jobs API) JSON for a BOX or job chain."""
    catalog = os.environ.get("INFORMATICA_DATABRICKS_CATALOG", "main")
    schema = "default"

    tasks = []
    for job in child_jobs:
        name = job["name"]
        jtype = classify_job(job)
        deps = dep_graph.get(name, [])
        depends_on = [{"task_key": d["job"]} for d in deps]

        wf_link = linkage.get(name)
        description = job.get("description", "")

        if wf_link:
            # pmcmd → notebook_task
            nb_path = f"/Shared/migration/NB_{wf_link['workflow']}"
            task = {
                "task_key": name,
                "description": f"AutoSys → Informatica {wf_link['workflow']}",
                "depends_on": depends_on,
                "notebook_task": {
                    "notebook_path": nb_path,
                    "source": "WORKSPACE",
                },
            }
        elif jtype == "FW":
            # File watcher → TODO annotation
            task = {
                "task_key": name,
                "description": f"AutoSys FW → file sensor. Watch: {job.get('watch_file', '')}. TODO: Use Databricks file arrival trigger.",
                "depends_on": depends_on,
                "notebook_task": {
                    "notebook_path": f"/Shared/migration/FW_{name}",
                    "source": "WORKSPACE",
                },
            }
        else:
            # Generic command
            task = {
                "task_key": name,
                "description": f"AutoSys CMD: {description}. TODO: Convert command.",
                "depends_on": depends_on,
                "notebook_task": {
                    "notebook_path": f"/Shared/migration/SCRIPT_{name}",
                    "source": "WORKSPACE",
                },
            }

        tasks.append(task)

    workflow = {
        "name": f"AUTOSYS_{box_name}",
        "tasks": tasks,
        "tags": {"source": "autosys", "migrated": "true"},
    }

    # Schedule
    if schedule_info and schedule_info.get("cron"):
        workflow["schedule"] = {
            "quartz_cron_expression": _cron_to_quartz(schedule_info["cron"]),
            "timezone_id": "UTC",
        }

    return workflow


def _cron_to_quartz(cron_5):
    """Convert 5-field cron to Quartz 7-field (seconds year added)."""
    parts = cron_5.strip().split()
    if len(parts) == 5:
        minute, hour, dom, month, dow = parts
        # Quartz: sec min hour dom month dow year
        # If dow is set and dom is *, use ? for dom
        if dow != "*":
            return f"0 {minute} {hour} ? {month} {dow} *"
        return f"0 {minute} {hour} {dom} {month} ? *"
    return f"0 {cron_5} *"


# ─────────────────────────────────────────────
#  Orchestration
# ─────────────────────────────────────────────

def group_jobs_by_box(jobs):
    """Group CMD/FW jobs under their parent BOX.

    Returns dict: {box_name: [child_jobs]}, plus
    a list of standalone jobs (no box_name).
    """
    boxes = {}
    standalone = []
    box_jobs = {}

    for job in jobs:
        jtype = classify_job(job)
        if jtype == "BOX":
            boxes[job["name"]] = job
            box_jobs.setdefault(job["name"], [])
        elif job.get("box_name"):
            box_jobs.setdefault(job["box_name"], []).append(job)
        else:
            standalone.append(job)

    return boxes, box_jobs, standalone


def build_standalone_chains(standalone, dep_graph):
    """Group standalone jobs into chains based on dependencies.

    Returns list of chains, where each chain is a list of jobs.
    """
    # Build adjacency: who depends on whom
    job_map = {j["name"]: j for j in standalone}
    chains = []
    visited = set()

    def _walk(name, chain):
        if name in visited or name not in job_map:
            return
        visited.add(name)
        chain.append(job_map[name])
        # Find jobs that depend on this one
        for other in standalone:
            deps = dep_graph.get(other["name"], [])
            if any(d["job"] == name for d in deps) and other["name"] not in visited:
                _walk(other["name"], chain)

    # Start from root jobs (no dependencies pointing to other standalone jobs)
    standalone_names = {j["name"] for j in standalone}
    for job in standalone:
        deps = dep_graph.get(job["name"], [])
        is_root = not any(d["job"] in standalone_names for d in deps)
        if is_root and job["name"] not in visited:
            chain = []
            _walk(job["name"], chain)
            if chain:
                chains.append(chain)

    # Add any orphans
    for job in standalone:
        if job["name"] not in visited:
            chains.append([job])
            visited.add(job["name"])

    return chains


def write_autosys_output(jobs, calendars, dep_graph, linkage, target, output_dir=None):
    """Generate pipeline/workflow JSON and summary for all parsed AutoSys jobs.

    Returns summary dict.
    """
    out = Path(output_dir) if output_dir else OUTPUT_DIR
    out.mkdir(parents=True, exist_ok=True)

    boxes, box_jobs, standalone = group_jobs_by_box(jobs)
    chains = build_standalone_chains(standalone, dep_graph)

    generated = []

    # Generate for each BOX
    for box_name, box_job in boxes.items():
        children = box_jobs.get(box_name, [])
        schedule = resolve_schedule(box_job)

        if target == "databricks":
            pipeline = generate_databricks_workflow(box_name, children, dep_graph, linkage, schedule)
        else:
            pipeline = generate_fabric_pipeline(box_name, children, dep_graph, linkage, schedule)

        filename = f"PL_AUTOSYS_{box_name}.json"
        with open(out / filename, "w", encoding="utf-8") as f:
            json.dump(pipeline, f, indent=2, ensure_ascii=False)

        generated.append({
            "name": box_name,
            "type": "BOX",
            "jobs": len(children),
            "file": filename,
            "has_schedule": bool(schedule),
        })
        print(f"  ✅ {filename} ({len(children)} activities)")

    # Generate for standalone chains
    for chain in chains:
        chain_name = chain[0]["name"]
        schedule = resolve_schedule(chain[0])

        if target == "databricks":
            pipeline = generate_databricks_workflow(chain_name, chain, dep_graph, linkage, schedule)
        else:
            pipeline = generate_fabric_pipeline(chain_name, chain, dep_graph, linkage, schedule)

        filename = f"PL_AUTOSYS_{chain_name}.json"
        with open(out / filename, "w", encoding="utf-8") as f:
            json.dump(pipeline, f, indent=2, ensure_ascii=False)

        generated.append({
            "name": chain_name,
            "type": "CHAIN",
            "jobs": len(chain),
            "file": filename,
            "has_schedule": bool(schedule),
        })
        print(f"  ✅ {filename} ({len(chain)} activities, standalone chain)")

    # Linkage summary
    total_linked = sum(1 for v in linkage.values() if v.get("linked"))
    total_pmcmd = len(linkage)
    unlinked = [k for k, v in linkage.items() if v.get("linked") is False]

    # Write summary
    summary = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "target": target,
        "total_jil_jobs": len(jobs),
        "job_types": {
            "BOX": len(boxes),
            "CMD": sum(1 for j in jobs if classify_job(j) == "CMD"),
            "FW": sum(1 for j in jobs if classify_job(j) == "FW"),
            "FT": sum(1 for j in jobs if classify_job(j) == "FT"),
        },
        "calendars": len(calendars),
        "pipelines_generated": len(generated),
        "informatica_linkage": {
            "pmcmd_jobs": total_pmcmd,
            "linked_to_inventory": total_linked,
            "unlinked": unlinked,
        },
        "pipelines": generated,
    }

    with open(out / "autosys_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    return summary


# ─────────────────────────────────────────────
#  Sprint 62 — Enhanced Condition Conversion
# ─────────────────────────────────────────────

def convert_condition_to_expression(condition_str):
    """Convert AutoSys condition string to pipeline expression (Sprint 62).

    Handles: s(job), f(job), n(job), d(job), AND, OR, NOT
    Returns Fabric expression or Databricks condition."""
    if not condition_str:
        return ""
    expr = condition_str
    # Replace condition functions with expression syntax
    expr = re.sub(r's\(([^)]+)\)', r"@equals(activity('\1').status, 'Succeeded')", expr)
    expr = re.sub(r'f\(([^)]+)\)', r"@equals(activity('\1').status, 'Failed')", expr)
    expr = re.sub(r'n\(([^)]+)\)', r"@not(equals(activity('\1').status, 'InProgress'))", expr)
    expr = re.sub(r'd\(([^)]+)\)', r"@equals(activity('\1').status, 'Skipped')", expr)
    expr = expr.replace(" AND ", " && ").replace(" OR ", " || ").replace("NOT ", "!")
    return expr


def generate_alarm_activity(job, target="fabric"):
    """Generate an alarm/notification activity from AutoSys alarm_if_fail (Sprint 62)."""
    name = job["name"]
    alarm = job.get("alarm_if_fail", "0")
    notification = job.get("send_notification", "0")

    if alarm != "1" and notification != "1":
        return None

    if target == "databricks":
        return {
            "task_key": f"alert_{name}",
            "depends_on": [{"task_key": name, "outcome": "FAILED"}],
            "notification_settings": {
                "no_alert_for_skipped_runs": True,
                "no_alert_for_canceled_runs": True,
                "alert_on_last_attempt": True,
            },
        }
    else:
        return {
            "name": f"Alert_{name}",
            "type": "WebActivity",
            "dependsOn": [{"activity": name, "dependencyConditions": ["Failed"]}],
            "typeProperties": {
                "url": "@pipeline().parameters.alert_webhook_url",
                "method": "POST",
                "body": json.dumps({
                    "title": f"AutoSys Job Failed: {name}",
                    "message": f"Job {name} has failed. Original AutoSys alarm.",
                }),
            },
        }


# ─────────────────────────────────────────────
#  Sprint 63 — Calendar Parsing & Profile Mapping
# ─────────────────────────────────────────────

def parse_calendar_definition(calendar_dict):
    """Parse an AutoSys calendar definition → structured schedule (Sprint 63).

    calendar_dict has: name, days (list of dates or patterns)
    Returns: {'name': str, 'type': str, 'dates': list, 'exclusions': list}
    """
    name = calendar_dict.get("name", "unknown")
    days = calendar_dict.get("days", [])

    # Classify calendar type
    if any("BUSINESS" in str(d).upper() for d in days):
        cal_type = "business_days"
    elif any("HOLIDAY" in str(d).upper() for d in days):
        cal_type = "holiday_exclusion"
    else:
        cal_type = "custom"

    return {
        "name": name,
        "type": cal_type,
        "dates": days,
        "exclusions": [],
        "note": f"AutoSys calendar '{name}' — review for business day / holiday logic",
    }


def map_machine_to_cluster(machine, profile=None):
    """Map AutoSys machine/profile to Databricks cluster annotation (Sprint 63).

    Returns: cluster config recommendation dict
    """
    machine = machine or ""
    profile = profile or ""

    # Common machine patterns
    if "gpu" in machine.lower() or "ml" in profile.lower():
        return {
            "cluster_type": "gpu",
            "node_type": "Standard_NC6s_v3",
            "note": f"GPU cluster recommended — original machine: {machine}",
        }
    elif "hpc" in machine.lower() or "large" in profile.lower():
        return {
            "cluster_type": "memory_optimized",
            "node_type": "Standard_E8s_v3",
            "note": f"Memory-optimized cluster — original machine: {machine}",
        }
    else:
        return {
            "cluster_type": "standard",
            "node_type": "Standard_DS4_v2",
            "note": f"Standard cluster — original machine: {machine}",
        }


def extract_global_variables(jobs):
    """Extract global variables from AutoSys jobs → pipeline parameters (Sprint 63).

    Looks for: std_out_file, std_err_file paths with ${...} patterns,
    environment variables in commands.
    """
    variables = {}
    env_pattern = re.compile(r'\$\{([A-Z_][A-Z0-9_]*)\}')

    for job in jobs:
        command = job.get("command", "")
        for match in env_pattern.finditer(command):
            var_name = match.group(1)
            variables[var_name] = {
                "source": "command",
                "job": job["name"],
                "default": f"${{{{pipeline().parameters.{var_name}}}}}",
            }

        # Check std_out_file/std_err_file
        for key in ("std_out_file", "std_err_file"):
            path = job.get(key, "")
            for match in env_pattern.finditer(path):
                var_name = match.group(1)
                variables[var_name] = {
                    "source": key,
                    "job": job["name"],
                    "default": f"/tmp/{var_name}",
                }

    return variables


# ─────────────────────────────────────────────
#  Sprint 64 — Coverage Report & Validation
# ─────────────────────────────────────────────

def generate_coverage_report(jobs, linkage, dep_graph, calendars):
    """Generate a migration coverage report for AutoSys (Sprint 64)."""
    total = len(jobs)
    linked = sum(1 for v in linkage.values() if v.get("linked"))
    pmcmd = len(linkage)
    unlinked_wf = [v["workflow"] for v in linkage.values() if v.get("linked") is False]

    # Job type breakdown
    type_counts = {}
    for j in jobs:
        jtype = classify_job(j)
        type_counts[jtype] = type_counts.get(jtype, 0) + 1

    # Dependency coverage
    jobs_with_deps = sum(1 for name in dep_graph if dep_graph[name])
    jobs_with_schedule = sum(1 for j in jobs if j.get("start_times") or j.get("days_of_week"))

    report = {
        "total_jobs": total,
        "job_types": type_counts,
        "dependency_coverage": {
            "jobs_with_dependencies": jobs_with_deps,
            "jobs_without_dependencies": total - jobs_with_deps,
        },
        "informatica_linkage": {
            "pmcmd_jobs": pmcmd,
            "linked_to_inventory": linked,
            "unlinked_workflows": unlinked_wf,
            "linkage_rate": round(linked / max(pmcmd, 1) * 100, 1),
        },
        "schedule_coverage": {
            "jobs_with_schedule": jobs_with_schedule,
            "calendars_found": len(calendars),
        },
        "conversion_rate": round(
            (linked + (total - pmcmd)) / max(total, 1) * 100, 1
        ),
    }

    return report


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────

def main():
    input_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else INPUT_DIR

    if not input_dir.exists():
        print(f"  ⚠️  AutoSys input directory not found: {input_dir}")
        print("     Place .jil files in input/autosys/ or pass a directory path.")
        return

    jil_files = sorted(input_dir.glob("*.jil"))
    if not jil_files:
        print(f"  ⚠️  No .jil files found in {input_dir}")
        return

    target = _get_target()
    target_label = "Databricks Workflows" if target == "databricks" else "Fabric Pipelines"

    print("=" * 60)
    print(f"  AutoSys JIL Migration [{target_label}]")
    print("=" * 60)
    print()

    # Parse all JIL files
    all_jobs = []
    all_calendars = []
    for jil_file in jil_files:
        print(f"  Parsing: {jil_file.name}")
        text = jil_file.read_text(encoding="utf-8")
        jobs, cals = parse_jil(text)
        all_jobs.extend(jobs)
        all_calendars.extend(cals)
        for j in jobs:
            jtype = classify_job(j)
            print(f"    → {j['name']} ({jtype})")

    print(f"\n  Total jobs parsed: {len(all_jobs)}")
    print(f"  Calendars found: {len(all_calendars)}")

    # Build dependency graph
    dep_graph = build_dependency_graph(all_jobs)

    # Load inventory for linkage (if available)
    inv_workflows = None
    if INVENTORY_PATH.exists():
        with open(INVENTORY_PATH, encoding="utf-8") as f:
            inv = json.load(f)
        inv_workflows = inv.get("workflows", [])

    # Link jobs to Informatica workflows
    linkage = link_jobs_to_workflows(all_jobs, inv_workflows)
    print(f"  Informatica links: {len(linkage)} jobs call pmcmd")

    # Generate output
    print()
    summary = write_autosys_output(all_jobs, all_calendars, dep_graph, linkage, target)

    print()
    print("=" * 60)
    print(f"  AutoSys pipelines generated: {summary['pipelines_generated']}")
    print(f"  Informatica linkage: {summary['informatica_linkage']['pmcmd_jobs']} pmcmd, "
          f"{summary['informatica_linkage']['linked_to_inventory']} linked")
    if summary['informatica_linkage']['unlinked']:
        print(f"  ⚠️  Unlinked workflows: {', '.join(summary['informatica_linkage']['unlinked'])}")
    print(f"  Output: {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
