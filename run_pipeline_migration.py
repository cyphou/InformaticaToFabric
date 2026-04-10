"""
Pipeline Migration — Phase 3
Reads inventory.json and generates Fabric Data Pipeline JSON definitions
for each Informatica workflow.

Outputs:
  output/pipelines/PL_<workflow_name>.json — one pipeline per workflow

Usage:
    python run_pipeline_migration.py
    python run_pipeline_migration.py path/to/inventory.json
"""

import json
import os
import sys
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output" / "pipelines"
INVENTORY_PATH = WORKSPACE / "output" / "inventory" / "inventory.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _get_target():
    """Return the target platform ('fabric' or 'databricks')."""
    return os.environ.get("INFORMATICA_MIGRATION_TARGET", "fabric")


def _notebook_activity(session_name, mapping_name, depends_on, params):
    """Generate a TridentNotebook activity JSON."""
    dep_list = []
    for dep in depends_on:
        if dep == "Start":
            continue
        dep_list.append({
            "activity": dep,
            "dependencyConditions": ["Succeeded"]
        })

    activity = {
        "name": f"NB_{mapping_name}" if mapping_name else session_name,
        "description": f"Migrated from Informatica session {session_name}.",
        "type": "TridentNotebook",
        "dependsOn": dep_list,
        "policy": {
            "timeout": "0.02:00:00",
            "retry": 2,
            "retryIntervalInSeconds": 60,
            "secureOutput": False,
            "secureInput": False
        },
        "typeProperties": {
            "notebookId": "",
            "notebook": {
                "referenceName": f"NB_{mapping_name}" if mapping_name else f"NB_{session_name}",
                "type": "NotebookReference"
            },
            "parameters": {},
            "sparkPool": {
                "referenceName": "default",
                "type": "BigDataPoolReference"
            }
        }
    }

    # Add parameters from mapping
    if params:
        for p in params:
            param_name = p.replace("$$", "").lower()
            activity["typeProperties"]["parameters"][param_name] = {
                "value": {"value": f"@pipeline().parameters.{param_name}", "type": "Expression"},
                "type": "string"
            }
    else:
        # Default load_date parameter
        activity["typeProperties"]["parameters"]["load_date"] = {
            "value": {"value": "@pipeline().parameters.load_date", "type": "Expression"},
            "type": "string"
        }

    return activity


def _email_activity(email_name, trigger_activity, trigger_condition="Failed"):
    """Generate a WebActivity for email notification."""
    return {
        "name": email_name,
        "description": f"Migrated from Informatica {email_name}. Triggered on {trigger_condition}.",
        "type": "WebActivity",
        "dependsOn": [{
            "activity": trigger_activity,
            "dependencyConditions": [trigger_condition]
        }],
        "policy": {
            "timeout": "0.00:10:00",
            "retry": 1,
            "retryIntervalInSeconds": 30
        },
        "typeProperties": {
            "url": {"value": "@pipeline().parameters.alert_webhook_url", "type": "Expression"},
            "method": "POST",
            "headers": {"Content-Type": "application/json"},
            "body": {
                "value": f"@json(concat('{{\"pipeline\":\"', pipeline().Pipeline, '\",\"activity\":\"{trigger_activity}\",\"status\":\"{trigger_condition}\",\"runId\":\"', pipeline().RunId, '\"}}'))",
                "type": "Expression"
            }
        }
    }


def _decision_activity(decision_name, depends_on_activity, true_activities, false_activities=None):
    """Generate an IfCondition for Informatica Decision tasks."""
    activity = {
        "name": decision_name,
        "description": f"Migrated from Informatica Decision task {decision_name}.",
        "type": "IfCondition",
        "dependsOn": [{
            "activity": depends_on_activity,
            "dependencyConditions": ["Succeeded"]
        }],
        "typeProperties": {
            "expression": {
                "value": f"@greater(int(activity('{depends_on_activity}').output.status.Output.result.exitValue), 0)",
                "type": "Expression"
            },
            "ifTrueActivities": true_activities,
            "ifFalseActivities": false_activities or []
        }
    }
    return activity


def _resolve_activity_name(session_name, session_to_mapping):
    """Resolve a session reference to an activity name."""
    mapping = session_to_mapping.get(session_name)
    if mapping:
        return f"NB_{mapping}"
    return session_name


def _event_wait_activity(event_name, depends_on_activity):
    """Generate a Wait activity for Informatica Event Wait tasks.

    Event Wait blocks execution until a signal is received.
    Maps to Fabric Wait Activity or WebActivity polling pattern.
    """
    return {
        "name": event_name,
        "description": f"Migrated from Informatica Event Wait '{event_name}'. "
                        "TODO: Replace with actual event source (webhook, file sensor, or queue).",
        "type": "Wait",
        "dependsOn": [{
            "activity": depends_on_activity,
            "dependencyConditions": ["Succeeded"]
        }] if depends_on_activity else [],
        "typeProperties": {
            "waitTimeInSeconds": 60
        }
    }


def _event_raise_activity(event_name, depends_on_activity):
    """Generate a WebActivity for Informatica Event Raise tasks.

    Event Raise signals that an event has occurred — other workflows
    waiting on this event can proceed. Maps to a Web Activity that
    posts to a webhook, queue, or event trigger.
    """
    return {
        "name": event_name,
        "description": f"Migrated from Informatica Event Raise '{event_name}'. "
                        "TODO: Configure webhook URL or event mechanism.",
        "type": "WebActivity",
        "dependsOn": [{
            "activity": depends_on_activity,
            "dependencyConditions": ["Succeeded"]
        }] if depends_on_activity else [],
        "policy": {
            "timeout": "0.00:10:00",
            "retry": 1,
            "retryIntervalInSeconds": 30
        },
        "typeProperties": {
            "url": {
                "value": "@pipeline().parameters.event_webhook_url",
                "type": "Expression"
            },
            "method": "POST",
            "headers": {"Content-Type": "application/json"},
            "body": {
                "value": f"@json(concat('{{\"event\":\"{event_name}\",\"pipeline\":\"', pipeline().Pipeline, '\",\"runId\":\"', pipeline().RunId, '\"}}'))",
                "type": "Expression"
            }
        }
    }


def _azure_function_activity(session_name, mapping_name, depends_on, func_info):
    """Generate an AzureFunctionActivity for mappings migrated to Azure Functions.

    Instead of a notebook activity, this invokes the corresponding Azure Function
    via its HTTP endpoint for mappings identified as Functions candidates.
    """
    dep_list = []
    for dep in depends_on:
        if dep == "Start":
            continue
        dep_list.append({
            "activity": dep,
            "dependencyConditions": ["Succeeded"]
        })

    trigger_type = func_info.get("trigger_type", "http")
    fn_name = f"FN_{mapping_name}" if mapping_name else f"FN_{session_name}"

    activity = {
        "name": fn_name,
        "description": (
            f"Migrated from Informatica session {session_name}. "
            f"Runs as Azure Function ({trigger_type} trigger). "
            f"Reason: {func_info.get('reason', '')}"
        ),
        "type": "AzureFunctionActivity",
        "dependsOn": dep_list,
        "policy": {
            "timeout": "0.00:10:00",
            "retry": 2,
            "retryIntervalInSeconds": 30,
            "secureOutput": False,
            "secureInput": False
        },
        "typeProperties": {
            "functionName": fn_name,
            "method": "POST",
            "headers": {"Content-Type": "application/json"},
            "body": {
                "value": "@json(concat('{\"pipeline\":\"', pipeline().Pipeline, "
                         "'\",\"runId\":\"', pipeline().RunId, '\"}'))",
                "type": "Expression"
            },
            "functionAppUrl": {
                "value": "@pipeline().parameters.function_app_url",
                "type": "Expression"
            }
        }
    }
    return activity


def generate_pipeline(workflow, mappings_by_name):
    """Generate a complete Fabric Pipeline JSON for a workflow."""
    name = workflow["name"]
    sessions = workflow.get("sessions", [])
    session_to_mapping = workflow.get("session_to_mapping", {})
    dependencies = workflow.get("dependencies", {})
    decision_tasks = workflow.get("decision_tasks", [])
    email_tasks = workflow.get("email_tasks", [])
    event_wait_tasks = workflow.get("event_wait_tasks", [])
    event_raise_tasks = workflow.get("event_raise_tasks", [])
    schedule = workflow.get("schedule", "")
    is_iics = workflow.get("format") == "iics"

    activities = []
    decision_set = set(decision_tasks)
    email_set = set(email_tasks)

    # Track which sessions are inside decision branches
    sessions_in_decisions = set()
    has_function_activities = False

    # 1. Build notebook or Azure Function activities for each session
    for session in sessions:
        mapping_name = session_to_mapping.get(session, session.replace("S_", ""))
        mapping = mappings_by_name.get(mapping_name, {})
        params = mapping.get("parameters", [])
        func_info = mapping.get("functions_candidate", {})

        # Resolve dependencies
        deps = dependencies.get(session, [])
        resolved_deps = []
        for dep in deps:
            if dep == "Start":
                continue
            elif dep in decision_set:
                # This session depends on a decision — it will be nested inside
                sessions_in_decisions.add(session)
                continue
            else:
                resolved_deps.append(_resolve_activity_name(dep, session_to_mapping))

        if session not in sessions_in_decisions:
            # Use AzureFunctionActivity if mapping is a Functions candidate
            if func_info.get("is_candidate", False):
                activity = _azure_function_activity(session, mapping_name, deps, func_info)
                has_function_activities = True
            else:
                activity = _notebook_activity(session, mapping_name, deps, params)
            # Fix dependsOn to use resolved names
            activity["dependsOn"] = [
                {"activity": _resolve_activity_name(d, session_to_mapping), "dependencyConditions": ["Succeeded"]}
                for d in deps if d != "Start"
            ]
            activities.append(activity)

    # 2. Build decision activities
    for dec_name in decision_tasks:
        # Find what the decision depends on
        dec_deps = dependencies.get(dec_name, [])
        dep_activity = _resolve_activity_name(dec_deps[0], session_to_mapping) if dec_deps else ""

        # Find sessions that depend on this decision
        true_sessions = []
        for session, deps in dependencies.items():
            if dec_name in deps and session not in email_set:
                mapping_name = session_to_mapping.get(session, session.replace("S_", ""))
                mapping = mappings_by_name.get(mapping_name, {})
                params = mapping.get("parameters", [])
                true_sessions.append(_notebook_activity(session, mapping_name, [], params))

        false_activities = [{
            "name": f"Set_Skip_{dec_name}",
            "type": "SetVariable",
            "dependsOn": [],
            "typeProperties": {
                "variableName": "skip_reason",
                "value": f"{dec_name} evaluated FALSE — downstream skipped."
            }
        }]

        activities.append(_decision_activity(dec_name, dep_activity, true_sessions, false_activities))

    # 3. Build email activities
    for email_name in email_tasks:
        email_deps = dependencies.get(email_name, [])
        for dep in email_deps:
            trigger = _resolve_activity_name(dep, session_to_mapping)
            activities.append(_email_activity(email_name, trigger))
            break  # One email activity per email task

    # 4. Build Event Wait activities (Sprint 66)
    for ew_name in event_wait_tasks:
        ew_deps = dependencies.get(ew_name, [])
        dep_activity = _resolve_activity_name(ew_deps[0], session_to_mapping) if ew_deps else ""
        activities.append(_event_wait_activity(ew_name, dep_activity))

    # 5. Build Event Raise activities (Sprint 66)
    for er_name in event_raise_tasks:
        er_deps = dependencies.get(er_name, [])
        dep_activity = _resolve_activity_name(er_deps[0], session_to_mapping) if er_deps else ""
        activities.append(_event_raise_activity(er_name, dep_activity))

    # 6. Collect pipeline parameters
    pipeline_params = {
        "load_date": {
            "type": "string",
            "defaultValue": "@utcnow('yyyy-MM-dd')"
        }
    }
    # Add alert webhook if there are email tasks
    if email_tasks:
        pipeline_params["alert_webhook_url"] = {
            "type": "string",
            "defaultValue": "https://your-logic-app-webhook-url"
        }
    # Add event webhook if there are event raise tasks
    if event_raise_tasks:
        pipeline_params["event_webhook_url"] = {
            "type": "string",
            "defaultValue": "https://your-event-webhook-url"
        }
    # Add function app URL if there are Azure Function activities
    if has_function_activities:
        pipeline_params["function_app_url"] = {
            "type": "string",
            "defaultValue": "https://your-function-app.azurewebsites.net"
        }

    # 7. Annotations
    annotations = [
        "MigratedFromInformatica",
        f"OriginalWorkflow:{name}",
    ]
    if schedule:
        annotations.append(f"OriginalSchedule:{schedule}")

    # 8. Schedule trigger (from schedule_cron if available)
    schedule_cron = workflow.get("schedule_cron", {})
    trigger = None
    cron_expr = schedule_cron.get("cron", "") if schedule_cron else ""
    if cron_expr:
        trigger = {
            "type": "ScheduleTrigger",
            "description": schedule_cron.get("note", ""),
            "typeProperties": {
                "recurrence": {
                    "cron": cron_expr,
                    "timeZone": "UTC",
                }
            }
        }

    pipeline = {
        "name": f"PL_{name}",
        "properties": {
            "description": f"Migrated from Informatica workflow {name}. {f'Original schedule: {schedule}.' if schedule else ''}",
            "activities": activities,
            "parameters": pipeline_params,
            "variables": {
                "skip_reason": {"type": "String", "defaultValue": ""}
            },
            "annotations": annotations
        }
    }

    if trigger:
        pipeline["properties"]["trigger"] = trigger

    return pipeline


def generate_databricks_workflow(workflow, mappings_by_name):
    """Generate a Databricks Workflow JSON for a workflow."""
    name = workflow["name"]
    sessions = workflow.get("sessions", [])
    session_to_mapping = workflow.get("session_to_mapping", {})
    dependencies = workflow.get("dependencies", {})
    schedule = workflow.get("schedule", "")
    schedule_cron = workflow.get("schedule_cron", {})

    tasks = []
    for session in sessions:
        mapping_name = session_to_mapping.get(session, session.replace("S_", ""))
        mapping = mappings_by_name.get(mapping_name, {})
        params = mapping.get("parameters", [])

        # Resolve dependencies
        deps = dependencies.get(session, [])
        depends_on = []
        for dep in deps:
            if dep == "Start":
                continue
            resolved = _resolve_activity_name(dep, session_to_mapping)
            depends_on.append({"task_key": resolved})

        task = {
            "task_key": f"NB_{mapping_name}" if mapping_name else session,
            "description": f"Migrated from Informatica session {session}.",
            "notebook_task": {
                "notebook_path": f"/Workspace/Shared/migration/NB_{mapping_name}" if mapping_name else f"/Workspace/Shared/migration/NB_{session}",
                "base_parameters": {},
            },
            "timeout_seconds": 7200,
            "max_retries": 2,
            "min_retry_interval_millis": 60000,
        }

        if depends_on:
            task["depends_on"] = depends_on

        # Add parameters
        if params:
            for p in params:
                param_name = p.replace("$$", "").lower()
                task["notebook_task"]["base_parameters"][param_name] = ""
        else:
            task["notebook_task"]["base_parameters"]["load_date"] = "{{job.start_time.iso_date}}"

        tasks.append(task)

    # Build the workflow job definition
    job = {
        "name": f"PL_{name}",
        "description": f"Migrated from Informatica workflow {name}. {f'Original schedule: {schedule}.' if schedule else ''}",
        "tasks": tasks,
        "tags": {
            "migrated_from": "informatica",
            "original_workflow": name,
        },
        "format": "MULTI_TASK",
    }

    # Schedule (cron)
    cron_expr = schedule_cron.get("cron", "") if schedule_cron else ""
    if cron_expr:
        job["schedule"] = {
            "quartz_cron_expression": cron_expr,
            "timezone_id": "UTC",
            "pause_status": "PAUSED",
        }

    return job


# ─────────────────────────────────────────────
#  Sprint 80: Eventstream Definition Generator
# ─────────────────────────────────────────────

def generate_eventstream(mapping, workflow=None):
    """Generate a Fabric Eventstream definition for a streaming/event-driven mapping.

    Converts Informatica real-time/event-driven workflows to Fabric Eventstream
    definitions, mapping Kafka/JMS/MQTT sources to Event Hub or Custom App inputs,
    and routing to Lakehouse or KQL Database destinations.

    Returns a dict representing the Eventstream JSON definition.
    """
    name = mapping["name"]
    streaming_info = mapping.get("streaming", {})
    cdc_info = mapping.get("cdc", {})
    sources = streaming_info.get("streaming_sources", [])
    targets = mapping.get("targets", [])

    # Build input sources
    input_sources = []
    for src in sources:
        src_type = src.get("type", "").lower()
        topic = src.get("topic", "default-topic")

        if "kafka" in src_type or "jms" in src_type:
            input_sources.append({
                "name": src.get("name", "kafka_input"),
                "type": "CustomApp",
                "description": f"Migrated from Informatica source: {src.get('name', '')} ({src_type})",
                "properties": {
                    "endpoint": "TODO: Configure Custom App endpoint or use Event Hub",
                    "consumerGroup": "$Default",
                    "originalTopic": topic,
                    "originalSourceType": src_type,
                }
            })
        elif any(kw in src_type for kw in ("eventhub", "event hub")):
            input_sources.append({
                "name": src.get("name", "eventhub_input"),
                "type": "EventHub",
                "description": f"Migrated from Informatica Event Hub source: {src.get('name', '')}",
                "properties": {
                    "eventHubNamespace": "TODO: Configure Event Hub namespace",
                    "eventHubName": topic,
                    "consumerGroup": "$Default",
                }
            })
        else:
            input_sources.append({
                "name": src.get("name", "custom_input"),
                "type": "CustomApp",
                "description": f"Migrated from Informatica source: {src.get('name', '')} ({src_type})",
                "properties": {
                    "endpoint": "TODO: Configure ingestion endpoint",
                    "originalSourceType": src_type,
                }
            })

    # If no streaming sources detected, use a generic Custom App
    if not input_sources:
        input_sources.append({
            "name": "custom_input",
            "type": "CustomApp",
            "description": "TODO: Configure event source",
            "properties": {"endpoint": "TODO"}
        })

    # Build destinations
    destinations = []
    for tgt in targets:
        tgt_lower = tgt.lower()
        if "gold" in tgt_lower or "agg" in tgt_lower:
            tier = "gold"
        elif "silver" in tgt_lower or "fact" in tgt_lower or "dim" in tgt_lower:
            tier = "silver"
        else:
            tier = "bronze"

        destinations.append({
            "name": f"dest_{tgt}",
            "type": "Lakehouse",
            "description": f"Route events to {tier}.{tgt_lower}",
            "properties": {
                "workspaceId": "TODO: Set workspace ID",
                "lakehouseId": "TODO: Set Lakehouse ID",
                "tableName": f"{tier}.{tgt_lower}",
                "inputSerialization": {
                    "type": "Json",
                    "properties": {"encoding": "UTF8"}
                }
            }
        })

    # Build transformations (lightweight in-stream processing)
    transformations = []
    if cdc_info.get("is_cdc", False):
        cdc_col = "CDC_OPERATION"
        if cdc_info.get("cdc_sources"):
            cdc_col = cdc_info["cdc_sources"][0].get("cdc_column", "CDC_OPERATION")
        transformations.append({
            "name": "filter_cdc_ops",
            "type": "Filter",
            "description": "Filter by CDC operation type",
            "properties": {
                "expression": f"{cdc_col} IN ('I', 'U', 'D')"
            }
        })

    eventstream = {
        "name": f"ES_{name}",
        "type": "Microsoft.Fabric.Eventstream",
        "description": f"Migrated from Informatica mapping {name}. "
                       f"Original streaming sources: {', '.join(s.get('name', '') for s in sources)}.",
        "properties": {
            "inputSources": input_sources,
            "transformations": transformations,
            "destinations": destinations,
            "annotations": [
                "MigratedFromInformatica",
                f"OriginalMapping:{name}",
            ],
        }
    }

    return eventstream


def main():
    inv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else INVENTORY_PATH
    if not inv_path.exists():
        print(f"ERROR: {inv_path} not found. Run run_assessment.py first.")
        sys.exit(1)

    with open(inv_path, encoding="utf-8") as f:
        inv = json.load(f)

    target = _get_target()
    target_label = "Databricks Workflows" if target == "databricks" else "Fabric Pipelines"

    # Check if dbt mode is active (auto or dbt)
    dbt_mode = os.environ.get("INFORMATICA_DBT_MODE", "")
    use_mixed = target == "databricks" and dbt_mode in ("auto", "dbt")

    print("=" * 60)
    print(f"  Pipeline Migration — Phase 3 [{target_label}]")
    if use_mixed:
        print(f"  Mode: Mixed dbt + notebook workflows (dbt_mode={dbt_mode})")
    print("=" * 60)
    print()

    # Build mapping lookup
    mappings_by_name = {m["name"]: m for m in inv.get("mappings", [])}
    workflows = inv.get("workflows", [])
    generated = 0

    # Pre-classify mappings for mixed mode
    dbt_mappings = []
    pyspark_mappings = []
    if use_mixed:
        try:
            from run_dbt_migration import classify_mappings
            all_mappings = inv.get("mappings", [])
            dbt_mappings, pyspark_mappings = classify_mappings(all_mappings)
        except ImportError:
            use_mixed = False

    for wf in workflows:
        if use_mixed:
            from run_dbt_migration import generate_mixed_workflow
            pipeline = generate_mixed_workflow(wf, dbt_mappings, pyspark_mappings)
        elif target == "databricks":
            pipeline = generate_databricks_workflow(wf, mappings_by_name)
        else:
            pipeline = generate_pipeline(wf, mappings_by_name)
        out_path = OUTPUT_DIR / f"PL_{wf['name']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(pipeline, f, indent=2, ensure_ascii=False)
        generated += 1

        if target == "databricks" or use_mixed:
            act_count = len(pipeline.get("tasks", []))
        else:
            act_count = len(pipeline["properties"]["activities"])
        print(f"  ✅ PL_{wf['name']}.json ({act_count} activities)")

    print()
    print("=" * 60)
    print(f"  Pipelines generated: {generated}")
    print(f"  Output: {OUTPUT_DIR}")

    # Sprint 80: Generate Eventstream definitions for streaming mappings
    all_mappings = inv.get("mappings", [])
    streaming_mappings = [m for m in all_mappings if m.get("streaming", {}).get("is_streaming", False)]
    es_generated = 0
    if streaming_mappings:
        print()
        print(f"  Generating Eventstream definitions for {len(streaming_mappings)} streaming mapping(s)...")
        for m in streaming_mappings:
            es_def = generate_eventstream(m)
            es_path = OUTPUT_DIR / f"ES_{m['name']}.json"
            with open(es_path, "w", encoding="utf-8") as f:
                json.dump(es_def, f, indent=2, ensure_ascii=False)
            es_generated += 1
            src_count = len(es_def["properties"]["inputSources"])
            dest_count = len(es_def["properties"]["destinations"])
            print(f"    ✅ ES_{m['name']}.json ({src_count} sources, {dest_count} destinations)")
        print(f"  Eventstreams generated: {es_generated}")

    print("=" * 60)


if __name__ == "__main__":
    main()
