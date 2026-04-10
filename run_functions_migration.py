"""
Azure Functions Migration — Event-Driven & CDC
Reads inventory.json and generates Azure Functions project artifacts
for Informatica mappings identified as Functions candidates.

Generates:
  output/functions/host.json              — Functions host configuration
  output/functions/requirements.txt       — Python dependencies
  output/functions/local.settings.json    — Local dev settings
  output/functions/function_app.py        — Main function app (v2 programming model)
  output/functions/FN_<mapping>.py        — Individual function modules

Usage:
    python run_functions_migration.py
    python run_functions_migration.py path/to/inventory.json
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output" / "functions"
INVENTORY_PATH = WORKSPACE / "output" / "inventory" / "inventory.json"


def _get_target():
    """Return the target platform ('fabric' or 'databricks')."""
    return os.environ.get("INFORMATICA_MIGRATION_TARGET", "fabric")


# ─────────────────────────────────────────────
#  Host & project scaffolding
# ─────────────────────────────────────────────

def generate_host_json():
    """Generate the Azure Functions host.json configuration."""
    return {
        "version": "2.0",
        "logging": {
            "applicationInsights": {
                "samplingSettings": {
                    "isEnabled": True,
                    "excludedTypes": "Request"
                }
            },
            "logLevel": {
                "default": "Information",
                "Host.Results": "Error",
                "Function": "Information",
                "Host.Aggregator": "Trace"
            }
        },
        "extensionBundle": {
            "id": "Microsoft.Azure.Functions.ExtensionBundle",
            "version": "[4.*, 5.0.0)"
        },
        "extensions": {
            "serviceBus": {
                "prefetchCount": 10,
                "messageHandlerOptions": {
                    "maxConcurrentCalls": 16,
                    "autoComplete": True
                }
            },
            "cosmosDB": {
                "connectionMode": "Direct"
            }
        },
        "functionTimeout": "00:10:00"
    }


def generate_local_settings():
    """Generate local.settings.json for local development."""
    return {
        "IsEncrypted": False,
        "Values": {
            "AzureWebJobsStorage": "UseDevelopmentStorage=true",
            "FUNCTIONS_WORKER_RUNTIME": "python",
            "ServiceBusConnection__fullyQualifiedNamespace": "<your-namespace>.servicebus.windows.net",
            "EventHubConnection__fullyQualifiedNamespace": "<your-namespace>.eventhubs.windows.net",
            "CosmosDBConnection": "<your-cosmosdb-connection-string>",
            "SqlConnectionString": "Server=<server>;Database=<db>;Authentication=Active Directory Default;",
            "FABRIC_LAKEHOUSE_ENDPOINT": "",
            "FABRIC_WORKSPACE_ID": "",
        },
        "ConnectionStrings": {}
    }


def generate_requirements_txt():
    """Generate requirements.txt for the Functions project."""
    return (
        "azure-functions\n"
        "azure-identity\n"
        "azure-servicebus\n"
        "azure-eventhub\n"
        "azure-cosmos\n"
        "pyodbc\n"
        "delta-sharing\n"
    )


# ─────────────────────────────────────────────
#  Function generators by trigger type
# ─────────────────────────────────────────────

def generate_service_bus_function(mapping):
    """Generate an Azure Function with Service Bus trigger for queue/ESB patterns."""
    name = mapping["name"]
    fn_name = f"FN_{name}"
    targets = mapping.get("targets", [])
    target_table = targets[0] if targets else "target_table"
    sources = mapping.get("sources", [])
    source_desc = sources[0] if sources else "source_queue"
    queue_name = source_desc.lower().replace(" ", "_")

    streaming_sources = mapping.get("streaming", {}).get("streaming_sources", [])
    if streaming_sources:
        topic = streaming_sources[0].get("topic", queue_name)
        queue_name = topic if topic else queue_name

    code = f'''"""
Azure Function: {fn_name}
Migrated from: Informatica mapping {name}
Trigger: Service Bus Queue
Pattern: Message queue consumer → transform → write to target
"""
import azure.functions as func
import json
import logging
import os
from datetime import datetime, timezone

app = func.FunctionApp()

@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name="{queue_name}",
    connection="ServiceBusConnection",
)
def {fn_name}(msg: func.ServiceBusMessage) -> None:
    """Process message from Service Bus queue.

    Migrated from Informatica mapping: {name}
    Original source: {source_desc}
    Target: {target_table}
    """
    logging.info(f"{fn_name}: Processing message ID={{msg.message_id}}")

    try:
        # Deserialize message body
        body = msg.get_body().decode("utf-8")
        payload = json.loads(body)

        # ── Transform (migrated from Informatica mapping logic) ──
        record = _transform(payload)

        # ── Write to target ──
        _write_to_target(record)

        logging.info(
            f"{fn_name}: Successfully processed message "
            f"ID={{msg.message_id}}"
        )
    except Exception as exc:
        logging.error(f"{fn_name}: Error processing message: {{exc}}")
        raise  # Let Service Bus handle retry / dead-letter


def _transform(payload: dict) -> dict:
    """Apply transformation logic.

    TODO: Implement mapping-specific transformations migrated from
    Informatica mapping {name}.
    """
    record = {{
        **payload,
        "_processed_at": datetime.now(timezone.utc).isoformat(),
        "_source": "{source_desc}",
    }}
    return record


def _write_to_target(record: dict) -> None:
    """Write transformed record to {target_table}.

    TODO: Configure target connection:
    - Fabric Lakehouse: Use OneLake REST API or Delta Sharing
    - Azure SQL: Use pyodbc with SqlConnectionString
    - Cosmos DB: Use azure-cosmos SDK
    """
    logging.info(f"Would write to {target_table}: {{json.dumps(record)[:200]}}")
'''
    return {"name": fn_name, "code": code, "trigger_type": "service_bus"}


def generate_event_hub_function(mapping):
    """Generate an Azure Function with Event Hub trigger."""
    name = mapping["name"]
    fn_name = f"FN_{name}"
    targets = mapping.get("targets", [])
    target_table = targets[0] if targets else "target_table"
    sources = mapping.get("sources", [])

    streaming_sources = mapping.get("streaming", {}).get("streaming_sources", [])
    event_hub_name = "events"
    if streaming_sources:
        topic = streaming_sources[0].get("topic", "")
        if topic:
            event_hub_name = topic

    code = f'''"""
Azure Function: {fn_name}
Migrated from: Informatica mapping {name}
Trigger: Event Hub
Pattern: Event stream consumer → transform → write to target
"""
import azure.functions as func
import json
import logging
import os
from datetime import datetime, timezone

app = func.FunctionApp()

@app.event_hub_message_trigger(
    arg_name="events",
    event_hub_name="{event_hub_name}",
    connection="EventHubConnection",
    consumer_group="$Default",
    cardinality="many",
)
def {fn_name}(events: list[func.EventHubEvent]) -> None:
    """Process batch of events from Event Hub.

    Migrated from Informatica mapping: {name}
    Target: {target_table}
    """
    logging.info(f"{fn_name}: Processing batch of {{len(events)}} events")

    records = []
    for event in events:
        try:
            body = event.get_body().decode("utf-8")
            payload = json.loads(body)
            record = _transform(payload)
            records.append(record)
        except Exception as exc:
            logging.error(
                f"{fn_name}: Error processing event: {{exc}}"
            )

    if records:
        _write_batch(records)
        logging.info(f"{fn_name}: Wrote {{len(records)}} records to {target_table}")


def _transform(payload: dict) -> dict:
    """Apply transformation logic.

    TODO: Implement mapping-specific transformations from
    Informatica mapping {name}.
    """
    return {{
        **payload,
        "_processed_at": datetime.now(timezone.utc).isoformat(),
    }}


def _write_batch(records: list[dict]) -> None:
    """Write batch of transformed records to {target_table}.

    TODO: Configure target connection.
    """
    logging.info(f"Would write {{len(records)}} records to {target_table}")
'''
    return {"name": fn_name, "code": code, "trigger_type": "event_hub"}


def generate_sql_trigger_function(mapping):
    """Generate an Azure Function with SQL trigger for CDC patterns."""
    name = mapping["name"]
    fn_name = f"FN_{name}"
    targets = mapping.get("targets", [])
    target_table = targets[0] if targets else "target_table"
    sources = mapping.get("sources", [])
    source_table = sources[0] if sources else "source_table"

    cdc_info = mapping.get("cdc", {})
    cdc_type = cdc_info.get("cdc_type", "upsert_only")
    merge_keys = cdc_info.get("merge_keys", [])
    key_col = merge_keys[0] if merge_keys else "id"

    code = f'''"""
Azure Function: {fn_name}
Migrated from: Informatica mapping {name}
Trigger: SQL (Change Tracking)
Pattern: CDC — row changes on {source_table} → transform → write to {target_table}
CDC Type: {cdc_type}
"""
import azure.functions as func
import json
import logging
import os
from datetime import datetime, timezone

app = func.FunctionApp()

@app.sql_trigger(
    arg_name="changes",
    table_name="{source_table}",
    connection_string_setting="SqlConnectionString",
)
def {fn_name}(changes: str) -> None:
    """Process CDC changes from SQL table {source_table}.

    Migrated from Informatica mapping: {name}
    CDC type: {cdc_type}
    Merge key: {key_col}
    Target: {target_table}
    """
    rows = json.loads(changes)
    logging.info(f"{fn_name}: Processing {{len(rows)}} change(s) from {source_table}")

    try:
        inserts = []
        updates = []
        deletes = []

        for row in rows:
            operation = row.get("_operation", 0)
            record = _transform(row)

            if operation == 0:  # Insert
                inserts.append(record)
            elif operation == 1:  # Update
                updates.append(record)
            elif operation == 2:  # Delete
                deletes.append(record)

        if inserts:
            _upsert_records(inserts, "{key_col}")
            logging.info(f"{fn_name}: Inserted {{len(inserts)}} record(s)")
        if updates:
            _upsert_records(updates, "{key_col}")
            logging.info(f"{fn_name}: Updated {{len(updates)}} record(s)")
        if deletes and "{cdc_type}" == "full_cdc":
            _delete_records(deletes, "{key_col}")
            logging.info(f"{fn_name}: Deleted {{len(deletes)}} record(s)")
    except Exception as exc:
        logging.error(f"{fn_name}: Error processing changes: {{exc}}")
        raise


def _transform(row: dict) -> dict:
    """Apply transformation logic.

    TODO: Implement mapping-specific transformations from
    Informatica mapping {name}.
    """
    result = {{k: v for k, v in row.items() if not k.startswith("_")}}
    result["_processed_at"] = datetime.now(timezone.utc).isoformat()
    return result


def _upsert_records(records: list[dict], key_col: str) -> None:
    """Upsert records to {target_table}.

    TODO: Configure target connection (pyodbc, Fabric REST API, etc.)
    """
    logging.info(f"Would upsert {{len(records)}} records to {target_table} on {{key_col}}")


def _delete_records(records: list[dict], key_col: str) -> None:
    """Delete records from {target_table}.

    TODO: Configure target connection.
    """
    keys = [r.get(key_col) for r in records]
    logging.info(f"Would delete {{len(keys)}} records from {target_table}")
'''
    return {"name": fn_name, "code": code, "trigger_type": "sql_trigger"}


def generate_cosmos_trigger_function(mapping):
    """Generate an Azure Function with Cosmos DB change feed trigger."""
    name = mapping["name"]
    fn_name = f"FN_{name}"
    targets = mapping.get("targets", [])
    target_table = targets[0] if targets else "target_table"
    sources = mapping.get("sources", [])
    source_container = sources[0] if sources else "source_container"

    code = f'''"""
Azure Function: {fn_name}
Migrated from: Informatica mapping {name}
Trigger: Cosmos DB Change Feed
Pattern: Change feed on {source_container} → transform → write to {target_table}
"""
import azure.functions as func
import json
import logging
import os
from datetime import datetime, timezone

app = func.FunctionApp()

@app.cosmos_db_trigger_v3(
    arg_name="documents",
    container_name="{source_container}",
    database_name="sourcedb",
    connection="CosmosDBConnection",
    lease_container_name="leases",
    create_lease_container_if_not_exists=True,
)
def {fn_name}(documents: func.DocumentList) -> None:
    """Process Cosmos DB change feed documents.

    Migrated from Informatica mapping: {name}
    Source container: {source_container}
    Target: {target_table}
    """
    logging.info(f"{fn_name}: Processing {{len(documents)}} document change(s)")

    try:
        records = []
        for doc in documents:
            payload = doc.to_dict()
            record = _transform(payload)
            records.append(record)

        if records:
            _write_batch(records)
            logging.info(f"{fn_name}: Wrote {{len(records)}} records to {target_table}")
    except Exception as exc:
        logging.error(f"{fn_name}: Error processing change feed: {{exc}}")
        raise


def _transform(payload: dict) -> dict:
    """Apply transformation logic.

    TODO: Implement mapping-specific transformations from
    Informatica mapping {name}.
    """
    result = {{k: v for k, v in payload.items() if not k.startswith("_")}}
    result["_processed_at"] = datetime.now(timezone.utc).isoformat()
    return result


def _write_batch(records: list[dict]) -> None:
    """Write batch to {target_table}.

    TODO: Configure target connection.
    """
    logging.info(f"Would write {{len(records)}} records to {target_table}")
'''
    return {"name": fn_name, "code": code, "trigger_type": "cosmos_trigger"}


def generate_http_function(mapping):
    """Generate an Azure Function with HTTP trigger for ESB/API patterns."""
    name = mapping["name"]
    fn_name = f"FN_{name}"
    targets = mapping.get("targets", [])
    target_table = targets[0] if targets else "target_table"

    code = f'''"""
Azure Function: {fn_name}
Migrated from: Informatica mapping {name}
Trigger: HTTP (REST API)
Pattern: ESB / API gateway → transform → write to target
"""
import azure.functions as func
import json
import logging
import os
from datetime import datetime, timezone

app = func.FunctionApp()

@app.route(
    route="{name.lower()}",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION,
)
def {fn_name}(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP endpoint replacing ESB/API gateway pattern.

    Migrated from Informatica mapping: {name}
    Target: {target_table}
    """
    logging.info(f"{fn_name}: HTTP trigger received request")

    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({{"error": "Invalid JSON body"}}),
            status_code=400,
            mimetype="application/json",
        )

    try:
        record = _transform(payload)
        _write_to_target(record)

        return func.HttpResponse(
            json.dumps({{
                "status": "accepted",
                "mapping": "{name}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }}),
            status_code=202,
            mimetype="application/json",
        )
    except Exception as exc:
        logging.error(f"{fn_name}: Error: {{exc}}")
        return func.HttpResponse(
            json.dumps({{"error": str(exc)}}),
            status_code=500,
            mimetype="application/json",
        )


def _transform(payload: dict) -> dict:
    """Apply transformation logic.

    TODO: Implement mapping-specific transformations from
    Informatica mapping {name}.
    """
    return {{
        **payload,
        "_processed_at": datetime.now(timezone.utc).isoformat(),
    }}


def _write_to_target(record: dict) -> None:
    """Write to {target_table}.

    TODO: Configure target connection.
    """
    logging.info(f"Would write to {target_table}: {{json.dumps(record)[:200]}}")
'''
    return {"name": fn_name, "code": code, "trigger_type": "http"}


def generate_timer_function(mapping):
    """Generate an Azure Function with Timer trigger for lightweight periodic processing."""
    name = mapping["name"]
    fn_name = f"FN_{name}"
    targets = mapping.get("targets", [])
    target_table = targets[0] if targets else "target_table"
    sources = mapping.get("sources", [])
    source_desc = sources[0] if sources else "source"

    code = f'''"""
Azure Function: {fn_name}
Migrated from: Informatica mapping {name}
Trigger: Timer (scheduled)
Pattern: Periodic poll → transform → write to target
"""
import azure.functions as func
import json
import logging
import os
from datetime import datetime, timezone

app = func.FunctionApp()

@app.timer_trigger(
    arg_name="timer",
    schedule="0 */5 * * * *",  # Every 5 minutes — TODO: adjust from original schedule
    run_on_startup=False,
)
def {fn_name}(timer: func.TimerRequest) -> None:
    """Periodic processing function.

    Migrated from Informatica mapping: {name}
    Source: {source_desc}
    Target: {target_table}
    """
    if timer.past_due:
        logging.warning(f"{fn_name}: Timer is past due")

    logging.info(f"{fn_name}: Timer triggered at {{datetime.now(timezone.utc).isoformat()}}")

    try:
        records = _read_source()
        transformed = [_transform(r) for r in records]
        if transformed:
            _write_batch(transformed)
            logging.info(f"{fn_name}: Processed {{len(transformed)}} records")
        else:
            logging.info(f"{fn_name}: No new records to process")
    except Exception as exc:
        logging.error(f"{fn_name}: Error: {{exc}}")
        raise


def _read_source() -> list[dict]:
    """Read from {source_desc}.

    TODO: Implement source read logic (SQL query, API call, etc.)
    """
    return []


def _transform(record: dict) -> dict:
    """Apply transformation logic.

    TODO: Implement mapping-specific transformations from
    Informatica mapping {name}.
    """
    return {{
        **record,
        "_processed_at": datetime.now(timezone.utc).isoformat(),
    }}


def _write_batch(records: list[dict]) -> None:
    """Write to {target_table}.

    TODO: Configure target connection.
    """
    logging.info(f"Would write {{len(records)}} records to {target_table}")
'''
    return {"name": fn_name, "code": code, "trigger_type": "timer"}


def generate_blob_trigger_function(mapping):
    """Generate an Azure Function with Blob trigger for file-based patterns."""
    name = mapping["name"]
    fn_name = f"FN_{name}"
    targets = mapping.get("targets", [])
    target_table = targets[0] if targets else "target_table"
    sources = mapping.get("sources", [])
    source_desc = sources[0] if sources else "source_container"

    code = f'''"""
Azure Function: {fn_name}
Migrated from: Informatica mapping {name}
Trigger: Blob Storage
Pattern: File landing → parse → transform → write to target
"""
import azure.functions as func
import json
import logging
import os
from datetime import datetime, timezone

app = func.FunctionApp()

@app.blob_trigger(
    arg_name="blob",
    path="landing/{name.lower()}/{{blobname}}",
    connection="AzureWebJobsStorage",
)
def {fn_name}(blob: func.InputStream) -> None:
    """Process files landing in blob storage.

    Migrated from Informatica mapping: {name}
    Source: {source_desc}
    Target: {target_table}
    """
    logging.info(
        f"{fn_name}: Processing blob '{{blob.name}}' "
        f"({{blob.length}} bytes)"
    )

    try:
        content = blob.read().decode("utf-8")
        records = _parse_file(content, blob.name)
        transformed = [_transform(r) for r in records]

        if transformed:
            _write_batch(transformed)
            logging.info(f"{fn_name}: Wrote {{len(transformed)}} records from {{blob.name}}")
    except Exception as exc:
        logging.error(f"{fn_name}: Error processing {{blob.name}}: {{exc}}")
        raise


def _parse_file(content: str, filename: str) -> list[dict]:
    """Parse file content into records.

    TODO: Implement based on file format (CSV, JSON, XML, etc.)
    """
    if filename.endswith(".json"):
        data = json.loads(content)
        return data if isinstance(data, list) else [data]
    # CSV parsing — TODO: implement with csv module
    return []


def _transform(record: dict) -> dict:
    """Apply transformation logic.

    TODO: Implement mapping-specific transformations from
    Informatica mapping {name}.
    """
    return {{
        **record,
        "_processed_at": datetime.now(timezone.utc).isoformat(),
    }}


def _write_batch(records: list[dict]) -> None:
    """Write to {target_table}.

    TODO: Configure target connection.
    """
    logging.info(f"Would write {{len(records)}} records to {target_table}")
'''
    return {"name": fn_name, "code": code, "trigger_type": "blob"}


# ─────────────────────────────────────────────
#  Dispatch & orchestration
# ─────────────────────────────────────────────

_TRIGGER_GENERATORS = {
    "service_bus": generate_service_bus_function,
    "event_hub": generate_event_hub_function,
    "sql_trigger": generate_sql_trigger_function,
    "cosmos_trigger": generate_cosmos_trigger_function,
    "http": generate_http_function,
    "timer": generate_timer_function,
    "blob": generate_blob_trigger_function,
}


def generate_function(mapping):
    """Generate an Azure Function for a mapping based on its trigger type.

    Returns a dict with keys: name, code, trigger_type.
    """
    func_info = mapping.get("functions_candidate", {})
    trigger_type = func_info.get("trigger_type", "http")
    generator = _TRIGGER_GENERATORS.get(trigger_type, generate_http_function)
    return generator(mapping)


def generate_function_app_entry(functions):
    """Generate the main function_app.py that imports all individual functions.

    Uses the Azure Functions v2 programming model with blueprints.
    """
    lines = [
        '"""',
        "Azure Functions App — Informatica Migration",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}",
        f"Functions: {len(functions)}",
        '"""',
        "import azure.functions as func",
        "",
        "app = func.FunctionApp()",
        "",
        "# ── Registered Functions ──",
    ]

    for fn in functions:
        fn_name = fn["name"]
        trigger = fn["trigger_type"]
        lines.append(f"# {fn_name} — trigger: {trigger}")

    lines.append("")
    lines.append("# Each function is defined in its own module (FN_*.py).")
    lines.append("# To consolidate into a single function_app.py, move the")
    lines.append("# decorated functions from each module into this file.")
    lines.append("")

    return "\n".join(lines)


def main():
    inv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else INVENTORY_PATH
    if not inv_path.exists():
        print(f"ERROR: {inv_path} not found. Run run_assessment.py first.")
        sys.exit(1)

    with open(inv_path, encoding="utf-8") as f:
        inv = json.load(f)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("  Azure Functions Migration")
    print("=" * 60)
    print()

    # Find Functions candidates
    all_mappings = inv.get("mappings", [])
    candidates = [
        m for m in all_mappings
        if m.get("functions_candidate", {}).get("is_candidate", False)
    ]

    if not candidates:
        print("  No Azure Functions candidates found in inventory.")
        print("  (Mappings with event-driven/CDC/ESB patterns are auto-detected)")
        print("=" * 60)
        return

    print(f"  Found {len(candidates)} Functions candidate(s):")
    for c in candidates:
        fc = c["functions_candidate"]
        print(f"    • {c['name']} → {fc['trigger_type']} ({fc['reason']})")
    print()

    # Generate project scaffold
    host_json = generate_host_json()
    with open(OUTPUT_DIR / "host.json", "w", encoding="utf-8") as f:
        json.dump(host_json, f, indent=2)
    print("  ✅ host.json")

    local_settings = generate_local_settings()
    with open(OUTPUT_DIR / "local.settings.json", "w", encoding="utf-8") as f:
        json.dump(local_settings, f, indent=2)
    print("  ✅ local.settings.json")

    req_txt = generate_requirements_txt()
    with open(OUTPUT_DIR / "requirements.txt", "w", encoding="utf-8") as f:
        f.write(req_txt)
    print("  ✅ requirements.txt")

    # Generate individual functions
    functions = []
    for mapping in candidates:
        fn_result = generate_function(mapping)
        fn_path = OUTPUT_DIR / f"{fn_result['name']}.py"
        with open(fn_path, "w", encoding="utf-8") as f:
            f.write(fn_result["code"])
        functions.append(fn_result)
        print(f"  ✅ {fn_result['name']}.py ({fn_result['trigger_type']})")

    # Generate main entry point
    entry_code = generate_function_app_entry(functions)
    with open(OUTPUT_DIR / "function_app.py", "w", encoding="utf-8") as f:
        f.write(entry_code)
    print("  ✅ function_app.py")

    print()
    print("=" * 60)
    print(f"  Functions generated: {len(functions)}")
    print(f"  Output: {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
