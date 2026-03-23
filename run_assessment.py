"""
Informatica-to-Fabric Assessment Agent
Parses all XML mappings, workflows, sessions, and SQL files.
Produces: inventory.json, complexity_report.md, dependency_dag.json
"""

import xml.etree.ElementTree as ET
import json
import os
import re
import sys
import traceback
from pathlib import Path
from collections import OrderedDict

WORKSPACE = Path(r"c:\Users\pidoudet\OneDrive - Microsoft\Boulot\PBI SME\OracleToPostgre\InformaticaToDBFabric")
INPUT_DIR = WORKSPACE / "input"
OUTPUT_DIR = WORKSPACE / "output" / "inventory"

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# IICS (Informatica Intelligent Cloud Services) root tags for format detection
IICS_ROOT_TAGS = {"exportMetadata", "weightedCSPackage", "dTemplate", "design"}
POWERCENTER_ROOT_TAGS = {"POWERMART", "REPOSITORY", "FOLDER", "MAPPING", "WORKFLOW"}

# Transformation type abbreviation mapping
TRANSFORMATION_ABBREV = {
    "Source Qualifier": "SQ",
    "Expression": "EXP",
    "Filter": "FIL",
    "Aggregator": "AGG",
    "Joiner": "JNR",
    "Lookup Procedure": "LKP",
    "Router": "RTR",
    "Update Strategy": "UPD",
    "Sequence Generator": "SEQ",
    "Stored Procedure": "SP",
    "Custom Transformation": "CT",
    "Java Transformation": "JTX",
    "Normalizer": "NRM",
    "Rank": "RNK",
    "Sorter": "SRT",
    "Union": "UNI",
    "Transaction Control": "TC",
    "XML Generator": "XMLG",
    "XML Parser": "XMLP",
    "HTTP Transformation": "HTTP",
    "Unconnected Lookup": "ULKP",
}

# Oracle-specific SQL constructs to flag
ORACLE_PATTERNS = {
    "MERGE": r"\bMERGE\b",
    "DECODE": r"\bDECODE\s*\(",
    "NVL": r"\bNVL\s*\(",
    "NVL2": r"\bNVL2\s*\(",
    "SYSDATE": r"\bSYSDATE\b",
    "SYSTIMESTAMP": r"\bSYSTIMESTAMP\b",
    "ROWNUM": r"\bROWNUM\b",
    "ROWID": r"\bROWID\b",
    "CONNECT BY": r"\bCONNECT\s+BY\b",
    "START WITH": r"\bSTART\s+WITH\b",
    "DUAL": r"\bFROM\s+DUAL\b",
    "TO_DATE": r"\bTO_DATE\s*\(",
    "TO_CHAR": r"\bTO_CHAR\s*\(",
    "TO_NUMBER": r"\bTO_NUMBER\s*\(",
    "LISTAGG": r"\bLISTAGG\s*\(",
    "REGEXP_LIKE": r"\bREGEXP_LIKE\s*\(",
    "REGEXP_REPLACE": r"\bREGEXP_REPLACE\s*\(",
    "DBMS_": r"\bDBMS_\w+",
    "UTL_": r"\bUTL_\w+",
    "PRAGMA": r"\bPRAGMA\b",
    "EXCEPTION WHEN": r"\bEXCEPTION\s+WHEN\b",
    "CURSOR": r"\bCURSOR\b",
    "BULK COLLECT": r"\bBULK\s+COLLECT\b",
    "FORALL": r"\bFORALL\b",
    "PLS_INTEGER": r"\bPLS_INTEGER\b",
    "VARCHAR2": r"\bVARCHAR2\b",
    "NUMBER": r"\bNUMBER\s*\(",
    "SEQUENCE.NEXTVAL": r"\w+\.NEXTVAL\b",
    "PACKAGE BODY": r"\bPACKAGE\s+BODY\b",
    "CREATE OR REPLACE": r"\bCREATE\s+OR\s+REPLACE\b",
}

# Warnings and issues collectors
warnings = []
issues = []  # Structured issues for migration_issues.md


def detect_xml_format(filepath):
    """Detect whether an XML file is PowerCenter or IICS format.
    Returns 'powercenter', 'iics', or 'unknown'.
    """
    try:
        for event, elem in ET.iterparse(filepath, events=("start",)):
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag in POWERCENTER_ROOT_TAGS:
                return "powercenter"
            if tag in IICS_ROOT_TAGS:
                return "iics"
            return "unknown"
    except ET.ParseError:
        return "unknown"
    return "unknown"


def safe_parse_xml(filepath):
    """Safely parse an XML file, handling encoding issues and malformed XML.
    Returns (tree, root) or (None, None) on failure.
    """
    # Try standard parse first
    try:
        tree = ET.parse(filepath)
        return tree, tree.getroot()
    except ET.ParseError as e:
        warnings.append(f"XML parse error in {filepath.name}: {e}")
    except Exception as e:
        warnings.append(f"Unexpected error reading {filepath.name}: {e}")

    # Fallback: read as text, strip invalid chars, re-parse
    try:
        content = filepath.read_text(encoding="utf-8", errors="replace")
        # Remove XML declaration if malformed
        content = re.sub(r'<\?xml[^?]*\?>', '<?xml version="1.0" encoding="UTF-8"?>', content, count=1)
        # Remove null bytes
        content = content.replace("\x00", "")
        root = ET.fromstring(content)
        warnings.append(f"Recovered {filepath.name} after cleaning invalid characters")
        return ET.ElementTree(root), root
    except Exception as e2:
        warnings.append(f"FATAL: Cannot parse {filepath.name} even after cleanup: {e2}")
        issues.append({
            "type": "parse_error",
            "severity": "ERROR",
            "file": str(filepath.name),
            "detail": str(e2),
            "action": "Fix XML manually or re-export from Informatica"
        })
        return None, None


def abbrev(tx_type):
    """Abbreviate transformation type. Unknown types are flagged."""
    short = TRANSFORMATION_ABBREV.get(tx_type)
    if short is None:
        warnings.append(f"Unknown transformation type: '{tx_type}' — using raw name")
        issues.append({
            "type": "unsupported_transformation",
            "severity": "WARNING",
            "detail": f"Transformation type '{tx_type}' not in known mapping",
            "action": "Review manually — may need custom PySpark logic"
        })
        return tx_type
    return short


def parse_mapping_xml(filepath):
    """Parse a single Informatica mapping XML file."""
    fmt = detect_xml_format(filepath)
    if fmt == "iics":
        warnings.append(f"IICS format detected in {filepath.name} — IICS parsing not yet supported")
        issues.append({
            "type": "unsupported_format",
            "severity": "WARNING",
            "file": str(filepath.name),
            "detail": "IICS (Cloud) format detected. Only PowerCenter XML is currently supported.",
            "action": "Export as PowerCenter XML or implement IICS parser"
        })
        return []
    if fmt == "unknown":
        warnings.append(f"Unrecognized XML format in {filepath.name} — attempting PowerCenter parse")

    tree, root = safe_parse_xml(filepath)
    if root is None:
        return []  # Partial results — file skipped

    mappings = []

    mapping_elements = (
        root.findall(".//MAPPING")
        or root.findall("MAPPING")
        or ([root] if root.tag == "MAPPING" else [])
    )

    if not mapping_elements:
        for tag in ["POWERMART", "REPOSITORY", "FOLDER"]:
            mapping_elements = root.findall(f".//{tag}//MAPPING")
            if mapping_elements:
                break

    if not mapping_elements:
        mapping_elements = list(root.iter("MAPPING"))

    for mapping_el in mapping_elements:
      try:
        m_name = mapping_el.get("NAME", os.path.basename(filepath).replace(".xml", ""))

        # Sources — look both inside MAPPING and at FOLDER level (sibling of MAPPING)
        sources = []
        source_elements = list(mapping_el.iter("SOURCE"))
        if not source_elements:
            # Sources may be siblings at the FOLDER level
            source_elements = list(root.iter("SOURCE"))
        for src in source_elements:
            db = src.get("DATABASETYPE", "")
            owner = src.get("OWNERNAME", src.get("DBDNAME", ""))
            name = src.get("NAME", "")
            if owner:
                sources.append(f"{db}.{owner}.{name}" if db else f"{owner}.{name}")
            else:
                sources.append(name)

        # Targets — look both inside MAPPING and at FOLDER level
        targets = []
        target_elements = list(mapping_el.iter("TARGET"))
        if not target_elements:
            target_elements = list(root.iter("TARGET"))
        for tgt in target_elements:
            db = tgt.get("DATABASETYPE", "")
            owner = tgt.get("OWNERNAME", tgt.get("DBDNAME", ""))
            name = tgt.get("NAME", "")
            if owner:
                targets.append(f"{db}.{owner}.{name}" if db else f"{owner}.{name}")
            else:
                targets.append(name)

        # Transformations
        transformations = []
        tx_details = []
        for tx in mapping_el.iter("TRANSFORMATION"):
            tx_type = tx.get("TYPE", "Unknown")
            tx_name = tx.get("NAME", "")
            short = abbrev(tx_type)
            if short not in transformations:
                transformations.append(short)
            tx_details.append({"name": tx_name, "type": tx_type, "abbrev": short})

        # SQL Overrides
        sql_overrides = []
        has_sql_override = False

        for ta in mapping_el.iter("TABLEATTRIBUTE"):
            attr_name = ta.get("NAME", "")
            attr_value = ta.get("VALUE", "")
            if attr_name in ("Sql Query", "Source Filter", "User Defined Join", "Lookup Sql Override") and attr_value.strip():
                sql_overrides.append({
                    "type": attr_name,
                    "value": attr_value.strip()
                })
                has_sql_override = True

        for attr in mapping_el.iter("ATTRIBUTE"):
            attr_name = attr.get("NAME", "")
            attr_value = attr.get("VALUE", "")
            if "sql" in attr_name.lower() and attr_value.strip():
                sql_overrides.append({
                    "type": attr_name,
                    "value": attr_value.strip()
                })
                has_sql_override = True

        # Stored Procedures
        has_stored_proc = "SP" in transformations

        # Parameters
        xml_text = ET.tostring(mapping_el, encoding="unicode")
        param_matches = re.findall(r'\$\$\w+', xml_text)
        parameters = sorted(set(param_matches))

        # Lookup conditions
        lookup_conditions = []
        for tx in mapping_el.iter("TRANSFORMATION"):
            if tx.get("TYPE", "") == "Lookup Procedure":
                lkp_name = tx.get("NAME", "")
                for ta in tx.iter("TABLEATTRIBUTE"):
                    if ta.get("NAME", "") == "Lookup Sql Override":
                        val = ta.get("VALUE", "")
                        if val.strip():
                            lookup_conditions.append({"lookup": lkp_name, "sql": val.strip()})
                    if ta.get("NAME", "") == "Lookup condition":
                        val = ta.get("VALUE", "")
                        if val.strip():
                            lookup_conditions.append({"lookup": lkp_name, "condition": val.strip()})

        # Connectors
        connectors = []
        for conn in mapping_el.iter("CONNECTOR"):
            connectors.append({
                "from_instance": conn.get("FROMINSTANCE", ""),
                "from_field": conn.get("FROMFIELD", ""),
                "to_instance": conn.get("TOINSTANCE", ""),
                "to_field": conn.get("TOFIELD", ""),
                "from_type": conn.get("FROMINSTANCETYPE", ""),
                "to_type": conn.get("TOINSTANCETYPE", ""),
            })

        # Target Load Order
        target_load_order = []
        for tlo in mapping_el.iter("TARGETLOADORDER"):
            tlo_name = tlo.get("TARGETINSTANCE", tlo.get("ORDER", ""))
            if tlo_name:
                target_load_order.append(tlo_name)

        # Complexity Classification
        complexity = classify_complexity(transformations, sql_overrides, sources, targets, has_stored_proc, lookup_conditions)

        mapping_info = {
            "name": m_name,
            "sources": sources,
            "targets": targets,
            "transformations": transformations,
            "transformation_details": tx_details,
            "has_sql_override": has_sql_override,
            "has_stored_proc": has_stored_proc,
            "complexity": complexity,
            "sql_overrides": sql_overrides,
            "lookup_conditions": lookup_conditions,
            "parameters": parameters,
            "target_load_order": target_load_order,
            "connector_count": len(connectors),
        }

        mappings.append(mapping_info)
      except Exception as e:
        warnings.append(f"Error parsing mapping element in {filepath.name}: {e}")
        issues.append({
            "type": "mapping_parse_error",
            "severity": "WARNING",
            "file": str(filepath.name),
            "detail": str(e),
            "action": "Review mapping XML element manually"
        })
        continue

    if not mappings:
        warnings.append(f"WARNING: No <MAPPING> elements found in {filepath.name}")

    return mappings


def classify_complexity(transformations, sql_overrides, sources, targets, has_stored_proc, lookup_conditions):
    tx_set = set(transformations)

    # Custom: Java or Custom transformation
    if tx_set & {"JTX", "CT"}:
        return "Custom"

    # Complex indicators
    complex_indicators = 0

    if "RTR" in tx_set:
        complex_indicators += 2
    if "UPD" in tx_set:
        complex_indicators += 2  # Update Strategy requires Delta MERGE — inherently complex
    if has_stored_proc or "SP" in tx_set:
        complex_indicators += 2
    if len(targets) > 1:
        complex_indicators += 1

    for ovr in sql_overrides:
        val = ovr.get("value", "")
        if re.search(r'\bUNION\b|\bSUBQUERY\b|\bWITH\s+\w+\s+AS\b|\bMERGE\b', val, re.IGNORECASE):
            complex_indicators += 2
        elif re.search(r'\bDECODE\b|\bNVL\b|\bSYSDATE\b|\bROWNUM\b', val, re.IGNORECASE):
            complex_indicators += 1
        elif val:
            complex_indicators += 0.5

    if complex_indicators >= 2:
        return "Complex"

    # Medium indicators
    medium_indicators = 0
    if tx_set & {"LKP", "AGG", "JNR"}:
        medium_indicators += 1
    if len(sources) > 1:
        medium_indicators += 1
    if sql_overrides:
        medium_indicators += 1
    if lookup_conditions:
        medium_indicators += 1

    if medium_indicators >= 1:
        return "Medium"

    return "Simple"


def parse_workflow_xml(filepath):
    """Parse a workflow XML file."""
    tree, root = safe_parse_xml(filepath)
    if root is None:
        return []

    workflows = []

    wf_elements = (
        root.findall(".//WORKFLOW")
        or root.findall("WORKFLOW")
        or ([root] if root.tag == "WORKFLOW" else [])
    )
    if not wf_elements:
        for tag in ["POWERMART", "REPOSITORY", "FOLDER"]:
            wf_elements = root.findall(f".//{tag}//WORKFLOW")
            if wf_elements:
                break
    if not wf_elements:
        wf_elements = list(root.iter("WORKFLOW"))

    for wf_el in wf_elements:
        wf_name = wf_el.get("NAME", os.path.basename(filepath).replace(".xml", ""))

        # Sessions
        sessions = []
        session_to_mapping = {}
        for sess in wf_el.iter("SESSION"):
            s_name = sess.get("NAME", "")
            m_name = sess.get("MAPPINGNAME", "")
            sessions.append(s_name)
            if m_name:
                session_to_mapping[s_name] = m_name

        # Task Instances
        task_instances = []
        for ti in wf_el.iter("TASKINSTANCE"):
            ti_name = ti.get("NAME", "")
            ti_type = ti.get("TASKTYPE", ti.get("TYPE", ""))
            task_instances.append({"name": ti_name, "type": ti_type})

        # Worklets
        worklets = []
        for wl in wf_el.iter("WORKLET"):
            worklets.append(wl.get("NAME", ""))

        # Timers
        has_timer = len(list(wf_el.iter("TIMER"))) > 0

        # Decisions
        decision_tasks = []
        has_decision = False
        for dec in wf_el.iter("DECISION"):
            has_decision = True
            decision_tasks.append(dec.get("NAME", ""))

        # Email tasks
        email_tasks = []

        # Task-instance-based type scanning
        for ti in wf_el.iter("TASKINSTANCE"):
            ti_type = ti.get("TASKTYPE", ti.get("TYPE", "")).upper()
            ti_name = ti.get("NAME", "")
            if "TIMER" in ti_type:
                has_timer = True
            if "DECISION" in ti_type:
                has_decision = True
                if ti_name not in decision_tasks:
                    decision_tasks.append(ti_name)
            if "EMAIL" in ti_type:
                if ti_name not in email_tasks:
                    email_tasks.append(ti_name)

        # Dependencies / Links
        dependencies = {}
        links = []

        for link in wf_el.iter("WORKFLOWLINK"):
            from_task = link.get("FROMTASK", "")
            to_task = link.get("TOTASK", "")
            condition = link.get("CONDITION", "")
            if from_task and to_task:
                links.append({"from": from_task, "to": to_task, "condition": condition})
                if to_task not in dependencies:
                    dependencies[to_task] = []
                if from_task not in dependencies[to_task]:
                    dependencies[to_task].append(from_task)

        # Schedule
        schedule = None
        for sched in wf_el.iter("SCHEDULER"):
            sched_name = sched.get("NAME", "")
            schedule = sched_name

        # Pre/Post SQL from sessions
        pre_post_sql = []
        for sess in wf_el.iter("SESSION"):
            s_name = sess.get("NAME", "")
            for attr in sess.iter("ATTRIBUTE"):
                attr_name = attr.get("NAME", "")
                attr_value = attr.get("VALUE", "")
                if attr_name in ("Pre SQL", "Post SQL") and attr_value.strip():
                    pre_post_sql.append({
                        "session": s_name,
                        "type": attr_name,
                        "value": attr_value.strip()
                    })

        workflow_info = {
            "name": wf_name,
            "sessions": sessions,
            "session_to_mapping": session_to_mapping,
            "dependencies": dependencies,
            "links": links,
            "task_instances": task_instances,
            "worklets": worklets,
            "has_timer": has_timer,
            "has_decision": has_decision,
            "decision_tasks": decision_tasks,
            "email_tasks": email_tasks,
            "pre_post_sql": pre_post_sql,
            "schedule": schedule,
        }
        workflows.append(workflow_info)

    if not workflows:
        warnings.append(f"WARNING: No <WORKFLOW> elements found in {filepath.name}")

    return workflows


def parse_sql_file(filepath):
    """Parse Oracle SQL file and identify Oracle-specific constructs."""
    content = filepath.read_text(encoding="utf-8", errors="replace")

    oracle_constructs = []
    for construct_name, pattern in ORACLE_PATTERNS.items():
        matches = list(re.finditer(pattern, content, re.IGNORECASE))
        if matches:
            lines = []
            for m in matches:
                line_num = content[:m.start()].count('\n') + 1
                lines.append(line_num)
            oracle_constructs.append({
                "construct": construct_name,
                "occurrences": len(matches),
                "lines": lines,
            })

    return {
        "file": filepath.name,
        "path": str(filepath.relative_to(WORKSPACE)),
        "oracle_constructs": oracle_constructs,
        "total_lines": content.count('\n') + 1,
    }


def extract_connections_from_mappings(all_mappings):
    """Infer connection info from sources/targets."""
    connections = {}
    for m in all_mappings:
        for src in m["sources"]:
            parts = src.split(".")
            if len(parts) >= 2:
                db_type = parts[0] if len(parts) == 3 else "Unknown"
                schema = parts[-2]
                key = f"{db_type}_{schema}"
                if key not in connections:
                    connections[key] = {
                        "name": f"CONN_{schema}",
                        "type": db_type if db_type != "Unknown" else "Inferred",
                        "schema": schema,
                    }
    return list(connections.values())


def build_dependency_dag(workflows):
    """Build a DAG from workflow dependencies."""
    dag = {"workflows": []}

    for wf in workflows:
        wf_dag = {
            "workflow": wf["name"],
            "nodes": [],
            "edges": [],
        }

        all_tasks = set()
        all_tasks.add("Start")
        for s in wf["sessions"]:
            all_tasks.add(s)
        for ti in wf["task_instances"]:
            all_tasks.add(ti["name"])
        for dt in wf.get("decision_tasks", []):
            all_tasks.add(dt)
        for et in wf.get("email_tasks", []):
            all_tasks.add(et)

        wf_dag["nodes"] = sorted(all_tasks)

        for link in wf["links"]:
            wf_dag["edges"].append({
                "from": link["from"],
                "to": link["to"],
                "condition": link.get("condition", ""),
            })

        dag["workflows"].append(wf_dag)

    return dag


def write_inventory_json(all_mappings, all_workflows, connections, sql_files):
    """Write the inventory.json output."""
    mappings_out = []
    for m in all_mappings:
        mappings_out.append({
            "name": m["name"],
            "sources": m["sources"],
            "targets": m["targets"],
            "transformations": m["transformations"],
            "has_sql_override": m["has_sql_override"],
            "has_stored_proc": m["has_stored_proc"],
            "complexity": m["complexity"],
            "sql_overrides": m["sql_overrides"],
            "lookup_conditions": m["lookup_conditions"],
            "parameters": m["parameters"],
            "target_load_order": m["target_load_order"],
        })

    workflows_out = []
    for wf in all_workflows:
        workflows_out.append({
            "name": wf["name"],
            "sessions": wf["sessions"],
            "session_to_mapping": wf["session_to_mapping"],
            "dependencies": wf["dependencies"],
            "has_timer": wf["has_timer"],
            "has_decision": wf["has_decision"],
            "decision_tasks": wf["decision_tasks"],
            "email_tasks": wf["email_tasks"],
            "pre_post_sql": wf.get("pre_post_sql", []),
            "schedule": wf["schedule"],
        })

    inventory = {
        "generated": "2026-03-23",
        "source_platform": "Informatica PowerCenter",
        "target_platform": "Microsoft Fabric",
        "mappings": mappings_out,
        "workflows": workflows_out,
        "connections": connections,
        "sql_files": sql_files,
        "summary": {
            "total_mappings": len(mappings_out),
            "total_workflows": len(workflows_out),
            "total_sessions": sum(len(wf["sessions"]) for wf in workflows_out),
            "total_sql_files": len(sql_files),
            "complexity_breakdown": {
                "Simple": sum(1 for m in mappings_out if m["complexity"] == "Simple"),
                "Medium": sum(1 for m in mappings_out if m["complexity"] == "Medium"),
                "Complex": sum(1 for m in mappings_out if m["complexity"] == "Complex"),
                "Custom": sum(1 for m in mappings_out if m["complexity"] == "Custom"),
            },
        },
    }

    out_path = OUTPUT_DIR / "inventory.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(inventory, f, indent=2, ensure_ascii=False)
    print(f"  Written: {out_path}")
    return inventory


def write_complexity_report(inventory):
    """Write complexity_report.md."""
    summary = inventory["summary"]
    cb = summary["complexity_breakdown"]

    lines = [
        "# Informatica-to-Fabric Migration -- Complexity Report",
        "",
        f"**Generated:** {inventory['generated']}",
        f"**Source Platform:** {inventory['source_platform']}",
        f"**Target Platform:** {inventory['target_platform']}",
        "",
        "---",
        "",
        "## Summary",
        "",
        "| Metric | Count |",
        "|--------|-------|",
        f"| Total Mappings | {summary['total_mappings']} |",
        f"| Total Workflows | {summary['total_workflows']} |",
        f"| Total Sessions | {summary['total_sessions']} |",
        f"| SQL Files Analyzed | {summary['total_sql_files']} |",
        "",
        "---",
        "",
        "## Complexity Breakdown",
        "",
        "| Complexity | Count | % | Migration Approach |",
        "|------------|-------|---|-------------------|",
    ]

    total = summary["total_mappings"] or 1
    approaches = {
        "Simple": "Auto-generate PySpark notebooks",
        "Medium": "Semi-automated with manual review",
        "Complex": "Manual assist required",
        "Custom": "Redesign required",
    }
    for level in ["Simple", "Medium", "Complex", "Custom"]:
        count = cb.get(level, 0)
        pct = f"{count/total*100:.0f}%"
        lines.append(f"| **{level}** | {count} | {pct} | {approaches[level]} |")

    lines += [
        "",
        "---",
        "",
        "## Mapping Details",
        "",
        "| Mapping | Complexity | Sources | Targets | Transformations | SQL Override | Stored Proc |",
        "|---------|------------|---------|---------|-----------------|-------------|-------------|",
    ]

    for m in inventory["mappings"]:
        tx = " -> ".join(m["transformations"])
        lines.append(
            f"| {m['name']} | **{m['complexity']}** | {', '.join(m['sources'])} | {', '.join(m['targets'])} | {tx} | {'Yes' if m['has_sql_override'] else 'No'} | {'Yes' if m['has_stored_proc'] else 'No'} |"
        )

    lines += ["", "---", "", "## SQL Overrides Found", ""]

    any_overrides = False
    for m in inventory["mappings"]:
        if m["sql_overrides"]:
            any_overrides = True
            lines.append(f"### {m['name']}")
            for ovr in m["sql_overrides"]:
                lines.append(f"- **{ovr['type']}:**")
                lines.append(f"  ```sql")
                lines.append(f"  {ovr['value']}")
                lines.append(f"  ```")
            lines.append("")

    if not any_overrides:
        lines.append("_No SQL overrides detected in mapping XML._")
        lines.append("")

    # SQL Files
    if inventory["sql_files"]:
        lines += ["", "---", "", "## Oracle SQL Files Analysis", ""]
        for sf in inventory["sql_files"]:
            lines.append(f"### {sf['file']}")
            lines.append(f"- **Path:** `{sf['path']}`")
            lines.append(f"- **Total lines:** {sf['total_lines']}")
            lines.append(f"- **Oracle-specific constructs:**")
            lines.append("")
            if sf["oracle_constructs"]:
                lines.append("| Construct | Occurrences | Lines |")
                lines.append("|-----------|-------------|-------|")
                for oc in sf["oracle_constructs"]:
                    line_refs = ", ".join(str(l) for l in oc["lines"][:10])
                    if len(oc["lines"]) > 10:
                        line_refs += "..."
                    lines.append(f"| `{oc['construct']}` | {oc['occurrences']} | {line_refs} |")
            else:
                lines.append("_No Oracle-specific constructs detected._")
            lines.append("")

    # Warnings
    if warnings:
        lines += ["", "---", "", "## Warnings", ""]
        for w in warnings:
            lines.append(f"- {w}")
        lines.append("")

    # Recommendations
    lines += [
        "",
        "---",
        "",
        "## Migration Recommendations",
        "",
        "1. **Simple mappings** -- Auto-generate using the notebook-migration agent. Expect minimal manual intervention.",
        "2. **Medium mappings** -- Use notebook-migration with manual review of LKP/AGG/JNR logic. Validate join conditions.",
        "3. **Complex mappings** -- Hand off SQL overrides to sql-migration agent first, then generate notebooks.",
        "4. **Custom mappings** -- Require full redesign. Java transformations must be rewritten in PySpark.",
        "5. **Oracle SQL files** -- Hand off to sql-migration for MERGE, DECODE, NVL, etc.",
        "6. **Workflow orchestration** -- Hand off to pipeline-migration for Fabric Pipeline JSON generation.",
        "",
    ]

    out_path = OUTPUT_DIR / "complexity_report.md"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"  Written: {out_path}")


def write_dependency_dag(dag):
    """Write dependency_dag.json."""
    out_path = OUTPUT_DIR / "dependency_dag.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(dag, f, indent=2, ensure_ascii=False)
    print(f"  Written: {out_path}")


def main():
    print("=" * 60)
    print("  Informatica-to-Fabric Assessment")
    print("=" * 60)
    print()

    # 1. Parse Mappings
    print("[1/6] Parsing mapping XML files...")
    mapping_dir = INPUT_DIR / "mappings"
    all_mappings = []
    if mapping_dir.exists():
        for xml_file in sorted(mapping_dir.glob("*.xml")):
            print(f"  Parsing: {xml_file.name}")
            try:
                mappings = parse_mapping_xml(xml_file)
                all_mappings.extend(mappings)
                for m in mappings:
                    print(f"    -> {m['name']} ({m['complexity']}) -- {len(m['transformations'])} transformations")
            except Exception as e:
                warnings.append(f"Error processing {xml_file.name}: {e}")
                print(f"    ERROR: {e}")
    print(f"  Total mappings found: {len(all_mappings)}")
    print()

    # 2. Parse Workflows
    print("[2/6] Parsing workflow XML files...")
    workflow_dir = INPUT_DIR / "workflows"
    all_workflows = []
    if workflow_dir.exists():
        for xml_file in sorted(workflow_dir.glob("*.xml")):
            print(f"  Parsing: {xml_file.name}")
            try:
                workflows = parse_workflow_xml(xml_file)
                all_workflows.extend(workflows)
                for wf in workflows:
                    print(f"    -> {wf['name']} -- {len(wf['sessions'])} sessions, {len(wf['links'])} links")
            except Exception as e:
                warnings.append(f"Error processing {xml_file.name}: {e}")
                print(f"    ERROR: {e}")
    print(f"  Total workflows found: {len(all_workflows)}")
    print()

    # 3. Parse SQL files
    print("[3/6] Parsing SQL files...")
    sql_dir = INPUT_DIR / "sql"
    sql_files = []
    if sql_dir.exists():
        for sql_file in sorted(sql_dir.glob("*.sql")):
            print(f"  Parsing: {sql_file.name}")
            analysis = parse_sql_file(sql_file)
            sql_files.append(analysis)
            constructs = [oc["construct"] for oc in analysis["oracle_constructs"]]
            print(f"    -> Oracle constructs: {', '.join(constructs) if constructs else 'none'}")
    print(f"  Total SQL files analyzed: {len(sql_files)}")
    print()

    # 4. Extract connections
    print("[4/6] Inferring connections...")
    connections = extract_connections_from_mappings(all_mappings)
    print(f"  Connections inferred: {len(connections)}")
    print()

    # 5. Write outputs
    print("[5/6] Writing output files...")
    inventory = write_inventory_json(all_mappings, all_workflows, connections, sql_files)
    write_complexity_report(inventory)
    dag = build_dependency_dag(all_workflows)
    write_dependency_dag(dag)
    print()

    # 6. Summary
    print("[6/6] Assessment complete!")
    print()
    print("=" * 60)
    cb = inventory["summary"]["complexity_breakdown"]
    print(f"  Mappings:     {inventory['summary']['total_mappings']}")
    print(f"  Workflows:    {inventory['summary']['total_workflows']}")
    print(f"  Sessions:     {inventory['summary']['total_sessions']}")
    print(f"  SQL files:    {inventory['summary']['total_sql_files']}")
    print(f"  Connections:  {len(connections)}")
    print()
    print("  Complexity breakdown:")
    for level in ["Simple", "Medium", "Complex", "Custom"]:
        print(f"    {level:8s}: {cb.get(level, 0)}")
    print()

    total_overrides = sum(len(m["sql_overrides"]) for m in all_mappings)
    total_oracle = sum(len(sf["oracle_constructs"]) for sf in sql_files)
    print(f"  SQL overrides in mappings: {total_overrides}")
    print(f"  Oracle constructs in SQL files: {total_oracle}")

    if warnings:
        print()
        print("  WARNINGS:")
        for w in warnings:
            print(f"    - {w}")

    if issues:
        print()
        print(f"  ISSUES REQUIRING ATTENTION: {len(issues)}")
        for i in issues:
            print(f"    [{i['severity']}] {i['type']}: {i.get('detail', '')}")

        # Write structured issues file for migration_issues.md generation
        issues_path = OUTPUT_DIR / "parse_issues.json"
        with open(issues_path, "w", encoding="utf-8") as f:
            json.dump(issues, f, indent=2, ensure_ascii=False)
        print(f"  Written: {issues_path}")

    print()
    print("=" * 60)
    print("  Outputs written to: output/inventory/")
    print("    - inventory.json")
    print("    - complexity_report.md")
    print("    - dependency_dag.json")
    if issues:
        print("    - parse_issues.json")
    print("=" * 60)

    # Return non-zero exit code if there were errors (not just warnings)
    error_count = sum(1 for i in issues if i["severity"] == "ERROR")
    if error_count:
        print(f"\n  Completed with {error_count} error(s). Partial results saved.")
        sys.exit(1)


if __name__ == "__main__":
    main()
