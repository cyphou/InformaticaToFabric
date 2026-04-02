"""
Informatica-to-Fabric Assessment Agent
Parses all XML mappings, workflows, sessions, and SQL files.
Produces: inventory.json, complexity_report.md, dependency_dag.json
"""

import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
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
    # Sprint 6 additions
    "Mapplet": "MPLT",
    "SQL Transformation": "SQLT",
    "Data Masking": "DM",
    "External Procedure": "EP",
    "Advanced External Procedure": "AEP",
    "Web Service Consumer": "WSC",
    "Association": "ASSOC",
    "Key Generator": "KEYGEN",
    "Address Validator": "ADDRVAL",
    # Sprint 22 additions
    "Data Quality": "DQ",
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
    # Sprint 6: Oracle analytic / window functions
    "LEAD": r"\bLEAD\s*\(",
    "LAG": r"\bLAG\s*\(",
    "DENSE_RANK": r"\bDENSE_RANK\s*\(",
    "NTILE": r"\bNTILE\s*\(",
    "FIRST_VALUE": r"\bFIRST_VALUE\s*\(",
    "LAST_VALUE": r"\bLAST_VALUE\s*\(",
    "ROW_NUMBER": r"\bROW_NUMBER\s*\(",
    "OVER": r"\bOVER\s*\(",
    "PARTITION BY": r"\bPARTITION\s+BY\b",
    # Sprint 7: Global temp tables, materialized views, DB links
    "GLOBAL TEMPORARY TABLE": r"\bGLOBAL\s+TEMPORARY\s+TABLE\b",
    "MATERIALIZED VIEW": r"\bMATERIALIZED\s+VIEW\b",
    "DB_LINK": r"@\w+",
    # Sprint 31: Oracle Object Types
    "CREATE TYPE": r"\bCREATE\s+(?:OR\s+REPLACE\s+)?TYPE\b",
    "OBJECT TYPE": r"\bAS\s+OBJECT\b",
}

# SQL Server-specific patterns (Sprint 7.3)
SQLSERVER_PATTERNS = {
    "GETDATE": r"\bGETDATE\s*\(",
    "ISNULL": r"\bISNULL\s*\(",
    "CHARINDEX": r"\bCHARINDEX\s*\(",
    "LEN": r"\bLEN\s*\(",
    "CONVERT": r"\bCONVERT\s*\(",
    "CAST": r"\bCAST\s*\(",
    "TOP": r"\bSELECT\s+TOP\b",
    "IDENTITY": r"\bIDENTITY\s*\(",
    "NOLOCK": r"\(NOLOCK\)",
    "@@": r"@@\w+",
    "NVARCHAR": r"\bNVARCHAR\b",
    "DATETIME": r"\bDATETIME\b",
    "BIT": r"\bBIT\b",
    "EXEC/EXECUTE": r"\bEXEC(?:UTE)?\s+",
    "CTE": r"\bWITH\s+\w+\s+AS\s*\(",
    "CROSS APPLY": r"\bCROSS\s+APPLY\b",
    "OUTER APPLY": r"\bOUTER\s+APPLY\b",
    "STRING_AGG": r"\bSTRING_AGG\s*\(",
}

# Teradata-specific patterns (Sprint 23)
TERADATA_PATTERNS = {
    "QUALIFY": r"\bQUALIFY\b",
    "SAMPLE": r"\bSAMPLE\s+\d+",
    "SEL": r"(?:^|\n)\s*SEL\s+",
    "COLLECT STATISTICS": r"\bCOLLECT\s+STAT(?:ISTIC)?S?\b",
    "VOLATILE TABLE": r"\bVOLATILE\s+TABLE\b",
    "MULTISET TABLE": r"\bMULTISET\s+TABLE\b",
    "SET TABLE": r"\bSET\s+TABLE\b",
    ".DATE": r"\bDATE\s*'",
    "FORMAT": r"\bFORMAT\s+'[^']*'",
    "CASESPECIFIC": r"\bCASESPECIFIC\b",
    "TERADATA_CAST": r"\bCAST\s*\([^)]+AS\s+FORMAT\b",
    "HASH": r"\bHASHROW\b|\bHASHBUCKET\b",
    "ZEROIFNULL": r"\bZEROIFNULL\s*\(",
    "NULLIFZERO": r"\bNULLIFZERO\s*\(",
    "TITLE": r"\bTITLE\s+'[^']*'",
}

# DB2-specific patterns (Sprint 23)
DB2_PATTERNS = {
    "FETCH FIRST": r"\bFETCH\s+FIRST\s+\d+\s+ROWS?\s+ONLY\b",
    "VALUE": r"\bVALUE\s*\(",
    "CURRENT DATE": r"\bCURRENT\s+DATE\b",
    "CURRENT TIMESTAMP": r"\bCURRENT\s+TIMESTAMP\b",
    "RRN": r"\bRRN\s*\(",
    "DECIMAL": r"\bDECIMAL\s*\(\d+,\s*\d+\)",
    "WITH UR": r"\bWITH\s+UR\b",
    "CONCAT": r"\bCONCAT\s*\(",
    "DAYOFWEEK": r"\bDAYOFWEEK\s*\(",
    "DAYS": r"\bDAYS\s*\(",
}

# MySQL-specific patterns (Sprint 23)
MYSQL_PATTERNS = {
    "IFNULL": r"\bIFNULL\s*\(",
    "NOW": r"\bNOW\s*\(\s*\)",
    "LIMIT": r"\bLIMIT\s+\d+",
    "AUTO_INCREMENT": r"\bAUTO_INCREMENT\b",
    "BACKTICK": r"`\w+`",
    "GROUP_CONCAT": r"\bGROUP_CONCAT\s*\(",
    "DATE_FORMAT": r"\bDATE_FORMAT\s*\(",
    "STR_TO_DATE": r"\bSTR_TO_DATE\s*\(",
    "DATEDIFF": r"\bDATEDIFF\s*\(",
    "UNSIGNED": r"\bUNSIGNED\b",
}

# PostgreSQL-specific patterns (Sprint 23)
POSTGRESQL_PATTERNS = {
    "DOUBLE_COLON_CAST": r"::\w+",
    "SERIAL": r"\bSERIAL\b|\bBIGSERIAL\b",
    "ILIKE": r"\bILIKE\b",
    "TEXT": r"\bTEXT\b",
    "BOOLEAN": r"\bBOOLEAN\b",
    "RETURNING": r"\bRETURNING\b",
    "ARRAY_AGG": r"\bARRAY_AGG\s*\(",
    "STRING_TO_ARRAY": r"\bSTRING_TO_ARRAY\s*\(",
    "GENERATE_SERIES": r"\bGENERATE_SERIES\s*\(",
    "PG_CATALOG": r"\bPG_CATALOG\b",
}

# Sprint 39: PII detection patterns (column/field name heuristics)
PII_COLUMN_PATTERNS = {
    "SSN": r"(?i)\b(ssn|social_security|soc_sec)\b",
    "EMAIL": r"(?i)\b(email|e_mail|email_addr)\b",
    "PHONE": r"(?i)\b(phone|mobile|cell|fax|tel)\b",
    "ADDRESS": r"(?i)\b(address|addr|street|city|zip|postal)\b",
    "NAME": r"(?i)\b(first_name|last_name|full_name|fname|lname|surname)\b",
    "DOB": r"(?i)\b(dob|date_of_birth|birth_date|birthdate)\b",
    "CREDIT_CARD": r"(?i)\b(credit_card|card_num|cc_num|card_number)\b",
    "PASSPORT": r"(?i)\b(passport|passport_no|passport_num)\b",
    "IP_ADDRESS": r"(?i)\b(ip_addr|ip_address|client_ip|source_ip)\b",
    "NATIONAL_ID": r"(?i)\b(national_id|id_number|citizen_id|tax_id|tin)\b",
}

# Sprint 39: Sensitivity classification levels
SENSITIVITY_LEVELS = {
    "SSN": "Highly Confidential",
    "CREDIT_CARD": "Highly Confidential",
    "PASSPORT": "Highly Confidential",
    "NATIONAL_ID": "Highly Confidential",
    "DOB": "Confidential",
    "EMAIL": "Confidential",
    "PHONE": "Confidential",
    "NAME": "Internal",
    "ADDRESS": "Internal",
    "IP_ADDRESS": "Internal",
}


def detect_pii_columns(mappings):
    """Scan mapping field names for PII patterns. Returns list of PII findings."""
    pii_findings = []
    compiled = {cat: re.compile(pat) for cat, pat in PII_COLUMN_PATTERNS.items()}
    for m in mappings:
        # Check sources, targets, and field-level lineage
        all_fields = []
        all_fields.extend(m.get("sources", []))
        all_fields.extend(m.get("targets", []))
        for lineage in m.get("field_lineage", []):
            all_fields.append(lineage.get("source_field", ""))
            all_fields.append(lineage.get("target_field", ""))

        for field in all_fields:
            for category, pattern in compiled.items():
                if pattern.search(field):
                    pii_findings.append({
                        "mapping": m["name"],
                        "field": field,
                        "pii_category": category,
                        "sensitivity": SENSITIVITY_LEVELS.get(category, "Internal"),
                    })
    return pii_findings


def extract_dq_rules(mappings):
    """Extract data quality rules from filter, expression, and DQ transformations."""
    dq_rules = []
    for m in mappings:
        for tx in m.get("transformations", []):
            if tx in ("FIL", "DQ"):
                dq_rules.append({
                    "mapping": m["name"],
                    "type": "filter" if tx == "FIL" else "data_quality",
                    "description": f"{tx} transformation in {m['name']}",
                })
        # Check for NOT NULL / CHECK constraints in SQL overrides
        for ovr in m.get("sql_overrides", []):
            sql = ovr.get("value", "")
            if re.search(r"\bIS\s+NOT\s+NULL\b", sql, re.IGNORECASE):
                dq_rules.append({
                    "mapping": m["name"],
                    "type": "not_null_check",
                    "description": f"NOT NULL constraint in SQL override",
                })
            if re.search(r"\bCHECK\s*\(", sql, re.IGNORECASE):
                dq_rules.append({
                    "mapping": m["name"],
                    "type": "check_constraint",
                    "description": f"CHECK constraint in SQL override",
                })
    return dq_rules

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
    Uses a restricted parser to mitigate XXE and entity expansion attacks.
    """
    # Use a parser that disables external entities and DTD processing
    def _make_parser():
        parser = ET.XMLParser()
        return parser

    # Try standard parse first
    try:
        tree = ET.parse(filepath, parser=_make_parser())
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

        # Sprint 25: Field-level lineage
        field_lineage = extract_field_lineage(mapping_el)

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
            "field_lineage": field_lineage,
        }

        # Sprint 25: Conversion score & effort estimate (needs mapping_info to exist)
        mapping_info["conversion_score"] = calculate_conversion_score(mapping_info)
        mapping_info["manual_effort_hours"] = estimate_manual_effort(mapping_info)
        mapping_info["lineage_summary"] = f"{len(field_lineage)} field paths traced"

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


# =====================================================================
# Sprint 25: Lineage & Conversion Scoring
# =====================================================================

# Transformations with fully-automated PySpark conversion rules
AUTO_CONVERTIBLE_TX = {
    "SQ", "EXP", "FIL", "AGG", "JNR", "LKP", "RTR", "UPD",
    "RNK", "SRT", "UNI", "NRM", "SEQ", "SP", "SQLT", "DM",
    "WSC", "MPLT",
}
# Placeholder-only (no real conversion logic yet)
PLACEHOLDER_TX = {"JTX", "CT", "HTTP", "XMLG", "XMLP", "TC", "ULKP", "DQ"}
# Infrastructure types excluded from scoring (not actual transforms)
_INFRA_TX = {"TGT", "SRC", "target", "source", "reader", "writer"}
# XML tag names that leak into transformation lists (not real transforms)
_NOISE_TX = {"port", "group", "PORT", "GROUP", "field", "FIELD"}


def extract_field_lineage(mapping_el):
    """Extract field-level lineage from CONNECTOR elements.

    Returns a list of lineage entries:
      [{"source_field": ..., "source_instance": ..., "target_field": ...,
        "target_instance": ..., "transformations": [...]}]
    """
    # Build connector graph: (from_instance, from_field) -> [(to_instance, to_field)]
    graph = {}
    # Also track all outgoing edges per instance (for field-name changes)
    instance_outgoing = {}  # instance -> [(to_instance, to_field)]
    for conn in mapping_el.iter("CONNECTOR"):
        src = (conn.get("FROMINSTANCE", ""), conn.get("FROMFIELD", ""))
        dst = (conn.get("TOINSTANCE", ""), conn.get("TOFIELD", ""))
        from_type = conn.get("FROMINSTANCETYPE", "")
        to_type = conn.get("TOINSTANCETYPE", "")
        graph.setdefault(src, []).append((dst, from_type, to_type))
        instance_outgoing.setdefault(src[0], []).append(dst)

    # Build instance type map from CONNECTOR metadata
    instance_type = {}
    for conn in mapping_el.iter("CONNECTOR"):
        fi = conn.get("FROMINSTANCE", "")
        ft = conn.get("FROMINSTANCETYPE", "")
        ti = conn.get("TOINSTANCE", "")
        tt = conn.get("TOINSTANCETYPE", "")
        if fi and ft:
            instance_type[fi] = ft
        if ti and tt:
            instance_type[ti] = tt

    # Identify source (SQ) instances and target instances
    sq_instances = {name for name, typ in instance_type.items()
                    if typ == "Source Qualifier"}
    tgt_instances = {name for name, typ in instance_type.items()
                     if typ == "Target Definition"}

    lineage = []

    # Trace every field from source qualifier nodes through the graph to targets
    for (inst, field), destinations in graph.items():
        if inst not in sq_instances:
            continue
        # BFS from this source field to find all target field arrivals
        queue = [(inst, field, [inst])]
        visited = set()
        while queue:
            cur_inst, cur_field, path = queue.pop(0)
            key = (cur_inst, cur_field)
            if key in visited:
                continue
            visited.add(key)

            if cur_inst in tgt_instances:
                # Build transformation list (instances between source and target)
                transformations = []
                for p in path[1:]:  # skip source
                    if p not in tgt_instances and p in instance_type:
                        transformations.append({
                            "instance": p,
                            "type": instance_type.get(p, "Unknown"),
                        })
                lineage.append({
                    "source_field": field,
                    "source_instance": inst,
                    "target_field": cur_field,
                    "target_instance": cur_inst,
                    "transformations": transformations,
                })
                continue

            # Follow graph edges from current position (exact field match)
            for (next_inst, next_field), _ft, _tt in graph.get(key, []):
                queue.append((next_inst, next_field, path + [next_inst]))

            # Also follow all outgoing edges from this instance (field-name changes)
            # This handles transformations that rename fields (e.g., ID_IN -> ID_OUT)
            if cur_inst not in sq_instances and cur_inst not in tgt_instances:
                for (next_inst, next_field) in instance_outgoing.get(cur_inst, []):
                    if (cur_inst, next_field) not in visited:
                        queue.append((next_inst, next_field, path + [next_inst]))

    return lineage


def calculate_conversion_score(mapping):
    """Calculate a conversion quality score (0-100) for a mapping.

    Scoring factors:
    - % of transformations with auto-conversion rules (50% weight)
    - % of SQL overrides that are auto-convertible (30% weight)
    - Absence of placeholder/gap indicators (20% weight)
    """
    tx_list = mapping.get("transformations", [])
    sql_overrides = mapping.get("sql_overrides", [])

    # Normalize: uppercase, filter XML noise tags and infrastructure types
    tx_list = [t.upper() for t in tx_list if t.lower() not in _NOISE_TX]
    tx_list = [t for t in tx_list if t not in _INFRA_TX]

    # Factor 1: Transformation coverage (50%)
    if tx_list:
        auto_count = sum(1 for t in tx_list if t in AUTO_CONVERTIBLE_TX)
        tx_score = (auto_count / len(tx_list)) * 100
    else:
        tx_score = 100  # No transformations = nothing to convert

    # Factor 2: SQL override convertibility (30%)
    if sql_overrides:
        convertible = 0
        for ovr in sql_overrides:
            val = ovr.get("value", "")
            # Check if the SQL uses only known auto-convertible patterns
            has_unsupported = bool(re.search(
                r'\bCONNECT\s+BY\b|\bPRAGMA\b|\bCURSOR\b|\bBULK\s+COLLECT\b|'
                r'\bFORALL\b|\bPACKAGE\s+BODY\b|\bDBMS_\w+|\bUTL_\w+',
                val, re.IGNORECASE
            ))
            if not has_unsupported:
                convertible += 1
        sql_score = (convertible / len(sql_overrides)) * 100
    else:
        sql_score = 100  # No SQL overrides = no conversion needed

    # Factor 3: Placeholder gap penalty (20%)
    placeholder_count = sum(1 for t in tx_list if t in PLACEHOLDER_TX)
    if tx_list:
        gap_score = ((len(tx_list) - placeholder_count) / len(tx_list)) * 100
    else:
        gap_score = 100

    score = round(tx_score * 0.5 + sql_score * 0.3 + gap_score * 0.2)
    return min(100, max(0, score))


def estimate_manual_effort(mapping):
    """Estimate manual effort in hours based on complexity and gaps."""
    complexity = mapping.get("complexity", "Simple")
    tx_list = mapping.get("transformations", [])
    sql_overrides = mapping.get("sql_overrides", [])

    base_hours = {"Simple": 0.5, "Medium": 2, "Complex": 4, "Custom": 8}
    hours = base_hours.get(complexity, 2)

    # Add per-placeholder gap
    placeholder_count = sum(1 for t in tx_list if t in PLACEHOLDER_TX)
    hours += placeholder_count * 2

    # Add for complex SQL overrides
    for ovr in sql_overrides:
        val = ovr.get("value", "")
        if re.search(r'\bCONNECT\s+BY\b|\bPACKAGE\s+BODY\b|\bCURSOR\b', val, re.IGNORECASE):
            hours += 3
        elif re.search(r'\bMERGE\b|\bDECODE\b', val, re.IGNORECASE):
            hours += 0.5

    return round(hours, 1)


def generate_lineage_mermaid(mapping_name, lineage_entries):
    """Generate a Mermaid flowchart for a mapping's field-level lineage."""
    if not lineage_entries:
        return ""

    # Collect unique instances in order and their types
    instance_order = []
    instance_types = {}
    seen = set()
    for entry in lineage_entries:
        src = entry["source_instance"]
        tgt = entry["target_instance"]
        if src not in seen:
            instance_order.append(src)
            seen.add(src)
            instance_types[src] = "Source Qualifier"
        for tx in entry.get("transformations", []):
            inst = tx["instance"]
            if inst not in seen:
                instance_order.append(inst)
                seen.add(inst)
                instance_types[inst] = tx.get("type", "Unknown")
        if tgt not in seen:
            instance_order.append(tgt)
            seen.add(tgt)
            instance_types[tgt] = "Target"

    # Build unique edges
    edges = set()
    for entry in lineage_entries:
        chain = [entry["source_instance"]]
        for tx in entry.get("transformations", []):
            chain.append(tx["instance"])
        chain.append(entry["target_instance"])
        for i in range(len(chain) - 1):
            edges.add((chain[i], chain[i + 1]))

    # Sanitize node IDs for Mermaid (replace spaces/special chars)
    def node_id(name):
        return re.sub(r'[^A-Za-z0-9_]', '_', name)

    lines = [f"```mermaid", f"flowchart LR"]
    # Node definitions with labels
    for inst in instance_order:
        nid = node_id(inst)
        typ = instance_types.get(inst, "")
        if "Source" in typ:
            lines.append(f'    {nid}["{inst}<br/>📥 {typ}"]')
        elif "Target" in typ:
            lines.append(f'    {nid}["{inst}<br/>📤 Target"]')
        else:
            short = TRANSFORMATION_ABBREV.get(typ, typ)
            lines.append(f'    {nid}["{inst}<br/>⚙️ {short}"]')

    # Edges
    for src, dst in sorted(edges):
        lines.append(f"    {node_id(src)} --> {node_id(dst)}")

    lines.append("```")
    return "\n".join(lines)


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
        for ti in wf.get("task_instances", []):
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
            "conversion_score": m.get("conversion_score", 0),
            "manual_effort_hours": m.get("manual_effort_hours", 0),
            "lineage_summary": m.get("lineage_summary", ""),
            "field_lineage": m.get("field_lineage", []),
        })

    workflows_out = []
    for wf in all_workflows:
        schedule_str = wf["schedule"]
        schedule_cron = parse_scheduler_cron(schedule_str)
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
            "schedule": schedule_str,
            "schedule_cron": schedule_cron,
        })

    inventory = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
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
            "avg_conversion_score": round(
                sum(m.get("conversion_score", 0) for m in mappings_out) / max(len(mappings_out), 1), 1
            ),
            "total_manual_effort_hours": round(
                sum(m.get("manual_effort_hours", 0) for m in mappings_out), 1
            ),
            "total_field_lineage_paths": sum(
                len(m.get("field_lineage", [])) for m in mappings_out
            ),
        },
    }

    out_path = OUTPUT_DIR / "inventory.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(inventory, f, indent=2, ensure_ascii=False)
    print(f"  Written: {out_path}")
    return inventory


def write_lineage_json(all_mappings):
    """Write lineage.json with per-mapping field-level lineage and Mermaid diagrams."""
    lineage_data = []
    for m in all_mappings:
        entry = {
            "mapping": m["name"],
            "complexity": m.get("complexity", "Unknown"),
            "conversion_score": m.get("conversion_score", 0),
            "manual_effort_hours": m.get("manual_effort_hours", 0),
            "field_lineage": m.get("field_lineage", []),
            "mermaid_diagram": generate_lineage_mermaid(
                m["name"], m.get("field_lineage", [])
            ),
        }
        lineage_data.append(entry)

    out_path = OUTPUT_DIR / "lineage.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(lineage_data, f, indent=2, ensure_ascii=False)
    print(f"  Written: {out_path}")
    return lineage_data


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
        "## Conversion Readiness",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Average Conversion Score | {summary.get('avg_conversion_score', 0)}/100 |",
        f"| Total Manual Effort (est.) | {summary.get('total_manual_effort_hours', 0)} hours |",
        f"| Total Field Lineage Paths | {summary.get('total_field_lineage_paths', 0)} |",
        "",
        "---",
        "",
        "## Mapping Details",
        "",
        "| Mapping | Complexity | Score | Effort (h) | Sources | Targets | Transformations | SQL Override | Stored Proc |",
        "|---------|------------|-------|------------|---------|---------|-----------------|-------------|-------------|",
    ]

    for m in inventory["mappings"]:
        tx = " -> ".join(m["transformations"])
        score = m.get("conversion_score", 0)
        effort = m.get("manual_effort_hours", 0)
        lines.append(
            f"| {m['name']} | **{m['complexity']}** | {score}/100 | {effort} | {', '.join(m['sources'])} | {', '.join(m['targets'])} | {tx} | {'Yes' if m['has_sql_override'] else 'No'} | {'Yes' if m['has_stored_proc'] else 'No'} |"
        )

    lines += ["", "---", "", "## SQL Overrides Found", ""]

    any_overrides = False
    for m in inventory["mappings"]:
        if m["sql_overrides"]:
            any_overrides = True
            lines.append(f"### {m['name']}")
            for ovr in m["sql_overrides"]:
                lines.append(f"- **{ovr['type']}:**")
                lines.append("  ```sql")
                lines.append(f"  {ovr['value']}")
                lines.append("  ```")
            lines.append("")

    if not any_overrides:
        lines.append("_No SQL overrides detected in mapping XML._")
        lines.append("")

    # Field Lineage Diagrams (Complex/Custom mappings)
    complex_mappings = [m for m in inventory["mappings"]
                        if m.get("complexity") in ("Complex", "Custom")
                        and m.get("field_lineage")]
    if complex_mappings:
        lines += ["", "---", "", "## Field Lineage Diagrams (Complex/Custom)", ""]
        for m in complex_mappings:
            mermaid = generate_lineage_mermaid(m["name"], m["field_lineage"])
            if mermaid:
                lines.append(f"### {m['name']} (Score: {m.get('conversion_score', 0)}/100)")
                lines.append("")
                lines.append(mermaid)
                lines.append("")

    # SQL Files
    if inventory["sql_files"]:
        lines += ["", "---", "", "## Oracle SQL Files Analysis", ""]
        for sf in inventory["sql_files"]:
            lines.append(f"### {sf['file']}")
            lines.append(f"- **Path:** `{sf['path']}`")
            lines.append(f"- **Total lines:** {sf['total_lines']}")
            lines.append("- **Oracle-specific constructs:**")
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


# =====================================================================
# Sprint 28: Migration Wave Planner
# =====================================================================

def _build_mapping_dependency_graph(all_mappings, all_workflows):
    """Build a dependency graph between mappings based on workflow ordering.

    Returns: dict mapping_name -> set of mapping_names it depends on.
    """
    deps = {m["name"]: set() for m in all_mappings}

    for wf in all_workflows:
        stm = wf.get("session_to_mapping", {})
        edges = wf.get("links", [])

        # Build session ordering from workflow edges
        for edge in edges:
            from_session = edge.get("from", "")
            to_session = edge.get("to", "")
            from_mapping = stm.get(from_session, "")
            to_mapping = stm.get(to_session, "")
            if from_mapping and to_mapping and from_mapping != to_mapping:
                deps.setdefault(to_mapping, set()).add(from_mapping)

        # Also infer dependencies from shared tables (source of one is target of another)
    target_to_mapping = {}
    for m in all_mappings:
        for tgt in m.get("targets", []):
            target_to_mapping[tgt.lower()] = m["name"]

    for m in all_mappings:
        for src in m.get("sources", []):
            producer = target_to_mapping.get(src.lower(), "")
            if producer and producer != m["name"]:
                deps.setdefault(m["name"], set()).add(producer)

    return deps


def topological_sort_waves(deps):
    """Topological sort into waves of parallelizable mappings.

    Returns list of waves: [[mapping_name, ...], ...]
    Each wave contains mappings with no unresolved dependencies.
    """
    remaining = {k: set(v) for k, v in deps.items()}
    waves = []
    completed = set()

    while remaining:
        # Find all mappings with no pending dependencies
        ready = [m for m, d in remaining.items() if not (d - completed)]
        if not ready:
            # Cycle detected — break by picking the mapping with fewest deps
            ready = [min(remaining, key=lambda m: len(remaining[m] - completed))]

        waves.append(sorted(ready))
        completed.update(ready)
        for m in ready:
            remaining.pop(m, None)

    return waves


def find_critical_path(deps, effort_map):
    """Find the critical path (longest chain by estimated effort).

    Returns: (path_list, total_effort)
    """
    # Build adjacency list (who depends on me → successors)
    successors = {}
    for m, dep_set in deps.items():
        for d in dep_set:
            successors.setdefault(d, set()).add(m)
        successors.setdefault(m, set())

    # Dynamic programming: longest path from each node
    memo = {}

    def longest_from(node, visited):
        if node in memo:
            return memo[node]
        if node in visited:
            return ([node], effort_map.get(node, 1))
        visited_copy = visited | {node}
        best_path = [node]
        best_effort = effort_map.get(node, 1)
        for succ in successors.get(node, []):
            sub_path, sub_effort = longest_from(succ, visited_copy)
            total = effort_map.get(node, 1) + sub_effort
            if total > best_effort:
                best_effort = total
                best_path = [node] + sub_path
        memo[node] = (best_path, best_effort)
        return memo[node]

    # Find start nodes (no dependencies)
    start_nodes = [m for m, d in deps.items() if not d]
    if not start_nodes:
        start_nodes = list(deps.keys())[:1]

    best_overall_path = []
    best_overall_effort = 0
    for start in start_nodes:
        path, effort = longest_from(start, set())
        if effort > best_overall_effort:
            best_overall_effort = effort
            best_overall_path = path

    return best_overall_path, round(best_overall_effort, 1)


def generate_wave_plan(all_mappings, all_workflows):
    """Generate a full migration wave plan.

    Returns dict with waves, critical_path, stats.
    """
    deps = _build_mapping_dependency_graph(all_mappings, all_workflows)
    waves = topological_sort_waves(deps)

    effort_map = {m["name"]: m.get("manual_effort_hours", 1) for m in all_mappings}
    mapping_info = {m["name"]: m for m in all_mappings}

    wave_plan = []
    for i, wave_mappings in enumerate(waves, 1):
        wave = {
            "wave_number": i,
            "mappings": wave_mappings,
            "parallel_count": len(wave_mappings),
            "dependencies": [],
            "estimated_effort_hours": round(
                sum(effort_map.get(m, 1) for m in wave_mappings), 1
            ),
            "complexity_breakdown": {},
        }
        for m_name in wave_mappings:
            m_deps = deps.get(m_name, set())
            if m_deps:
                wave["dependencies"].append({
                    "mapping": m_name,
                    "depends_on": sorted(m_deps),
                })
            complexity = mapping_info.get(m_name, {}).get("complexity", "Unknown")
            wave["complexity_breakdown"][complexity] = wave["complexity_breakdown"].get(complexity, 0) + 1

        wave_plan.append(wave)

    critical_path, critical_effort = find_critical_path(deps, effort_map)

    return {
        "total_waves": len(wave_plan),
        "total_mappings": sum(len(w["mappings"]) for w in wave_plan),
        "total_effort_hours": round(sum(w["estimated_effort_hours"] for w in wave_plan), 1),
        "critical_path": critical_path,
        "critical_path_effort_hours": critical_effort,
        "waves": wave_plan,
    }


def write_wave_plan(wave_data):
    """Write wave_plan.json and wave_plan.md."""
    # JSON
    json_path = OUTPUT_DIR / "wave_plan.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(wave_data, f, indent=2, ensure_ascii=False)
    print(f"  Written: {json_path}")

    # Markdown
    lines = [
        "# Migration Wave Plan",
        "",
        f"**Total Waves:** {wave_data['total_waves']}",
        f"**Total Mappings:** {wave_data['total_mappings']}",
        f"**Total Effort:** {wave_data['total_effort_hours']} hours",
        f"**Critical Path:** {' → '.join(wave_data['critical_path'])} ({wave_data['critical_path_effort_hours']}h)",
        "",
        "---",
        "",
    ]

    for wave in wave_data["waves"]:
        lines.append(f"## Wave {wave['wave_number']} ({wave['parallel_count']} mappings, {wave['estimated_effort_hours']}h)")
        lines.append("")
        lines.append("| Mapping | Dependencies |")
        lines.append("|---------|-------------|")
        dep_map = {d["mapping"]: d["depends_on"] for d in wave["dependencies"]}
        for m in wave["mappings"]:
            deps_str = ", ".join(dep_map.get(m, [])) or "—"
            lines.append(f"| {m} | {deps_str} |")
        lines.append("")

    # Mermaid Gantt
    lines.extend(["---", "", "## Wave Timeline", "", "```mermaid", "gantt"])
    lines.append("    title Migration Waves")
    lines.append("    dateFormat X")
    lines.append("    axisFormat Wave %s")
    offset = 0
    for wave in wave_data["waves"]:
        lines.append(f"    section Wave {wave['wave_number']}")
        for m in wave["mappings"]:
            lines.append(f"    {m} : {offset}, {offset + 1}")
        offset += 1
    lines.extend(["```", ""])

    md_path = OUTPUT_DIR / "wave_plan.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"  Written: {md_path}")


# =====================================================================
# Sprint 6: Mapplet expansion
# =====================================================================

def parse_mapplets(root):
    """Parse all MAPPLET definitions from a root XML element.
    Returns dict: mapplet_name -> list of transformation dicts.
    """
    mapplets = {}
    for mplt_el in root.iter("MAPPLET"):
        name = mplt_el.get("NAME", "")
        if not name:
            continue
        txs = []
        for tx in mplt_el.iter("TRANSFORMATION"):
            tx_type = tx.get("TYPE", "Unknown")
            tx_name = tx.get("NAME", "")
            short = abbrev(tx_type)
            txs.append({"name": tx_name, "type": tx_type, "abbrev": short})
        mapplets[name] = txs
    return mapplets


def expand_mapplet_refs(mapping_info, mapplets_dict):
    """Expand Mapplet references in a mapping's transformation chain.
    Modifies mapping_info in place.
    """
    expanded_tx = []
    expanded_details = []
    has_mapplet = False

    for td in mapping_info.get("transformation_details", []):
        if td["abbrev"] == "MPLT":
            has_mapplet = True
            mplt_name = td["name"]
            # Try to resolve — MAPPLETNAME may be stored in the name
            resolved = mapplets_dict.get(mplt_name, [])
            if resolved:
                for inner_tx in resolved:
                    if inner_tx["abbrev"] not in expanded_tx:
                        expanded_tx.append(inner_tx["abbrev"])
                    expanded_details.append({
                        "name": f"{mplt_name}.{inner_tx['name']}",
                        "type": inner_tx["type"],
                        "abbrev": inner_tx["abbrev"],
                        "from_mapplet": mplt_name,
                    })
            else:
                # Unresolved mapplet — keep the MPLT reference and warn
                if "MPLT" not in expanded_tx:
                    expanded_tx.append("MPLT")
                expanded_details.append(td)
                warnings.append(f"Unresolved Mapplet reference: '{mplt_name}' in mapping '{mapping_info['name']}'")
                issues.append({
                    "type": "unresolved_mapplet",
                    "severity": "WARNING",
                    "detail": f"Mapplet '{mplt_name}' referenced but not found in parsed files",
                    "action": "Ensure the Mapplet XML file is in input/mappings/"
                })
        else:
            if td["abbrev"] not in expanded_tx:
                expanded_tx.append(td["abbrev"])
            expanded_details.append(td)

    if has_mapplet:
        mapping_info["transformations"] = expanded_tx
        mapping_info["transformation_details"] = expanded_details
        mapping_info["has_mapplet"] = True
    else:
        mapping_info["has_mapplet"] = False


# =====================================================================
# Sprint 6: Parameter file (.prm) parser
# =====================================================================

def parse_parameter_files(prm_dir):
    """Parse Informatica .prm parameter files.
    Returns list of dicts with section, key-value pairs.
    """
    param_files = []
    if not prm_dir.exists():
        return param_files

    for prm_file in sorted(prm_dir.glob("*.prm")):
        try:
            content = prm_file.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            warnings.append(f"Cannot read parameter file {prm_file.name}: {e}")
            continue

        sections = {}
        current_section = "Global"

        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Section header: [SectionName]
            section_match = re.match(r'^\[(.+)\]$', line)
            if section_match:
                current_section = section_match.group(1)
                continue
            # Key=Value pair
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip()
                if current_section not in sections:
                    sections[current_section] = {}
                sections[current_section][key] = value

        param_files.append({
            "file": prm_file.name,
            "path": str(prm_file.relative_to(WORKSPACE)),
            "sections": sections,
            "total_params": sum(len(v) for v in sections.values()),
        })
        print(f"  Parsing: {prm_file.name} ({sum(len(v) for v in sections.values())} params)")

    return param_files


# =====================================================================
# Sprint 7.1: IICS XML parser
# =====================================================================

def parse_iics_mapping(filepath):
    """Parse an IICS (Cloud) mapping export file.
    IICS uses a completely different XML schema than PowerCenter.
    """
    tree, root = safe_parse_xml(filepath)
    if root is None:
        return []

    # Detect namespace from root tag (IICS exports often have xmlns)
    ns = ""
    if "}" in root.tag:
        ns = root.tag.split("}")[0] + "}"

    dt_tag = f"{ns}dTemplate"

    mappings = []

    # IICS exports use dTemplate elements with objectType attributes
    for tmpl in root.iter(dt_tag):
        obj_type = tmpl.get("objectType", "")
        name = tmpl.get("name", "")

        if "mapping" not in obj_type.lower():
            continue

        # Extract transformations from nested dTemplate elements
        transformations = []
        tx_details = []
        sources = []
        targets = []

        for child in tmpl.iter(dt_tag):
            child_type = child.get("objectType", "").lower()
            child_name = child.get("name", "")

            if "source" in child_type or "reader" in child_type:
                sources.append(child_name)
            elif "target" in child_type or "writer" in child_type:
                targets.append(child_name)
            elif child_type and child != tmpl:
                # Map IICS object types to familiar abbreviations
                tx_type = child_type.split(".")[-1] if "." in child_type else child_type
                short = _iics_type_to_abbrev(tx_type)
                if short not in transformations:
                    transformations.append(short)
                tx_details.append({"name": child_name, "type": tx_type, "abbrev": short})

        if not transformations and not sources:
            continue

        complexity = classify_complexity(transformations, [], sources, targets, False, [])

        mappings.append({
            "name": name,
            "sources": sources,
            "targets": targets,
            "transformations": transformations,
            "transformation_details": tx_details,
            "has_sql_override": False,
            "has_stored_proc": False,
            "complexity": complexity,
            "sql_overrides": [],
            "lookup_conditions": [],
            "parameters": [],
            "target_load_order": [],
            "connector_count": 0,
            "has_mapplet": False,
            "format": "iics",
        })

    if not mappings:
        warnings.append(f"WARNING: No IICS mapping objects found in {filepath.name}")

    return mappings


def _iics_type_to_abbrev(iics_type):
    """Map common IICS transformation type names to our abbreviations."""
    iics_type_lower = iics_type.lower()
    mapping = {
        "source": "SQ", "reader": "SQ", "sourcetransformation": "SQ",
        "target": "TGT", "writer": "TGT", "targettransformation": "TGT",
        "expression": "EXP", "filter": "FIL", "aggregator": "AGG",
        "joiner": "JNR", "lookup": "LKP", "router": "RTR",
        "updatestrategy": "UPD", "sequencegenerator": "SEQ",
        "sorter": "SRT", "rank": "RNK", "union": "UNI",
        "normalizer": "NRM", "sqlparser": "SQLT", "sql": "SQLT",
        "java": "JTX", "custom": "CT", "http": "HTTP",
        "datamasking": "DM", "webserviceconsumer": "WSC",
    }
    for key, val in mapping.items():
        if key in iics_type_lower:
            return val
    return iics_type


# =====================================================================
# Sprint 19: IICS Taskflow, Sync/MassIngestion, Connection parsers
# =====================================================================

# IICS taskflow element type → Fabric activity category mapping
IICS_TASKFLOW_TYPE_MAP = {
    "mappingtask": "NotebookActivity",
    "commandtask": "ScriptActivity",
    "notificationtask": "WebActivity",
    "humantask": "WebActivity",
    "exclusivegateway": "IfCondition",
    "parallelgateway": "Parallel",
    "timerevent": "WaitActivity",
    "subflow": "ExecutePipeline",
}


def parse_iics_taskflow(filepath):
    """Parse an IICS Taskflow export file.
    Returns a list of workflow-like dicts compatible with the PowerCenter format.
    """
    tree, root = safe_parse_xml(filepath)
    if root is None:
        return []

    # IICS exports may use namespaces on the root but xmlns="" on children
    # We need to search for both namespaced and non-namespaced tags
    ns = ""
    if "}" in root.tag:
        ns = root.tag.split("}")[0] + "}"

    def find_all(tag):
        """Find elements by tag, trying both namespaced and non-namespaced."""
        results = list(root.iter(f"{ns}{tag}")) if ns else []
        results.extend(root.iter(tag))
        return results

    taskflows = []

    for tmpl in find_all("dTemplate"):
        obj_type = tmpl.get("objectType", "").lower()
        name = tmpl.get("name", "")

        if "taskflow" not in obj_type:
            continue

        sessions = []
        session_to_mapping = {}
        has_timer = False
        has_decision = False
        decision_tasks = []
        email_tasks = []
        pre_post_sql = []
        links = []
        dependencies = {}
        parameters = []

        for child in tmpl:
            child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag

            if child_tag == "dTemplate":
                child_type = child.get("objectType", "").lower()
                child_name = child.get("name", "")
                type_key = child_type.split(".")[-1] if "." in child_type else child_type

                # Extract attributes from child
                attrs = {}
                for attr_el in child:
                    attr_tag = attr_el.tag.split("}")[-1] if "}" in attr_el.tag else attr_el.tag
                    if attr_tag == "dAttribute":
                        attrs[attr_el.get("name", "")] = attr_el.get("value", "")

                if "mappingtask" in type_key:
                    sessions.append(child_name)
                    mapping_ref = attrs.get("mappingName", child_name)
                    session_to_mapping[child_name] = mapping_ref
                elif "commandtask" in type_key:
                    sessions.append(child_name)
                elif "exclusivegateway" in type_key:
                    has_decision = True
                    decision_tasks.append(child_name)
                elif "timerevent" in type_key:
                    has_timer = True
                elif "notificationtask" in type_key:
                    email_tasks.append(child_name)

            elif child_tag == "dLink":
                from_task = child.get("from", "")
                to_task = child.get("to", "")
                condition = child.get("condition", "")
                links.append({"from": from_task, "to": to_task, "condition": condition})
                if to_task not in dependencies:
                    dependencies[to_task] = []
                dependencies[to_task].append(from_task)

            elif child_tag == "dParameter":
                param_name = child.get("name", "")
                if param_name:
                    parameters.append(param_name)

        taskflows.append({
            "name": name,
            "sessions": sessions,
            "session_to_mapping": session_to_mapping,
            "dependencies": dependencies,
            "has_timer": has_timer,
            "has_decision": has_decision,
            "decision_tasks": decision_tasks,
            "email_tasks": email_tasks,
            "pre_post_sql": pre_post_sql,
            "links": links,
            "schedule": "",
            "format": "iics",
            "parameters": parameters,
        })

    return taskflows


def parse_iics_sync_tasks(filepath):
    """Parse IICS Synchronization Task objects from export XML.
    Returns a list of simplified mapping-like dicts for inventory.
    """
    tree, root = safe_parse_xml(filepath)
    if root is None:
        return []

    ns = ""
    if "}" in root.tag:
        ns = root.tag.split("}")[0] + "}"

    def find_all(tag):
        results = list(root.iter(f"{ns}{tag}")) if ns else []
        results.extend(root.iter(tag))
        return results

    tasks = []
    for tmpl in find_all("dTemplate"):
        obj_type = tmpl.get("objectType", "").lower()
        if "synctask" not in obj_type:
            continue
        name = tmpl.get("name", "")
        attrs = {}
        for child in tmpl:
            child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if child_tag == "dAttribute":
                attrs[child.get("name", "")] = child.get("value", "")

        source = attrs.get("sourceName", "")
        target = attrs.get("targetName", "")
        mapping_ref = attrs.get("mappingName", "")

        tasks.append({
            "name": name,
            "sources": [source] if source else [],
            "targets": [target] if target else [],
            "transformations": ["SQ", "TGT"],
            "has_sql_override": False,
            "has_stored_proc": False,
            "complexity": "Simple",
            "sql_overrides": [],
            "lookup_conditions": [],
            "parameters": [],
            "target_load_order": [],
            "connector_count": 0,
            "has_mapplet": False,
            "format": "iics",
            "iics_type": "SynchronizationTask",
            "mapping_ref": mapping_ref,
        })
    return tasks


def parse_iics_mass_ingestion(filepath):
    """Parse IICS Mass Ingestion Task objects from export XML.
    Returns a list of simplified mapping-like dicts for inventory.
    """
    tree, root = safe_parse_xml(filepath)
    if root is None:
        return []

    ns = ""
    if "}" in root.tag:
        ns = root.tag.split("}")[0] + "}"

    def find_all(tag):
        results = list(root.iter(f"{ns}{tag}")) if ns else []
        results.extend(root.iter(tag))
        return results

    tasks = []
    for tmpl in find_all("dTemplate"):
        obj_type = tmpl.get("objectType", "").lower()
        if "massingestion" not in obj_type:
            continue
        name = tmpl.get("name", "")
        attrs = {}
        for child in tmpl:
            child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if child_tag == "dAttribute":
                attrs[child.get("name", "")] = child.get("value", "")

        source = attrs.get("sourceName", "")
        target = attrs.get("targetName", "")
        file_pattern = attrs.get("filePattern", "")
        load_type = attrs.get("loadType", "full")

        tasks.append({
            "name": name,
            "sources": [source] if source else [],
            "targets": [target] if target else [],
            "transformations": ["SQ", "TGT"],
            "has_sql_override": False,
            "has_stored_proc": False,
            "complexity": "Simple",
            "sql_overrides": [],
            "lookup_conditions": [],
            "parameters": [],
            "target_load_order": [],
            "connector_count": 0,
            "has_mapplet": False,
            "format": "iics",
            "iics_type": "MassIngestion",
            "file_pattern": file_pattern,
            "load_type": load_type,
        })
    return tasks


def parse_iics_connections(filepath):
    """Parse IICS connection objects from export XML.
    Returns a list of connection dicts compatible with inventory format.
    """
    tree, root = safe_parse_xml(filepath)
    if root is None:
        return []

    ns = ""
    if "}" in root.tag:
        ns = root.tag.split("}")[0] + "}"

    def find_all(tag):
        results = list(root.iter(f"{ns}{tag}")) if ns else []
        results.extend(root.iter(tag))
        return results

    connections = []
    for tmpl in find_all("dTemplate"):
        obj_type = tmpl.get("objectType", "").lower()
        if "connection" not in obj_type:
            continue
        name = tmpl.get("name", "")
        attrs = {}
        for child in tmpl:
            child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if child_tag == "dAttribute":
                attrs[child.get("name", "")] = child.get("value", "")

        conn_type = attrs.get("type", "Unknown")
        connections.append({
            "name": name,
            "type": conn_type,
            "host": attrs.get("host", ""),
            "schema": attrs.get("schema", ""),
            "format": "iics",
        })
    return connections


# =====================================================================
# Sprint 22: IICS Data Quality Task + Application Integration parsers
# =====================================================================

def parse_iics_dq_tasks(filepath):
    """Parse IICS Data Quality Task objects from export XML.
    Returns a list of mapping-like dicts for inventory.
    """
    tree, root = safe_parse_xml(filepath)
    if root is None:
        return []

    ns = ""
    if "}" in root.tag:
        ns = root.tag.split("}")[0] + "}"

    def find_all(tag):
        results = list(root.iter(f"{ns}{tag}")) if ns else []
        results.extend(root.iter(tag))
        return results

    tasks = []
    for tmpl in find_all("dTemplate"):
        obj_type = tmpl.get("objectType", "").lower()
        if "dataqualitytask" not in obj_type and "dqtask" not in obj_type and "dataquality" not in obj_type:
            continue
        name = tmpl.get("name", "")
        attrs = {}
        sources = []
        targets = []
        for child in tmpl:
            child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if child_tag == "dAttribute":
                attrs[child.get("name", "")] = child.get("value", "")
            elif child_tag == "dTemplate":
                # Extract source/target connections from child fields
                for sub in child:
                    sub_tag = sub.tag.split("}")[-1] if "}" in sub.tag else sub.tag
                    if sub_tag == "dAttribute":
                        sub_name = sub.get("name", "")
                        sub_val = sub.get("value", "")
                        if sub_name == "sourceConnection" and sub_val:
                            sources.append(sub_val)
                        elif sub_name == "targetConnection" and sub_val:
                            targets.append(sub_val)

        source = attrs.get("sourceName", "")
        target = attrs.get("targetName", "")
        if source and source not in sources:
            sources.append(source)
        if target and target not in targets:
            targets.append(target)
        rule_set = attrs.get("ruleSet", attrs.get("ruleName", ""))
        profile_name = attrs.get("profileName", "")

        tasks.append({
            "name": name,
            "sources": sources,
            "targets": targets,
            "transformations": ["SQ", "DQ", "TGT"],
            "has_sql_override": False,
            "has_stored_proc": False,
            "complexity": "Complex",
            "sql_overrides": [],
            "lookup_conditions": [],
            "parameters": [],
            "target_load_order": [],
            "connector_count": 0,
            "has_mapplet": False,
            "format": "iics",
            "iics_type": "DataQualityTask",
            "rule_set": rule_set,
            "profile_name": profile_name,
        })
    return tasks


def parse_iics_app_integration(filepath):
    """Parse IICS Application Integration (event-driven) objects from export XML.
    Returns a list of workflow-like dicts for inventory.
    """
    tree, root = safe_parse_xml(filepath)
    if root is None:
        return []

    ns = ""
    if "}" in root.tag:
        ns = root.tag.split("}")[0] + "}"

    def find_all(tag):
        results = list(root.iter(f"{ns}{tag}")) if ns else []
        results.extend(root.iter(tag))
        return results

    integrations = []
    for tmpl in find_all("dTemplate"):
        obj_type = tmpl.get("objectType", "").lower()
        if "appintegration" not in obj_type and "applicationintegration" not in obj_type and "serviceconnector" not in obj_type:
            continue
        name = tmpl.get("name", "")
        attrs = {}
        steps = []
        links = []
        session_to_mapping = {}

        for child in tmpl:
            child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if child_tag == "dAttribute":
                attrs[child.get("name", "")] = child.get("value", "")
            elif child_tag == "dTemplate":
                child_type = child.get("objectType", "").lower()
                child_name = child.get("name", "")
                steps.append(child_name)
                # Extract mapping references from step attributes
                for sub in child:
                    sub_tag = sub.tag.split("}")[-1] if "}" in sub.tag else sub.tag
                    if sub_tag == "dAttribute":
                        sub_name = sub.get("name", "")
                        sub_val = sub.get("value", "")
                        if sub_name == "mappingName" and sub_val:
                            session_to_mapping[child_name] = sub_val
            elif child_tag == "dLink":
                from_task = child.get("from", "")
                to_task = child.get("to", "")
                links.append({"from": from_task, "to": to_task, "condition": ""})

        event_type = attrs.get("eventType", attrs.get("triggerType", ""))
        service_url = attrs.get("serviceUrl", attrs.get("endpoint", ""))

        dependencies = {}
        for link in links:
            to_task = link["to"]
            if to_task not in dependencies:
                dependencies[to_task] = []
            dependencies[to_task].append(link["from"])

        integrations.append({
            "name": name,
            "sessions": steps,
            "session_to_mapping": session_to_mapping,
            "dependencies": dependencies,
            "has_timer": False,
            "has_decision": False,
            "decision_tasks": [],
            "email_tasks": [],
            "pre_post_sql": [],
            "links": links,
            "schedule": "",
            "format": "iics",
            "iics_type": "ApplicationIntegration",
            "event_type": event_type,
            "service_url": service_url,
        })
    return integrations


# =====================================================================
# Sprint 7.6: Connection XML parser
# =====================================================================

def parse_connection_objects(root):
    """Parse PowerCenter connection objects from XML.
    Extracts DBCONNECTION, FTPCONNECTION, etc.
    """
    parsed_connections = []

    # Database connections
    for conn in root.iter("DBCONNECTION"):
        parsed_connections.append({
            "name": conn.get("NAME", ""),
            "type": "Database",
            "subtype": conn.get("DBTYPE", conn.get("DATABASETYPE", "")),
            "connect_string": conn.get("CONNECTSTRING", ""),
            "user": conn.get("USERNAME", ""),
            "code_page": conn.get("CODEPAGE", ""),
        })

    # FTP connections
    for conn in root.iter("FTPCONNECTION"):
        parsed_connections.append({
            "name": conn.get("NAME", ""),
            "type": "FTP",
            "host": conn.get("HOSTNAME", ""),
            "remote_dir": conn.get("REMOTEDIRECTORY", ""),
        })

    # Generic connection objects (various types)
    for conn in root.iter("CONNECTION"):
        conn_name = conn.get("NAME", "")
        conn_type = conn.get("TYPE", conn.get("CONNECTIONTYPE", ""))
        if conn_name and not any(c["name"] == conn_name for c in parsed_connections):
            parsed_connections.append({
                "name": conn_name,
                "type": conn_type or "Unknown",
            })

    return parsed_connections


def detect_source_db_type(sql_content):
    """Detect source DB type from SQL content.
    Returns 'oracle', 'sqlserver', 'teradata', 'db2', 'mysql', 'postgresql', or 'unknown'.
    """
    all_patterns = {
        "oracle": ORACLE_PATTERNS,
        "sqlserver": SQLSERVER_PATTERNS,
        "teradata": TERADATA_PATTERNS,
        "db2": DB2_PATTERNS,
        "mysql": MYSQL_PATTERNS,
        "postgresql": POSTGRESQL_PATTERNS,
    }
    scores = {}
    for db_type, patterns in all_patterns.items():
        score = 0
        for pattern in patterns.values():
            if re.search(pattern, sql_content, re.IGNORECASE):
                score += 1
        scores[db_type] = score

    best = max(scores, key=scores.get)
    if scores[best] >= 2:
        return best
    if scores[best] >= 1:
        return best
    return "unknown"


def parse_sql_file(filepath):
    """Parse SQL file, detect DB type, and identify DB-specific constructs."""
    content = filepath.read_text(encoding="utf-8", errors="replace")

    db_type = detect_source_db_type(content)

    all_pattern_sets = {
        "oracle": ORACLE_PATTERNS,
        "sqlserver": SQLSERVER_PATTERNS,
        "teradata": TERADATA_PATTERNS,
        "db2": DB2_PATTERNS,
        "mysql": MYSQL_PATTERNS,
        "postgresql": POSTGRESQL_PATTERNS,
    }

    constructs_by_db = {}
    for db_name, patterns in all_pattern_sets.items():
        found = []
        for construct_name, pattern in patterns.items():
            matches = list(re.finditer(pattern, content, re.IGNORECASE))
            if matches:
                lines = []
                for m in matches:
                    line_num = content[:m.start()].count('\n') + 1
                    lines.append(line_num)
                found.append({
                    "construct": construct_name,
                    "occurrences": len(matches),
                    "lines": lines,
                })
        if found:
            constructs_by_db[db_name] = found

    # Sprint 20: Detect advanced constructs
    global_temp_tables = detect_global_temp_tables(content)
    materialized_views = detect_materialized_views(content)
    db_links = detect_db_links(content)

    result = {
        "file": filepath.name,
        "path": str(filepath.relative_to(WORKSPACE)),
        "db_type": db_type,
        "oracle_constructs": constructs_by_db.get("oracle", []),
        "sqlserver_constructs": constructs_by_db.get("sqlserver", []),
        "teradata_constructs": constructs_by_db.get("teradata", []),
        "db2_constructs": constructs_by_db.get("db2", []),
        "mysql_constructs": constructs_by_db.get("mysql", []),
        "postgresql_constructs": constructs_by_db.get("postgresql", []),
        "global_temp_tables": global_temp_tables,
        "materialized_views": materialized_views,
        "db_links": db_links,
        "total_lines": content.count('\n') + 1,
    }
    return result


# =====================================================================
# Sprint 20: Gap Remediation — P1/P2
# =====================================================================

def parse_session_config(workflow_root):
    """Extract session config properties from workflow/session XML.
    Returns a list of config dicts with Spark equivalents.
    """
    CONFIG_MAP = {
        "DTM buffer size": {"spark": "spark.sql.shuffle.partitions", "note": "Adjust based on data volume"},
        "Commit Interval": {"spark": "spark.databricks.delta.optimizeWrite.enabled", "note": "Use Delta auto-optimize"},
        "Sorter Cache Size": {"spark": "spark.sql.execution.sortMergeJoinThreshold", "note": "Sorter memory"},
        "Lookup Cache Size": {"spark": "spark.sql.autoBroadcastJoinThreshold", "note": "Broadcast threshold"},
        "Session Sort Order": {"spark": "spark.sql.shuffle.sortBased", "note": "Sort-based shuffle"},
        "Treat Source Rows As": {"spark": "merge_strategy", "note": "DD_INSERT/UPDATE/DELETE"},
        "Maximum Memory Allowed For Auto Memory Attributes": {"spark": "spark.executor.memory", "note": "Executor memory"},
    }

    configs = []
    # Check SESSION/SESSTRANSFORMATIONINST/ATTRIBUTE
    for session in workflow_root.iter("SESSION"):
        session_name = session.get("NAME", "")
        for attr in session.iter("ATTRIBUTE"):
            attr_name = attr.get("NAME", "")
            attr_value = attr.get("VALUE", "")
            if attr_name in CONFIG_MAP and attr_value:
                mapping = CONFIG_MAP[attr_name]
                configs.append({
                    "session": session_name,
                    "infa_property": attr_name,
                    "infa_value": attr_value,
                    "spark_property": mapping["spark"],
                    "note": mapping["note"],
                })
        # CONFIGREFERENCE elements
        for cfg_ref in session.iter("CONFIGREFERENCE"):
            cfg_name = cfg_ref.get("REFOBJECTNAME", "")
            if cfg_name:
                configs.append({
                    "session": session_name,
                    "infa_property": "Session Config Reference",
                    "infa_value": cfg_name,
                    "spark_property": "spark.conf.set()",
                    "note": f"Resolve config object '{cfg_name}'",
                })
    return configs


def parse_scheduler_cron(schedule_str):
    """Convert Informatica schedule names/intervals to Fabric cron.
    Returns a dict with cron components or a note for manual config.
    """
    if not schedule_str:
        return {"cron": "", "note": "No schedule defined"}

    s = schedule_str.upper().strip()

    # Common schedule patterns
    CRON_PATTERNS = {
        "DAILY": {"cron": "0 0 2 * * *", "note": "Daily at 2 AM UTC"},
        "HOURLY": {"cron": "0 0 * * * *", "note": "Every hour"},
        "WEEKLY": {"cron": "0 0 2 * * 1", "note": "Weekly Monday 2 AM UTC"},
        "MONTHLY": {"cron": "0 0 2 1 * *", "note": "Monthly 1st at 2 AM UTC"},
    }

    for keyword, result in CRON_PATTERNS.items():
        if keyword in s:
            return result

    # Extract time if present (e.g., "DAILY_02AM", "SCHED_0600")
    time_match = re.search(r'(\d{1,2})\s*(?:AM|PM|:\d{2})', s)
    if time_match:
        hour = int(time_match.group(1))
        if "PM" in s and hour < 12:
            hour += 12
        return {"cron": f"0 0 {hour} * * *", "note": f"Daily at {hour}:00 UTC (inferred)"}

    time_match = re.search(r'(\d{2})(\d{2})', s)
    if time_match:
        hour = int(time_match.group(1))
        minute = int(time_match.group(2))
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return {"cron": f"0 {minute} {hour} * * *", "note": f"Daily at {hour}:{minute:02d} UTC (inferred)"}

    return {"cron": "", "note": f"Manual configuration needed (original: {schedule_str})"}


def detect_global_temp_tables(sql_content):
    """Detect Oracle Global Temporary Tables in SQL and suggest Spark equivalents.
    Returns list of detected GTT references.
    """
    gtts = []
    # CREATE GLOBAL TEMPORARY TABLE
    for m in re.finditer(
        r'CREATE\s+GLOBAL\s+TEMPORARY\s+TABLE\s+(\w+)',
        sql_content, re.IGNORECASE
    ):
        table_name = m.group(1)
        line_num = sql_content[:m.start()].count('\n') + 1
        gtts.append({
            "table": table_name,
            "line": line_num,
            "spark_equivalent": f'spark.sql("CREATE OR REPLACE TEMP VIEW {table_name} AS ...")',
            "note": "Replace with createOrReplaceTempView() — session-scoped like GTT",
        })
    # References via ON COMMIT (preserve/delete rows)
    for m in re.finditer(
        r'ON\s+COMMIT\s+(PRESERVE|DELETE)\s+ROWS',
        sql_content, re.IGNORECASE
    ):
        action = m.group(1).upper()
        line_num = sql_content[:m.start()].count('\n') + 1
        note = ("Temp view persists like PRESERVE ROWS" if action == "PRESERVE"
                else "Use cache/unpersist after use like DELETE ROWS")
        gtts.append({
            "table": "(from ON COMMIT clause)",
            "line": line_num,
            "spark_equivalent": "createOrReplaceTempView()",
            "note": note,
        })
    return gtts


def detect_materialized_views(sql_content):
    """Detect Oracle Materialized Views and suggest Delta table equivalents.
    Returns list of detected MV references.
    """
    mvs = []
    for m in re.finditer(
        r'CREATE\s+MATERIALIZED\s+VIEW\s+(\w+)',
        sql_content, re.IGNORECASE
    ):
        mv_name = m.group(1)
        line_num = sql_content[:m.start()].count('\n') + 1
        mvs.append({
            "name": mv_name,
            "line": line_num,
            "spark_equivalent": f"Delta table '{mv_name}' with scheduled notebook refresh",
            "note": "No native MV in Spark; use Delta table + scheduled pipeline for refresh",
        })
    return mvs


def detect_db_links(sql_content):
    """Detect Oracle database link references (@dblink syntax).
    Returns list of detected DB link references.
    """
    links = []
    for m in re.finditer(r'@(\w+)', sql_content):
        # Filter false positives (email-like, annotations)
        pre = sql_content[max(0, m.start() - 20):m.start()]
        if any(kw in pre.lower() for kw in ['from', 'table', 'join', 'into', 'update', 'select']):
            link_name = m.group(1)
            line_num = sql_content[:m.start()].count('\n') + 1
            links.append({
                "db_link": link_name,
                "line": line_num,
                "spark_equivalent": "Configure separate JDBC connection for remote database",
                "note": f"Replace @{link_name} with spark.read.jdbc() pointing to remote DB",
            })
    return links


def main():
    # Clear module-level state to prevent accumulation across multiple runs
    warnings.clear()
    issues.clear()

    print("=" * 60)
    print("  Informatica-to-Fabric Assessment")
    print("=" * 60)
    print()

    all_mapplets = {}  # Collected from all XML files for expansion

    # 1. Parse Mappings (PowerCenter + IICS)
    print("[1/10] Parsing mapping XML files...")
    mapping_dir = INPUT_DIR / "mappings"
    all_mappings = []
    if mapping_dir.exists():
        for xml_file in sorted(mapping_dir.glob("*.xml")):
            print(f"  Parsing: {xml_file.name}")
            try:
                fmt = detect_xml_format(xml_file)
                if fmt == "iics":
                    mappings = parse_iics_mapping(xml_file)
                    print("    [IICS format]")
                else:
                    mappings = parse_mapping_xml(xml_file)

                # Collect Mapplet definitions from the same file
                tree, root = safe_parse_xml(xml_file)
                if root is not None:
                    file_mapplets = parse_mapplets(root)
                    all_mapplets.update(file_mapplets)
                    if file_mapplets:
                        print(f"    Mapplets found: {', '.join(file_mapplets.keys())}")

                all_mappings.extend(mappings)
                for m in mappings:
                    print(f"    -> {m['name']} ({m['complexity']}) -- {len(m['transformations'])} transformations")
            except Exception as e:
                warnings.append(f"Error processing {xml_file.name}: {e}")
                print(f"    ERROR: {e}")
    print(f"  Total mappings found: {len(all_mappings)}")
    print()

    # 2. Expand Mapplet references
    print("[2/10] Expanding Mapplet references...")
    mplt_count = 0
    for m in all_mappings:
        expand_mapplet_refs(m, all_mapplets)
        if m.get("has_mapplet"):
            mplt_count += 1
            print(f"  Expanded: {m['name']} (Mapplet references resolved)")
    print(f"  Mapplets defined: {len(all_mapplets)}, Mappings with Mapplet refs: {mplt_count}")
    print()

    # 3. Parse Workflows (PowerCenter + IICS Taskflows)
    print("[3/10] Parsing workflow XML files...")
    workflow_dir = INPUT_DIR / "workflows"
    all_workflows = []
    if workflow_dir.exists():
        for xml_file in sorted(workflow_dir.glob("*.xml")):
            print(f"  Parsing: {xml_file.name}")
            try:
                fmt = detect_xml_format(xml_file)
                if fmt == "iics":
                    # Parse IICS Taskflows
                    taskflows = parse_iics_taskflow(xml_file)
                    all_workflows.extend(taskflows)
                    for tf in taskflows:
                        print(f"    [IICS Taskflow] -> {tf['name']} -- {len(tf['sessions'])} tasks, {len(tf['links'])} links")

                    # Parse IICS Sync Tasks and Mass Ingestion Tasks as mappings
                    sync_tasks = parse_iics_sync_tasks(xml_file)
                    for st in sync_tasks:
                        all_mappings.append(st)
                        print(f"    [IICS SyncTask] -> {st['name']}")

                    mi_tasks = parse_iics_mass_ingestion(xml_file)
                    for mi in mi_tasks:
                        all_mappings.append(mi)
                        print(f"    [IICS MassIngestion] -> {mi['name']}")

                    # Parse IICS Data Quality Tasks
                    dq_tasks = parse_iics_dq_tasks(xml_file)
                    for dq in dq_tasks:
                        all_mappings.append(dq)
                        print(f"    [IICS DQ Task] -> {dq['name']}")

                    # Parse IICS Application Integration (event-driven)
                    app_ints = parse_iics_app_integration(xml_file)
                    for ai in app_ints:
                        all_workflows.append(ai)
                        print(f"    [IICS AppIntegration] -> {ai['name']}")

                    # Parse IICS Connections
                    iics_conns = parse_iics_connections(xml_file)
                    if iics_conns:
                        print(f"    [IICS Connections] {len(iics_conns)} found")
                else:
                    workflows = parse_workflow_xml(xml_file)
                    all_workflows.extend(workflows)
                    for wf in workflows:
                        print(f"    -> {wf['name']} -- {len(wf['sessions'])} sessions, {len(wf['links'])} links")
            except Exception as e:
                warnings.append(f"Error processing {xml_file.name}: {e}")
                print(f"    ERROR: {e}")
    print(f"  Total workflows found: {len(all_workflows)}")
    print()

    # 3b. Extract session configs from workflow XMLs
    all_session_configs = []
    if workflow_dir.exists():
        for xml_file in sorted(workflow_dir.glob("*.xml")):
            fmt = detect_xml_format(xml_file)
            if fmt != "iics":
                tree, root = safe_parse_xml(xml_file)
                if root is not None:
                    configs = parse_session_config(root)
                    all_session_configs.extend(configs)
    if all_session_configs:
        print(f"  Session configs extracted: {len(all_session_configs)}")
        print()

    # 4. Parse SQL files (Oracle + SQL Server detection)
    print("[4/10] Parsing SQL files...")
    sql_dir = INPUT_DIR / "sql"
    sql_files = []
    if sql_dir.exists():
        for sql_file in sorted(sql_dir.glob("*.sql")):
            print(f"  Parsing: {sql_file.name}")
            analysis = parse_sql_file(sql_file)
            sql_files.append(analysis)
            db = analysis.get("db_type", "unknown")
            constructs = [oc["construct"] for oc in analysis["oracle_constructs"]]
            ss_constructs = [oc["construct"] for oc in analysis.get("sqlserver_constructs", [])]
            print(f"    -> DB type: {db} | Oracle: {', '.join(constructs) if constructs else 'none'} | MSSQL: {', '.join(ss_constructs) if ss_constructs else 'none'}")
    print(f"  Total SQL files analyzed: {len(sql_files)}")
    print()

    # 5. Parse Parameter files
    print("[5/10] Parsing parameter files...")
    param_files = parse_parameter_files(INPUT_DIR)
    # Also check common sub-folders
    for subdir in ["params", "parameters", "prm", "mappings", "workflows", "sessions"]:
        param_files.extend(parse_parameter_files(INPUT_DIR / subdir))
    print(f"  Total parameter files: {len(param_files)}, Total params: {sum(pf['total_params'] for pf in param_files)}")
    print()

    # 6. Extract connections (inferred + XML-parsed + IICS)
    print("[6/10] Extracting connections...")
    connections = extract_connections_from_mappings(all_mappings)
    # Parse connection objects from all XML files
    xml_connections = []
    for xml_dir_name in ["mappings", "workflows", "sessions"]:
        xml_dir = INPUT_DIR / xml_dir_name
        if xml_dir.exists():
            for xml_file in xml_dir.glob("*.xml"):
                fmt = detect_xml_format(xml_file)
                if fmt == "iics":
                    # Parse IICS connection objects
                    xml_connections.extend(parse_iics_connections(xml_file))
                else:
                    tree, root = safe_parse_xml(xml_file)
                    if root is not None:
                        xml_connections.extend(parse_connection_objects(root))
    # Deduplicate by name
    seen_conn_names = {c["name"] for c in connections}
    for xc in xml_connections:
        if xc["name"] and xc["name"] not in seen_conn_names:
            connections.append(xc)
            seen_conn_names.add(xc["name"])

    print(f"  Connections found: {len(connections)} (inferred + XML-parsed)")
    print()

    # 6b. Ensure all mappings have conversion scores (backfill IICS/synthetic)
    for m in all_mappings:
        if "conversion_score" not in m:
            m["conversion_score"] = calculate_conversion_score(m)
            m["manual_effort_hours"] = estimate_manual_effort(m)

    # 7. Write outputs
    print("[7/10] Writing output files...")
    inventory = write_inventory_json(all_mappings, all_workflows, connections, sql_files)
    lineage_data = write_lineage_json(all_mappings)
    # Add param_files and mapplets to inventory
    inventory["parameter_files"] = param_files
    inventory["mapplets"] = {name: [t["abbrev"] for t in txs] for name, txs in all_mapplets.items()}
    inventory["session_configs"] = all_session_configs
    inventory["summary"]["total_parameter_files"] = len(param_files)
    inventory["summary"]["total_mapplets"] = len(all_mapplets)
    inventory["summary"]["total_session_configs"] = len(all_session_configs)
    # Re-write with augmented data
    out_path = OUTPUT_DIR / "inventory.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(inventory, f, indent=2, ensure_ascii=False)

    write_complexity_report(inventory)
    dag = build_dependency_dag(all_workflows)
    write_dependency_dag(dag)

    # Sprint 28: Wave planner
    wave_data = generate_wave_plan(all_mappings, all_workflows)
    write_wave_plan(wave_data)

    # Sprint 39: PII detection & DQ rule extraction
    pii_findings = detect_pii_columns(all_mappings)
    dq_rules = extract_dq_rules(all_mappings)
    inventory["pii_findings"] = pii_findings
    inventory["dq_rules"] = dq_rules
    inventory["summary"]["total_pii_findings"] = len(pii_findings)
    inventory["summary"]["total_dq_rules"] = len(dq_rules)
    # Re-write inventory with PII/DQ data
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(inventory, f, indent=2, ensure_ascii=False)
    if pii_findings:
        print(f"  PII columns detected: {len(pii_findings)}")
        categories = {}
        for p in pii_findings:
            categories[p["pii_category"]] = categories.get(p["pii_category"], 0) + 1
        for cat, cnt in sorted(categories.items()):
            print(f"    {cat}: {cnt}")
    if dq_rules:
        print(f"  DQ rules extracted: {len(dq_rules)}")

    print()

    # 8. Summary
    print("[8/10] Assessment complete!")
    print()
    print("=" * 60)
    cb = inventory["summary"]["complexity_breakdown"]
    print(f"  Mappings:     {inventory['summary']['total_mappings']}")
    print(f"  Workflows:    {inventory['summary']['total_workflows']}")
    print(f"  Sessions:     {inventory['summary']['total_sessions']}")
    print(f"  SQL files:    {inventory['summary']['total_sql_files']}")
    print(f"  Param files:  {len(param_files)}")
    print(f"  Mapplets:     {len(all_mapplets)}")
    print(f"  Connections:  {len(connections)}")
    print()
    print("  Complexity breakdown:")
    for level in ["Simple", "Medium", "Complex", "Custom"]:
        print(f"    {level:8s}: {cb.get(level, 0)}")
    print()

    total_overrides = sum(len(m["sql_overrides"]) for m in all_mappings)
    total_oracle = sum(len(sf["oracle_constructs"]) for sf in sql_files)
    total_mssql = sum(len(sf.get("sqlserver_constructs", [])) for sf in sql_files)
    print(f"  SQL overrides in mappings: {total_overrides}")
    print(f"  Oracle constructs in SQL files: {total_oracle}")
    if total_mssql:
        print(f"  SQL Server constructs in SQL files: {total_mssql}")

    # Sprint 25: Lineage & conversion scoring stats
    avg_score = inventory["summary"].get("avg_conversion_score", 0)
    total_effort = inventory["summary"].get("total_manual_effort_hours", 0)
    total_lineage = inventory["summary"].get("total_field_lineage_paths", 0)
    print()
    print("  Conversion readiness:")
    print(f"    Avg conversion score: {avg_score}/100")
    print(f"    Est. manual effort:   {total_effort} hours")
    print(f"    Field lineage paths:  {total_lineage}")

    # Sprint 28: Wave plan stats
    print()
    print("  Migration waves:")
    print(f"    Total waves:          {wave_data['total_waves']}")
    print(f"    Critical path length: {len(wave_data['critical_path'])} mappings")
    print(f"    Critical path effort: {wave_data['critical_path_effort_hours']}h")

    # 9. Warnings and issues
    print()
    print("[9/10] Final report...")

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
    print("    - lineage.json")
    print("    - wave_plan.json")
    print("    - wave_plan.md")
    if issues:
        print("    - parse_issues.json")

    # 10. Generate HTML reports
    print()
    print("[10/10] Generating HTML reports...")
    try:
        from generate_html_reports import generate_assessment_report, generate_migration_report
        assessment_html = OUTPUT_DIR / "assessment_report.html"
        migration_html = OUTPUT_DIR / "migration_report.html"
        generate_assessment_report(inventory, assessment_html)
        generate_migration_report(inventory, migration_html)
        print("    - assessment_report.html")
        print("    - migration_report.html")
    except Exception as e:
        print(f"    WARNING: HTML report generation failed: {e}")
        print("    Run generate_html_reports.py separately to retry.")

    print("=" * 60)

    # Return non-zero exit code if there were errors (not just warnings)
    error_count = sum(1 for i in issues if i["severity"] == "ERROR")
    if error_count:
        print(f"\n  Completed with {error_count} error(s). Partial results saved.")
        sys.exit(1)


if __name__ == "__main__":
    main()
