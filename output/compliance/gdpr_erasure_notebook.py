"""
GDPR Right-to-Erasure Notebook (Article 17)
Generated: 2026-04-10 14:15:55 UTC

Usage: Set subject_id widget/parameter before running.
This notebook deletes all records matching the data subject ID
across all tables with GDPR-relevant PII columns.
"""

# Fabric notebook
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# --- Configuration ---
subject_id = spark.conf.get("spark.erasure.subject_id", "")
if not subject_id:
    raise ValueError("subject_id must be set before running erasure")

audit_records = []

# No GDPR-relevant tables detected
print('No tables with GDPR-relevant PII found.')
# --- Audit Log ---
import json
from datetime import datetime, timezone
audit = {
    "operation": "gdpr_erasure",
    "subject_id_hash": hash(subject_id),  # Do not log actual subject_id
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "tables_processed": len(audit_records),
    "total_deleted": sum(r["deleted"] for r in audit_records),
    "details": audit_records,
}
print(json.dumps(audit, indent=2))