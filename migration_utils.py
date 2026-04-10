"""
Shared utilities for the Informatica-to-Fabric/Databricks migration tool.

Provides target-platform resolution, catalog naming, table references,
and other helpers used across multiple run_*.py modules.
"""

import os
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent


# ─────────────────────────────────────────────
#  Target Platform Resolution
# ─────────────────────────────────────────────

def get_target(config=None):
    """Return the target platform ('fabric' or 'databricks').

    Resolution order:
      1. INFORMATICA_MIGRATION_TARGET environment variable
      2. config dict key 'target'
      3. Default: 'fabric'
    """
    return os.environ.get(
        "INFORMATICA_MIGRATION_TARGET",
        (config or {}).get("target", "fabric"),
    )


def get_catalog():
    """Return the Unity Catalog name for Databricks target."""
    return os.environ.get("INFORMATICA_DATABRICKS_CATALOG", "main")


# ─────────────────────────────────────────────
#  Table & API References
# ─────────────────────────────────────────────

def table_ref(tier, table_name):
    """Return a fully-qualified table reference for the active target platform.

    Fabric:     tier.table_name           (2-level namespace)
    Databricks: catalog.tier.table_name   (Unity Catalog 3-level namespace)
    """
    target = get_target()
    if target == "databricks":
        catalog = get_catalog()
        return f"{catalog}.{tier}.{table_name}"
    return f"{tier}.{table_name}"


def widget_get(param_name):
    """Return the widget-get call for the active target platform."""
    target = get_target()
    if target == "databricks":
        return f'dbutils.widgets.get("{param_name}")'
    return f'notebookutils.widgets.get("{param_name}")'


def secret_get(vault_or_scope, secret_name):
    """Return the secret-retrieval call for the active target platform."""
    target = get_target()
    if target == "databricks":
        return f'dbutils.secrets.get(scope="{vault_or_scope}", key="{secret_name}")'
    return f'notebookutils.credentials.getSecret("{vault_or_scope}", "{secret_name}")'


# ─────────────────────────────────────────────
#  Notebook Helpers
# ─────────────────────────────────────────────

def cell_sep():
    """Return a notebook cell separator comment."""
    return "\n# COMMAND ----------\n\n"
