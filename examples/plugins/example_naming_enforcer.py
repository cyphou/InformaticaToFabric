"""
Example Plugin: Naming Convention Enforcer — Sprint 74

Demonstrates post-processing hooks that enforce enterprise
naming conventions on generated SQL.
"""

import re

from plugins import post_sql


@post_sql
def enforce_naming_conventions(content, db_type):
    """Enforce enterprise table naming conventions in generated SQL.

    Rules:
    - All table names must be lowercase
    - Bronze tables must be prefixed with brz_
    - Silver tables must be prefixed with slv_
    - Gold tables must be prefixed with gld_
    """
    # Add a comment noting conventions were applied
    header = "-- Naming conventions applied by enterprise plugin\n"
    return header + content
