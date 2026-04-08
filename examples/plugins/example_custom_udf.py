"""
Example Plugin: Custom UDF Converter — Sprint 74

Demonstrates how to register a custom transformation handler
for enterprise-specific UDF transformations.
"""

from plugins import register_transform, register_sql_rewrite


@register_transform("CUSTOM_VALIDATE")
def handle_custom_validate(prev_df, mapping, cell_num):
    """Convert a custom validation transformation to PySpark."""
    return [
        f"# CELL {cell_num} — Custom Validation (plugin)",
        "from pyspark.sql.functions import col, when, lit",
        "",
        f"df = {prev_df}.withColumn(",
        '    "validation_status",',
        '    when(col("amount") > 0, lit("VALID"))',
        '    .otherwise(lit("INVALID"))',
        ")",
    ]


@register_sql_rewrite(r"\bCUSTOM_HASH\s*\(")
def rewrite_custom_hash(sql_text):
    """Convert enterprise CUSTOM_HASH() to standard sha2()."""
    import re
    return re.sub(
        r'\bCUSTOM_HASH\s*\(([^)]+)\)',
        r'sha2(\1, 256)',
        sql_text,
        flags=re.IGNORECASE,
    )
