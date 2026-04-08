"""
Example Plugin: Notebook Header Injector — Sprint 74

Demonstrates post-processing hooks that add custom headers
to all generated notebooks.
"""

from plugins import post_notebook


@post_notebook
def add_company_header(content, mapping):
    """Add a company-specific header to all generated notebooks."""
    header = (
        "# ================================================================\n"
        "# ACME Corporation — Data Platform Migration\n"
        "# ================================================================\n"
        f"# Mapping: {mapping.get('name', 'Unknown')}\n"
        "# Classification: Internal Use Only\n"
        "# ================================================================\n\n"
    )
    return header + content
