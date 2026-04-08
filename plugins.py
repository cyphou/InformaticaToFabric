"""
Plugin System — Sprint 74
Allows enterprises to register custom transformation converters,
SQL rewrite rules, and post-processing hooks without modifying core code.

Plugins are discovered from the `plugins/` directory automatically.
Each plugin is a Python module that uses decorators to register handlers.

Usage:
    # In plugins/my_custom.py:
    from plugins import register_transform, register_sql_rewrite, post_notebook

    @register_transform("CUSTOM_TX")
    def handle_custom_tx(prev_df, mapping, cell_num):
        return "df = prev_df.withColumn('custom', lit('value'))"

    @register_sql_rewrite(r"MY_FUNC\\(")
    def rewrite_my_func(sql_text):
        return sql_text.replace("MY_FUNC(", "my_pyspark_func(")

    @post_notebook
    def add_header(notebook_content, mapping):
        return "# Custom header\\n" + notebook_content
"""

import importlib
import logging
import re
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

WORKSPACE = Path(__file__).resolve().parent
PLUGINS_DIR = WORKSPACE / "plugins"

# ─────────────────────────────────────────────
#  Plugin Registries
# ─────────────────────────────────────────────

_transform_handlers = {}     # TX_TYPE → callable(prev_df, mapping, cell_num) → str
_sql_rewrite_rules = []      # [(compiled_pattern, callable(sql_text) → str)]
_post_notebook_hooks = []    # [callable(content, mapping) → str]
_post_pipeline_hooks = []    # [callable(content, workflow) → str]
_post_sql_hooks = []         # [callable(content, db_type) → str]


def register_transform(tx_type):
    """Decorator to register a custom transformation handler.

    @register_transform("CUSTOM_TX")
    def handle_custom_tx(prev_df, mapping, cell_num):
        return ["# Custom transformation", f"df = {prev_df}.withColumn(...)"]
    """
    def decorator(func):
        _transform_handlers[tx_type.upper()] = func
        logger.debug("Registered transform handler for %s", tx_type)
        return func
    return decorator


def register_sql_rewrite(pattern):
    """Decorator to register a custom SQL rewrite rule.

    @register_sql_rewrite(r"MY_UDF\\(")
    def rewrite_my_udf(sql_text):
        return sql_text.replace("MY_UDF(", "spark_my_udf(")
    """
    def decorator(func):
        compiled = re.compile(pattern, re.IGNORECASE)
        _sql_rewrite_rules.append((compiled, func))
        logger.debug("Registered SQL rewrite rule for pattern: %s", pattern)
        return func
    return decorator


def post_notebook(func):
    """Decorator to register a post-processing hook for notebooks.

    @post_notebook
    def add_custom_header(content, mapping):
        return "# My Company Header\\n" + content
    """
    _post_notebook_hooks.append(func)
    logger.debug("Registered post-notebook hook: %s", func.__name__)
    return func


def post_pipeline(func):
    """Decorator to register a post-processing hook for pipelines.

    @post_pipeline
    def add_custom_properties(content, workflow):
        import json
        data = json.loads(content)
        data["properties"]["custom"] = True
        return json.dumps(data, indent=2)
    """
    _post_pipeline_hooks.append(func)
    logger.debug("Registered post-pipeline hook: %s", func.__name__)
    return func


def post_sql(func):
    """Decorator to register a post-processing hook for SQL files.

    @post_sql
    def add_license_header(content, db_type):
        return "-- Licensed under MIT\\n" + content
    """
    _post_sql_hooks.append(func)
    logger.debug("Registered post-sql hook: %s", func.__name__)
    return func


# ─────────────────────────────────────────────
#  Plugin Discovery & Loading
# ─────────────────────────────────────────────

def discover_plugins(plugins_dir=None):
    """Discover and load all .py plugins from the plugins directory.

    Returns list of loaded module names.
    """
    if plugins_dir is None:
        plugins_dir = PLUGINS_DIR

    plugins_dir = Path(plugins_dir)
    if not plugins_dir.exists():
        logger.info("No plugins directory found at %s", plugins_dir)
        return []

    loaded = []
    # Add plugins dir to sys.path temporarily for imports
    plugins_parent = str(plugins_dir.parent)
    if plugins_parent not in sys.path:
        sys.path.insert(0, plugins_parent)

    for plugin_file in sorted(plugins_dir.glob("*.py")):
        if plugin_file.name.startswith("_"):
            continue
        module_name = f"plugins.{plugin_file.stem}"
        try:
            if module_name in sys.modules:
                importlib.reload(sys.modules[module_name])
            else:
                importlib.import_module(module_name)
            loaded.append(module_name)
            logger.info("Loaded plugin: %s", module_name)
        except Exception as e:
            logger.warning("Failed to load plugin %s: %s", plugin_file.name, e)

    return loaded


# ─────────────────────────────────────────────
#  Plugin Execution API
# ─────────────────────────────────────────────

def get_transform_handler(tx_type):
    """Return the registered handler for a transformation type, or None."""
    return _transform_handlers.get(tx_type.upper())


def apply_sql_rewrites(sql_text):
    """Apply all registered SQL rewrite rules to the text."""
    result = sql_text
    for pattern, handler in _sql_rewrite_rules:
        if pattern.search(result):
            try:
                result = handler(result)
            except Exception as e:
                logger.warning("SQL rewrite rule failed: %s", e)
    return result


def apply_post_notebook_hooks(content, mapping):
    """Apply all post-notebook hooks to the generated content."""
    result = content
    for hook in _post_notebook_hooks:
        try:
            result = hook(result, mapping)
        except Exception as e:
            logger.warning("Post-notebook hook %s failed: %s", hook.__name__, e)
    return result


def apply_post_pipeline_hooks(content, workflow):
    """Apply all post-pipeline hooks to the generated content."""
    result = content
    for hook in _post_pipeline_hooks:
        try:
            result = hook(result, workflow)
        except Exception as e:
            logger.warning("Post-pipeline hook %s failed: %s", hook.__name__, e)
    return result


def apply_post_sql_hooks(content, db_type):
    """Apply all post-sql hooks to the generated content."""
    result = content
    for hook in _post_sql_hooks:
        try:
            result = hook(result, db_type)
        except Exception as e:
            logger.warning("Post-sql hook %s failed: %s", hook.__name__, e)
    return result


def get_registered_transforms():
    """Return a copy of all registered transform handlers."""
    return dict(_transform_handlers)


def get_registered_sql_rewrites():
    """Return count of registered SQL rewrite rules."""
    return len(_sql_rewrite_rules)


def get_registered_hooks():
    """Return counts of registered hooks by type."""
    return {
        "post_notebook": len(_post_notebook_hooks),
        "post_pipeline": len(_post_pipeline_hooks),
        "post_sql": len(_post_sql_hooks),
    }


def clear_all():
    """Clear all registered plugins (for testing)."""
    _transform_handlers.clear()
    _sql_rewrite_rules.clear()
    _post_notebook_hooks.clear()
    _post_pipeline_hooks.clear()
    _post_sql_hooks.clear()
