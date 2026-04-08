"""
Python SDK — Sprint 75
Programmatic API for headless migration, enabling CI/CD pipelines
and custom automation scripts.

Usage:
    from sdk import MigrationSDK, MigrationConfig

    config = MigrationConfig(target="fabric")
    sdk = MigrationSDK(config)

    # Assess
    inventory = sdk.assess("input/mappings/")

    # Convert a single mapping
    sdk.convert_mapping("M_LOAD_CUSTOMERS")

    # Full migration
    results = sdk.migrate()

    # Validate
    sdk.validate()
"""

import json
import importlib
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

WORKSPACE = Path(__file__).resolve().parent


@dataclass
class MigrationConfig:
    """Configuration for the migration SDK — dataclass with all CLI flag equivalents."""

    target: str = "fabric"
    input_dir: str = "input"
    output_dir: str = "output"
    databricks_catalog: str = "main"
    dbt_mode: str = "auto"
    autosys_dir: str = ""
    config_path: str = "migration.yaml"
    verbose: bool = False
    dry_run: bool = False
    parallel: bool = False
    skip_phases: list = field(default_factory=list)
    only_phases: list = field(default_factory=list)
    tenant_id: str = ""

    def to_env_vars(self):
        """Convert config to environment variables used by migration modules."""
        env = {
            "INFORMATICA_MIGRATION_TARGET": self.target
            if self.target not in ("dbt", "pyspark", "auto")
            else "databricks",
        }
        if self.target in ("dbt", "pyspark", "auto"):
            env["INFORMATICA_DBT_MODE"] = self.target
        if self.databricks_catalog:
            env["INFORMATICA_DATABRICKS_CATALOG"] = self.databricks_catalog
        if self.autosys_dir:
            env["INFORMATICA_AUTOSYS_DIR"] = self.autosys_dir
        if self.input_dir != "input":
            env["INFORMATICA_INPUT_DIR"] = self.input_dir
        return env

    @classmethod
    def from_dict(cls, d):
        """Create config from a dictionary."""
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in d.items() if k in valid_fields}
        return cls(**filtered)

    @classmethod
    def from_yaml(cls, yaml_path):
        """Create config from a YAML file."""
        path = Path(yaml_path)
        if not path.exists():
            return cls()
        try:
            import yaml
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except ImportError:
            data = {}
        return cls.from_dict(data)


class MigrationSDK:
    """Primary SDK class for programmatic migration."""

    def __init__(self, config=None):
        if config is None:
            config = MigrationConfig()
        self.config = config
        self._inventory = None
        self._results = []

        # Apply env vars
        for key, value in config.to_env_vars().items():
            os.environ[key] = value

        if config.verbose:
            logging.basicConfig(level=logging.DEBUG)

    def assess(self, source_dir=None):
        """Run assessment phase on the given source directory.

        Returns the inventory dict with mappings, workflows, complexity scores.
        """
        if source_dir:
            os.environ["INFORMATICA_INPUT_DIR"] = str(source_dir)

        import run_assessment
        if "run_assessment" in sys.modules:
            importlib.reload(run_assessment)

        # Capture inventory
        inv_path = Path(self.config.output_dir) / "inventory" / "inventory.json"
        if not inv_path.is_absolute():
            inv_path = WORKSPACE / inv_path

        if not self.config.dry_run:
            old_argv = sys.argv
            sys.argv = ["run_assessment"]
            try:
                run_assessment.main()
            finally:
                sys.argv = old_argv

        if inv_path.exists():
            with open(inv_path, encoding="utf-8") as f:
                self._inventory = json.load(f)
        else:
            self._inventory = {"mappings": [], "workflows": []}

        self._results.append({"phase": "assessment", "status": "completed"})
        return self._inventory

    def convert_mapping(self, mapping_name, target=None):
        """Convert a single mapping by name.

        Returns the path to the generated notebook.
        """
        if target:
            os.environ["INFORMATICA_MIGRATION_TARGET"] = target

        inv = self._load_inventory()
        mapping = None
        for m in inv.get("mappings", []):
            if m["name"] == mapping_name:
                mapping = m
                break

        if not mapping:
            raise ValueError(f"Mapping '{mapping_name}' not found in inventory")

        import run_notebook_migration
        content = run_notebook_migration.generate_notebook(mapping)

        out_dir = WORKSPACE / self.config.output_dir / "notebooks"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"NB_{mapping_name}.py"

        if not self.config.dry_run:
            out_path.write_text(content, encoding="utf-8")

        return str(out_path)

    def convert_sql(self, sql_text, db_type="oracle"):
        """Convert a SQL string from source dialect to Spark SQL.

        Returns the converted SQL text.
        """
        import run_sql_migration
        return run_sql_migration.convert_sql(sql_text, db_type)

    def migrate(self, phases=None):
        """Run the full migration pipeline.

        Returns list of phase results.
        """
        old_argv = sys.argv
        argv = ["run_migration", "--target", self.config.target]

        if self.config.verbose:
            argv.append("--verbose")
        if self.config.dry_run:
            argv.append("--dry-run")
        if self.config.skip_phases:
            argv.extend(["--skip"] + [str(p) for p in self.config.skip_phases])
        if self.config.only_phases:
            argv.extend(["--only"] + [str(p) for p in self.config.only_phases])

        sys.argv = argv
        try:
            import run_migration
            run_migration.main()
        except SystemExit:
            pass
        finally:
            sys.argv = old_argv

        self._results.append({"phase": "full_migration", "status": "completed"})
        return self._results

    def validate(self, output_dir=None):
        """Generate validation notebooks.

        Returns list of generated validation file paths.
        """
        old_argv = sys.argv
        sys.argv = ["run_validation"]
        try:
            import run_validation
            run_validation.main()
        except SystemExit:
            pass
        finally:
            sys.argv = old_argv

        val_dir = WORKSPACE / self.config.output_dir / "validation"
        generated = [str(f) for f in val_dir.glob("VAL_*.py")] if val_dir.exists() else []
        self._results.append({"phase": "validation", "status": "completed", "files": len(generated)})
        return generated

    def get_inventory(self):
        """Return the current inventory (loads from file if needed)."""
        return self._load_inventory()

    def get_status(self):
        """Return the current migration status."""
        return {
            "config": {
                "target": self.config.target,
                "input_dir": self.config.input_dir,
                "output_dir": self.config.output_dir,
            },
            "results": self._results,
            "inventory_loaded": self._inventory is not None,
        }

    def _load_inventory(self):
        """Load inventory from file if not already loaded."""
        if self._inventory is not None:
            return self._inventory

        inv_path = Path(self.config.output_dir) / "inventory" / "inventory.json"
        if not inv_path.is_absolute():
            inv_path = WORKSPACE / inv_path

        if inv_path.exists():
            with open(inv_path, encoding="utf-8") as f:
                self._inventory = json.load(f)
        else:
            self._inventory = {"mappings": [], "workflows": []}

        return self._inventory


# Convenience one-liner for simple use


def migrate(source_dir=None, target="fabric", **kwargs):
    """One-liner migration entry point.

    from sdk import migrate
    results = migrate(target="databricks")
    """
    config = MigrationConfig(target=target, **kwargs)
    sdk = MigrationSDK(config)
    if source_dir:
        sdk.assess(source_dir)
    return sdk.migrate()


def assess(xml_path):
    """One-liner assessment entry point.

    from sdk import assess
    inventory = assess("input/mappings/")
    """
    config = MigrationConfig()
    sdk = MigrationSDK(config)
    return sdk.assess(xml_path)


def convert_mapping(name, target="fabric"):
    """One-liner mapping conversion.

    from sdk import convert_mapping
    path = convert_mapping("M_LOAD_CUSTOMERS")
    """
    config = MigrationConfig(target=target)
    sdk = MigrationSDK(config)
    return sdk.convert_mapping(name)


def validate(output_dir=None):
    """One-liner validation generation.

    from sdk import validate
    files = validate()
    """
    config = MigrationConfig()
    sdk = MigrationSDK(config)
    return sdk.validate(output_dir)
