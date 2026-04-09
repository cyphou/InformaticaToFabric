"""
Test Suite — IDMC Client & Migration Review (DD10–DD12)
=======================================================
Tests: IDMC inventory, component parsers, migration review, anti-patterns, rework.

Run:
    pytest tests/test_idmc_review.py -v
"""

import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from idmc_client import (
    CDGCParser,
    CDQParser,
    ComponentParser,
    IDMC_COMPONENTS,
    IDMCClient,
    IDMCInventoryBuilder,
    MDMParser,
    load_idmc_config,
)
from migration_review import (
    AntiPatternDetector,
    ConsolidationRecommender,
    DuplicateDetector,
    ReviewQueueManager,
    ReworkClassifier,
    SQLOptimizer,
    SimilarityScorer,
    load_review_config,
    run_migration_review,
)


# ─────────────────────────────────────────────
#  Fixtures
# ─────────────────────────────────────────────

@pytest.fixture
def idmc_config():
    return {
        "idmc": {
            "enabled": True,
            "api_base_url": "https://dm-us.informaticacloud.com",
            "org_id": "test-org",
            "components": {
                "cdi": True,
                "cdgc": True,
                "cdq": True,
                "mdm": True,
                "b2b": True,
            },
            "review": {
                "merge_threshold": 0.8,
                "auto_apply": False,
                "anti_patterns": True,
            },
        }
    }


@pytest.fixture
def sample_output_dir(tmp_path):
    """Create a sample output directory with test artifacts."""
    # Notebooks
    nb_dir = tmp_path / "notebooks"
    nb_dir.mkdir()
    (nb_dir / "NB_LOAD_CUSTOMERS.py").write_text(
        '# Notebook\ndf = spark.read.format("delta").load("/tables/customers")\n'
        'df.select("*").write.format("delta").saveAsTable("dim_customer")\n'
    )
    (nb_dir / "NB_LOAD_CUSTOMERS_V2.py").write_text(
        '# Notebook\ndf = spark.read.format("delta").load("/tables/customers")\n'
        'df.select("*").write.format("delta").saveAsTable("dim_customer")\n'
    )

    # SQL
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir()
    (sql_dir / "SQL_RANKINGS.sql").write_text(
        "SELECT * FROM sales_fact\nWHERE region = 'US'\nORDER BY amount DESC;\n"
    )

    # Pipeline JSON
    pl_dir = tmp_path / "pipelines"
    pl_dir.mkdir()
    (pl_dir / "PL_DAILY.json").write_text('{"name": "PL_DAILY", "activities": []}')

    # File with rework markers
    (nb_dir / "NB_COMPLEX.py").write_text(
        '# TODO: Handle edge case for null columns\n'
        '# FIXME: Performance issue with large datasets\n'
        'result = df.collect()  # unbounded collect\n'
    )

    return str(tmp_path)


# ─────────────────────────────────────────────
#  DD10: IDMC Configuration
# ─────────────────────────────────────────────

class TestLoadIDMCConfig:

    def test_defaults(self):
        cfg = load_idmc_config({})
        assert cfg["enabled"] is False

    def test_from_config(self, idmc_config):
        cfg = load_idmc_config(idmc_config)
        assert cfg["enabled"] is True
        assert cfg["org_id"] == "test-org"

    def test_env_var_override(self, idmc_config):
        with pytest.MonkeyPatch.context() as m:
            m.setenv("IDMC_USERNAME", "env_user")
            m.setenv("IDMC_PASSWORD", "env_pass")
            cfg = load_idmc_config(idmc_config)
            assert cfg["username"] == "env_user"
            assert cfg["password"] == "env_pass"


# ─────────────────────────────────────────────
#  DD10: IDMC Components Registry
# ─────────────────────────────────────────────

class TestIDMCComponents:

    def test_all_10_components_defined(self):
        expected = {"cdi", "cdgc", "cdq", "mdm", "b2b", "privacy",
                    "marketplace", "api_center", "dih", "opinsights"}
        assert set(IDMC_COMPONENTS.keys()) == expected

    def test_each_component_has_required_fields(self):
        for comp_id, comp in IDMC_COMPONENTS.items():
            assert "name" in comp, f"{comp_id} missing name"
            assert "api_path" in comp, f"{comp_id} missing api_path"
            assert "supported" in comp, f"{comp_id} missing supported"
            assert "artifact_types" in comp, f"{comp_id} missing artifact_types"
            assert len(comp["artifact_types"]) >= 3, f"{comp_id} has < 3 artifact types"

    def test_cdi_artifact_types(self):
        cdi = IDMC_COMPONENTS["cdi"]
        assert "mapping" in cdi["artifact_types"]
        assert "taskflow" in cdi["artifact_types"]
        assert "connection" in cdi["artifact_types"]

    def test_cdgc_artifact_types(self):
        cdgc = IDMC_COMPONENTS["cdgc"]
        assert "business_glossary" in cdgc["artifact_types"]
        assert "data_domain" in cdgc["artifact_types"]


# ─────────────────────────────────────────────
#  DD10: IDMC Client
# ─────────────────────────────────────────────

class TestIDMCClient:

    def test_create_client(self, idmc_config):
        client = IDMCClient(idmc_config)
        assert client.base_url == "https://dm-us.informaticacloud.com"
        assert not client.is_connected()

    def test_offline_mode(self):
        client = IDMCClient({})
        assert client.authenticate() is False
        assert not client.is_connected()

    def test_list_objects_offline(self, idmc_config):
        client = IDMCClient(idmc_config)
        result = client.list_objects("cdi")
        assert result == []  # Not connected


# ─────────────────────────────────────────────
#  DD10: Component Parsers
# ─────────────────────────────────────────────

class TestComponentParser:

    def test_parse_export_list(self):
        parser = ComponentParser("cdi")
        data = [
            {"id": "1", "name": "M_LOAD", "type": "mapping", "description": "Load customers"},
            {"id": "2", "name": "TF_DAILY", "type": "taskflow"},
        ]
        result = parser.parse_export(data)
        assert len(result) == 2
        assert result[0]["component"] == "cdi"
        assert result[0]["name"] == "M_LOAD"

    def test_parse_export_dict(self):
        parser = ComponentParser("cdgc")
        data = {"objects": [{"id": "1", "name": "Term1", "type": "glossary_term"}]}
        result = parser.parse_export(data)
        assert len(result) == 1

    def test_parse_empty(self):
        parser = ComponentParser("cdi")
        result = parser.parse_export([])
        assert result == []


class TestCDGCParser:

    def test_parse_glossary(self):
        parser = CDGCParser()
        data = {
            "terms": [
                {"name": "Customer ID", "definition": "Unique identifier", "domain": "CRM"},
                {"name": "Revenue", "definition": "Total revenue", "domain": "Finance"},
            ]
        }
        result = parser.parse_glossary(data)
        assert len(result) == 2
        assert result[0]["type"] == "business_term"
        assert result[0]["domain"] == "CRM"

    def test_parse_lineage(self):
        parser = CDGCParser()
        data = {
            "links": [
                {"source": {"table": "src"}, "target": {"table": "tgt"},
                 "transformation": "filter"},
            ]
        }
        result = parser.parse_lineage(data)
        assert len(result) == 1
        assert result[0]["type"] == "lineage_link"


class TestCDQParser:

    def test_parse_rules(self):
        parser = CDQParser()
        data = {
            "rules": [
                {"name": "NotNull_Email", "ruleType": "completeness",
                 "expression": "IS NOT NULL", "threshold": 95},
            ]
        }
        result = parser.parse_rules(data)
        assert len(result) == 1
        assert result[0]["rule_type"] == "completeness"
        assert result[0]["threshold"] == 95


class TestMDMParser:

    def test_parse_entities(self):
        parser = MDMParser()
        data = {
            "entities": [
                {"name": "Customer", "attributes": ["id", "name", "email"],
                 "matchRules": [{"type": "fuzzy"}]},
            ]
        }
        result = parser.parse_entities(data)
        assert len(result) == 1
        assert result[0]["type"] == "business_entity"
        assert len(result[0]["attributes"]) == 3


# ─────────────────────────────────────────────
#  DD10: Inventory Builder
# ─────────────────────────────────────────────

class TestIDMCInventoryBuilder:

    def test_build_empty_inventory(self, idmc_config):
        builder = IDMCInventoryBuilder(idmc_config)
        inv = builder.build_inventory()
        assert len(inv) > 0
        for comp_id, comp_data in inv.items():
            assert "component_name" in comp_data
            assert "object_count" in comp_data

    def test_build_from_export_dir(self, idmc_config, tmp_path):
        # Create export file
        export = tmp_path / "cdi_export.json"
        export.write_text(json.dumps([
            {"id": "1", "name": "M_LOAD", "type": "mapping"},
            {"id": "2", "name": "M_ETL", "type": "mapping"},
        ]))
        builder = IDMCInventoryBuilder(idmc_config)
        inv = builder.build_inventory(export_dir=str(tmp_path))
        assert inv["cdi"]["object_count"] == 2

    def test_complexity_report(self, idmc_config, tmp_path):
        export = tmp_path / "cdi_export.json"
        export.write_text(json.dumps([{"id": str(i), "name": f"M_{i}", "type": "mapping"}
                                       for i in range(15)]))
        builder = IDMCInventoryBuilder(idmc_config)
        builder.build_inventory(export_dir=str(tmp_path))
        report = builder.get_complexity_report()
        assert report["total_objects"] >= 15
        assert "complexity_distribution" in report

    def test_dependency_map(self, idmc_config):
        builder = IDMCInventoryBuilder(idmc_config)
        builder.build_inventory()
        dep_map = builder.get_dependency_map()
        assert "nodes" in dep_map
        assert "edges" in dep_map

    def test_save_inventory(self, idmc_config, tmp_path):
        builder = IDMCInventoryBuilder(idmc_config)
        builder.build_inventory()
        paths = builder.save_inventory(output_dir=str(tmp_path / "inv"))
        assert os.path.isfile(paths["inventory"])
        assert os.path.isfile(paths["complexity_report"])
        assert os.path.isfile(paths["dependency_map"])


# ─────────────────────────────────────────────
#  DD11: Review Configuration
# ─────────────────────────────────────────────

class TestLoadReviewConfig:

    def test_defaults(self):
        cfg = load_review_config({})
        assert cfg["merge_threshold"] == 0.8
        assert cfg["auto_apply"] is False

    def test_from_config(self, idmc_config):
        cfg = load_review_config(idmc_config)
        assert cfg["merge_threshold"] == 0.8
        assert cfg["anti_patterns"] is True


# ─────────────────────────────────────────────
#  DD11: Duplicate Detection
# ─────────────────────────────────────────────

class TestDuplicateDetector:

    def test_fingerprint(self):
        det = DuplicateDetector()
        fp = det.fingerprint("SELECT * FROM table")
        assert isinstance(fp, str)
        assert len(fp) == 64  # SHA256

    def test_same_content_same_fingerprint(self):
        det = DuplicateDetector()
        fp1 = det.fingerprint("SELECT id FROM users")
        fp2 = det.fingerprint("SELECT id FROM users")
        assert fp1 == fp2

    def test_different_content_different_fingerprint(self):
        det = DuplicateDetector()
        fp1 = det.fingerprint("SELECT id FROM users")
        fp2 = det.fingerprint("SELECT name FROM products")
        assert fp1 != fp2

    def test_find_duplicates(self, sample_output_dir):
        det = DuplicateDetector()
        nb_dir = os.path.join(sample_output_dir, "notebooks")
        dups = det.find_duplicates(directory=nb_dir)
        assert len(dups) >= 1
        assert dups[0]["count"] == 2  # V1 and V2 are identical

    def test_fingerprint_file(self, sample_output_dir):
        det = DuplicateDetector()
        fp = det.fingerprint_file(os.path.join(sample_output_dir, "notebooks", "NB_LOAD_CUSTOMERS.py"))
        assert fp is not None


# ─────────────────────────────────────────────
#  DD11: Similarity Scorer
# ─────────────────────────────────────────────

class TestSimilarityScorer:

    def test_identical_content(self):
        scorer = SimilarityScorer()
        assert scorer.score("hello world", "hello world") == 1.0

    def test_completely_different(self):
        scorer = SimilarityScorer()
        score = scorer.score("alpha beta gamma", "x y z")
        assert score == 0.0

    def test_partial_similarity(self):
        scorer = SimilarityScorer()
        score = scorer.score(
            "SELECT id, name FROM customers WHERE active = 1",
            "SELECT id, email FROM customers WHERE deleted = 0",
        )
        assert 0 < score < 1

    def test_empty_content(self):
        scorer = SimilarityScorer()
        assert scorer.score("", "") == 1.0
        assert scorer.score("hello", "") == 0.0

    def test_find_similar_pairs(self, sample_output_dir):
        scorer = SimilarityScorer()
        nb_dir = os.path.join(sample_output_dir, "notebooks")
        files = [os.path.join(nb_dir, f) for f in os.listdir(nb_dir)]
        pairs = scorer.find_similar_pairs(files, threshold=0.5)
        assert len(pairs) >= 1


# ─────────────────────────────────────────────
#  DD11: Anti-Pattern Detection
# ─────────────────────────────────────────────

class TestAntiPatternDetector:

    def test_detect_select_star(self):
        detector = AntiPatternDetector()
        findings = detector.scan_content("SELECT * FROM sales_fact WHERE year = 2024")
        ids = [f["id"] for f in findings]
        assert "AP002" in ids

    def test_detect_hardcoded_credentials(self):
        detector = AntiPatternDetector()
        findings = detector.scan_content('password = "super_secret_123"')
        ids = [f["id"] for f in findings]
        assert "AP006" in ids

    def test_detect_unbounded_collect(self):
        detector = AntiPatternDetector()
        findings = detector.scan_content("result = df.collect()")
        ids = [f["id"] for f in findings]
        assert "AP005" in ids

    def test_clean_content(self):
        detector = AntiPatternDetector()
        findings = detector.scan_content("df.select('id', 'name').filter(col('active') == 1)")
        assert len(findings) == 0

    def test_scan_directory(self, sample_output_dir):
        detector = AntiPatternDetector()
        results = detector.scan_directory(sample_output_dir)
        assert results["total"] > 0
        assert "by_severity" in results

    def test_scan_file(self, sample_output_dir):
        detector = AntiPatternDetector()
        path = os.path.join(sample_output_dir, "sql", "SQL_RANKINGS.sql")
        findings = detector.scan_file(path)
        assert any(f["id"] == "AP002" for f in findings)

    def test_line_numbers(self):
        detector = AntiPatternDetector()
        content = "line1\nline2\nSELECT * FROM table\nline4"
        findings = detector.scan_content(content)
        assert findings[0]["line"] == 3


# ─────────────────────────────────────────────
#  DD11: Consolidation Recommender
# ─────────────────────────────────────────────

class TestConsolidationRecommender:

    def test_analyze(self, sample_output_dir):
        recommender = ConsolidationRecommender(threshold=0.7)
        result = recommender.analyze(sample_output_dir)
        assert "duplicates" in result
        assert "similar_pairs" in result
        assert "recommendations" in result
        assert result["merge_candidates"] >= 1


# ─────────────────────────────────────────────
#  DD11: SQL Optimizer
# ─────────────────────────────────────────────

class TestSQLOptimizer:

    def test_detect_select_star(self):
        opt = SQLOptimizer()
        sug = opt.analyze_sql("SELECT * FROM orders")
        assert any(s["rule_id"] == "OPT001" for s in sug)

    def test_detect_not_in(self):
        opt = SQLOptimizer()
        sug = opt.analyze_sql("SELECT id FROM a WHERE id NOT IN (SELECT id FROM b)")
        assert any(s["rule_id"] == "OPT002" for s in sug)

    def test_detect_leading_wildcard(self):
        opt = SQLOptimizer()
        sug = opt.analyze_sql("SELECT * FROM t WHERE name LIKE '%john%'")
        assert any(s["rule_id"] == "OPT006" for s in sug)

    def test_clean_sql(self):
        opt = SQLOptimizer()
        sug = opt.analyze_sql("SELECT id, name FROM customers WHERE id = 1")
        assert len(sug) == 0

    def test_analyze_directory(self, sample_output_dir):
        opt = SQLOptimizer()
        sug = opt.analyze_directory(os.path.join(sample_output_dir, "sql"))
        assert len(sug) > 0


# ─────────────────────────────────────────────
#  DD11: Rework Classifier
# ─────────────────────────────────────────────

class TestReworkClassifier:

    def test_detect_todo(self):
        cls = ReworkClassifier()
        items = cls.classify_content("# TODO: Handle edge case")
        assert len(items) == 1
        assert items[0]["marker"] == "TODO"
        assert items[0]["severity"] == "medium"

    def test_detect_fixme(self):
        cls = ReworkClassifier()
        items = cls.classify_content("# FIXME: Performance issue")
        assert len(items) == 1
        assert items[0]["severity"] == "high"

    def test_detect_not_supported(self):
        cls = ReworkClassifier()
        items = cls.classify_content("# NOT SUPPORTED: Custom transformation")
        assert len(items) == 1
        assert items[0]["severity"] == "critical"

    def test_clean_content(self):
        cls = ReworkClassifier()
        items = cls.classify_content("df = spark.read.format('delta').load('path')")
        assert len(items) == 0

    def test_classify_directory(self, sample_output_dir):
        cls = ReworkClassifier()
        result = cls.classify_directory(sample_output_dir)
        assert result["total"] >= 2  # TODO and FIXME in NB_COMPLEX.py
        assert "by_severity" in result

    def test_line_numbers(self):
        cls = ReworkClassifier()
        content = "line1\n# TODO: fix\nline3"
        items = cls.classify_content(content)
        assert items[0]["line"] == 2


# ─────────────────────────────────────────────
#  DD11: Review Queue Manager
# ─────────────────────────────────────────────

class TestReviewQueueManager:

    def test_add_item(self):
        mgr = ReviewQueueManager()
        item = mgr.add_item("Merge duplicates", "merge", "medium")
        assert item["id"] == "REV-0001"
        assert item["status"] == "pending"

    def test_priority_ordering(self):
        mgr = ReviewQueueManager()
        mgr.add_item("Low priority", "optimize", "low")
        mgr.add_item("Critical fix", "rework", "critical")
        mgr.add_item("Medium task", "merge", "medium")
        queue = mgr.get_queue(sort_by_priority=True)
        assert queue[0]["severity"] == "critical"
        assert queue[-1]["severity"] == "low"

    def test_filter_by_category(self):
        mgr = ReviewQueueManager()
        mgr.add_item("A", "merge", "medium")
        mgr.add_item("B", "optimize", "low")
        mgr.add_item("C", "merge", "high")
        merges = mgr.get_queue(category="merge")
        assert len(merges) == 2

    def test_filter_by_status(self):
        mgr = ReviewQueueManager()
        mgr.add_item("A", "merge", "medium")
        mgr.add_item("B", "optimize", "low")
        mgr.update_status("REV-0001", "approved")
        pending = mgr.get_queue(status="pending")
        assert len(pending) == 1

    def test_update_status(self):
        mgr = ReviewQueueManager()
        mgr.add_item("A", "merge", "medium")
        updated = mgr.update_status("REV-0001", "approved", "Looks good")
        assert updated["status"] == "approved"

    def test_update_nonexistent(self):
        mgr = ReviewQueueManager()
        result = mgr.update_status("BOGUS", "approved")
        assert result is None

    def test_get_summary(self):
        mgr = ReviewQueueManager()
        mgr.add_item("A", "merge", "medium")
        mgr.add_item("B", "optimize", "low")
        mgr.add_item("C", "rework", "high")
        summary = mgr.get_summary()
        assert summary["total"] == 3
        assert summary["by_category"]["merge"] == 1


# ─────────────────────────────────────────────
#  DD11: Integrated Review
# ─────────────────────────────────────────────

class TestRunMigrationReview:

    def test_full_review(self, sample_output_dir):
        results = run_migration_review(output_dir=sample_output_dir)
        assert "duplicates" in results
        assert "anti_patterns" in results
        assert "rework" in results
        assert "queue" in results
        assert results["queue"]["total"] > 0

    def test_review_saves_report(self, sample_output_dir):
        run_migration_review(output_dir=sample_output_dir)
        report_path = os.path.join(sample_output_dir, "migration_review_report.json")
        assert os.path.isfile(report_path)

    def test_review_finds_duplicates(self, sample_output_dir):
        results = run_migration_review(output_dir=sample_output_dir)
        assert len(results["duplicates"]) >= 1

    def test_review_finds_anti_patterns(self, sample_output_dir):
        results = run_migration_review(output_dir=sample_output_dir)
        assert results["anti_patterns"]["total"] > 0

    def test_review_finds_rework(self, sample_output_dir):
        results = run_migration_review(output_dir=sample_output_dir)
        assert results["rework"]["total"] >= 2  # TODO + FIXME
