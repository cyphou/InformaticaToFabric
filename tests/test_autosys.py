"""
Tests for AutoSys JIL Migration — Sprint 61 (Phase 5)
Covers: JIL parsing, job classification, dependency graph,
condition parsing, schedule/cron conversion, Informatica linkage,
pipeline/workflow generation, standalone chains, and E2E flow.
"""

import json
import os
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import run_autosys_migration as mod


# ═════════════════════════════════════════════
#  JIL Parsing
# ═════════════════════════════════════════════

class TestJILParser:
    """Tests for parse_jil()."""

    def test_parse_single_job(self):
        jil = """
insert_job: CMD_TEST
job_type: CMD
command: echo hello
machine: SERVER_01
"""
        jobs, cals = mod.parse_jil(jil)
        assert len(jobs) == 1
        assert jobs[0]["name"] == "CMD_TEST"
        assert jobs[0]["job_type"] == "CMD"
        assert jobs[0]["command"] == "echo hello"

    def test_parse_multiple_jobs(self):
        jil = """
insert_job: JOB_A
job_type: BOX
owner: admin

insert_job: JOB_B
job_type: CMD
box_name: JOB_A
command: ls -la
"""
        jobs, _ = mod.parse_jil(jil)
        assert len(jobs) == 2
        assert jobs[0]["name"] == "JOB_A"
        assert jobs[1]["box_name"] == "JOB_A"

    def test_parse_strips_comments(self):
        jil = """
/* This is a comment */
insert_job: CMD_X
job_type: CMD
/* inline */ command: test
"""
        jobs, _ = mod.parse_jil(jil)
        assert len(jobs) == 1
        assert jobs[0]["name"] == "CMD_X"

    def test_parse_quoted_values(self):
        jil = """
insert_job: JOB_Q
job_type: CMD
start_times: "06:30"
description: "This is a test job"
"""
        jobs, _ = mod.parse_jil(jil)
        assert jobs[0]["start_times"] == "06:30"
        assert jobs[0]["description"] == "This is a test job"

    def test_parse_calendar(self):
        jil = """
insert_calendar: BIZ_DAYS
description: "Business days"

insert_job: JOB_Z
job_type: CMD
command: echo done
"""
        jobs, cals = mod.parse_jil(jil)
        assert len(cals) == 1
        assert cals[0]["name"] == "BIZ_DAYS"
        assert cals[0]["description"] == "Business days"
        assert len(jobs) == 1

    def test_parse_empty_input(self):
        jobs, cals = mod.parse_jil("")
        assert jobs == []
        assert cals == []

    def test_parse_comments_only(self):
        jil = """
/* nothing here */
/* just comments */
"""
        jobs, cals = mod.parse_jil(jil)
        assert jobs == []

    def test_parse_condition_attribute(self):
        jil = """
insert_job: JOB_DEP
job_type: CMD
condition: s(JOB_PARENT) AND s(JOB_OTHER)
command: echo dep
"""
        jobs, _ = mod.parse_jil(jil)
        assert jobs[0]["condition"] == "s(JOB_PARENT) AND s(JOB_OTHER)"

    def test_parse_file_watcher(self):
        jil = """
insert_job: FW_FILES
job_type: FW
watch_file: /data/incoming/*.csv
watch_interval: 120
machine: SERVER_02
"""
        jobs, _ = mod.parse_jil(jil)
        assert jobs[0]["job_type"] == "FW"
        assert jobs[0]["watch_file"] == "/data/incoming/*.csv"
        assert jobs[0]["watch_interval"] == "120"

    def test_parse_alarm_and_notification(self):
        jil = """
insert_job: JOB_ALARM
job_type: CMD
command: test
alarm_if_fail: 1
send_notification: 1
notification_msg: "Job failed!"
"""
        jobs, _ = mod.parse_jil(jil)
        assert jobs[0]["alarm_if_fail"] == "1"
        assert jobs[0]["notification_msg"] == "Job failed!"


# ═════════════════════════════════════════════
#  Job Classification
# ═════════════════════════════════════════════

class TestClassifyJob:

    def test_cmd_default(self):
        assert mod.classify_job({"name": "X"}) == "CMD"

    def test_cmd_explicit(self):
        assert mod.classify_job({"name": "X", "job_type": "CMD"}) == "CMD"

    def test_box(self):
        assert mod.classify_job({"name": "X", "job_type": "BOX"}) == "BOX"

    def test_fw(self):
        assert mod.classify_job({"name": "X", "job_type": "FW"}) == "FW"

    def test_ft(self):
        assert mod.classify_job({"name": "X", "job_type": "FT"}) == "FT"

    def test_case_insensitive(self):
        assert mod.classify_job({"name": "X", "job_type": "box"}) == "BOX"

    def test_unknown_type_defaults_cmd(self):
        assert mod.classify_job({"name": "X", "job_type": "UNKNOWN"}) == "CMD"


# ═════════════════════════════════════════════
#  Condition Parsing
# ═════════════════════════════════════════════

class TestParseCondition:

    def test_success_condition(self):
        deps = mod.parse_condition("s(JOB_A)")
        assert len(deps) == 1
        assert deps[0] == {"job": "JOB_A", "status": "success"}

    def test_failure_condition(self):
        deps = mod.parse_condition("f(JOB_B)")
        assert deps[0]["status"] == "failure"

    def test_notrunning_condition(self):
        deps = mod.parse_condition("n(JOB_C)")
        assert deps[0]["status"] == "notrunning"

    def test_done_condition(self):
        deps = mod.parse_condition("d(JOB_D)")
        assert deps[0]["status"] == "done"

    def test_compound_and(self):
        deps = mod.parse_condition("s(JOB_A) AND s(JOB_B)")
        assert len(deps) == 2
        assert deps[0]["job"] == "JOB_A"
        assert deps[1]["job"] == "JOB_B"

    def test_compound_or(self):
        deps = mod.parse_condition("s(JOB_X) OR f(JOB_Y)")
        assert len(deps) == 2
        assert deps[0]["status"] == "success"
        assert deps[1]["status"] == "failure"

    def test_empty_condition(self):
        assert mod.parse_condition("") == []
        assert mod.parse_condition(None) == []

    def test_mixed_conditions(self):
        deps = mod.parse_condition("s(A) AND f(B) AND n(C)")
        assert len(deps) == 3
        statuses = [d["status"] for d in deps]
        assert "success" in statuses
        assert "failure" in statuses
        assert "notrunning" in statuses


# ═════════════════════════════════════════════
#  Dependency Graph
# ═════════════════════════════════════════════

class TestDependencyGraph:

    def test_builds_graph(self):
        jobs = [
            {"name": "A", "condition": ""},
            {"name": "B", "condition": "s(A)"},
            {"name": "C", "condition": "s(A) AND s(B)"},
        ]
        graph = mod.build_dependency_graph(jobs)
        assert graph["A"] == []
        assert len(graph["B"]) == 1
        assert graph["B"][0]["job"] == "A"
        assert len(graph["C"]) == 2

    def test_graph_with_box(self):
        jobs = [
            {"name": "BOX_X", "job_type": "BOX"},
            {"name": "CMD_Y", "job_type": "CMD", "box_name": "BOX_X", "condition": ""},
        ]
        graph = mod.build_dependency_graph(jobs)
        assert "BOX_X" in graph
        assert "CMD_Y" in graph


# ═════════════════════════════════════════════
#  Schedule / Cron Conversion
# ═════════════════════════════════════════════

class TestScheduleConversion:

    def test_basic_schedule(self):
        job = {
            "name": "J",
            "date_conditions": "1",
            "days_of_week": "mo,tu,we,th,fr",
            "start_times": "02:00",
        }
        result = mod.resolve_schedule(job)
        assert result["cron"] == "00 02 * * 1,2,3,4,5"

    def test_all_days(self):
        job = {
            "name": "J",
            "date_conditions": "1",
            "days_of_week": "all",
            "start_times": "12:30",
        }
        result = mod.resolve_schedule(job)
        assert result["cron"] == "30 12 * * *"

    def test_with_start_mins(self):
        job = {
            "name": "J",
            "date_conditions": "1",
            "days_of_week": "all",
            "start_times": "23:00",
            "start_mins": "0,15,30,45",
        }
        result = mod.resolve_schedule(job)
        assert result["cron"] == "0,15,30,45 23 * * *"

    def test_with_calendar(self):
        job = {
            "name": "J",
            "date_conditions": "1",
            "days_of_week": "mo,tu,we,th,fr",
            "start_times": "06:00",
            "run_calendar": "BUSINESS_DAYS_US",
        }
        result = mod.resolve_schedule(job)
        assert "BUSINESS_DAYS_US" in result["note"]
        assert result["original"]["run_calendar"] == "BUSINESS_DAYS_US"

    def test_no_schedule(self):
        job = {"name": "J"}
        result = mod.resolve_schedule(job)
        assert result == {}

    def test_weekend_days(self):
        job = {
            "name": "J",
            "date_conditions": "1",
            "days_of_week": "sa,su",
            "start_times": "08:00",
        }
        result = mod.resolve_schedule(job)
        assert result["cron"] == "00 08 * * 6,0"


# ═════════════════════════════════════════════
#  Informatica Linkage
# ═════════════════════════════════════════════

class TestInformaticaLinkage:

    def test_extract_workflow_name(self):
        cmd = "pmcmd startworkflow -sv INT_SVC -d DOMAIN -u admin -p *** -f SALES -w WF_DAILY_SALES_LOAD -wait"
        result = mod.extract_informatica_workflow(cmd)
        assert result["workflow"] == "WF_DAILY_SALES_LOAD"
        assert result["folder"] == "SALES"

    def test_extract_without_folder(self):
        cmd = "pmcmd startworkflow -sv SVC -d DOM -u u -p p -w WF_TEST"
        result = mod.extract_informatica_workflow(cmd)
        assert result["workflow"] == "WF_TEST"
        assert result["folder"] is None

    def test_no_pmcmd(self):
        cmd = "/opt/scripts/run_report.sh"
        result = mod.extract_informatica_workflow(cmd)
        assert result is None

    def test_empty_command(self):
        assert mod.extract_informatica_workflow("") is None
        assert mod.extract_informatica_workflow(None) is None

    def test_link_jobs_to_workflows_with_inventory(self):
        jobs = [
            {"name": "J1", "job_type": "CMD", "command": "pmcmd startworkflow -sv S -d D -u u -p p -f F -w WF_DAILY_SALES_LOAD"},
            {"name": "J2", "job_type": "CMD", "command": "echo hello"},
            {"name": "J3", "job_type": "BOX"},
        ]
        inv_wfs = [{"name": "WF_DAILY_SALES_LOAD"}, {"name": "WF_OTHER"}]
        linkage = mod.link_jobs_to_workflows(jobs, inv_wfs)
        assert "J1" in linkage
        assert linkage["J1"]["linked"] is True
        assert "J2" not in linkage

    def test_link_jobs_unlinked(self):
        jobs = [
            {"name": "J1", "job_type": "CMD", "command": "pmcmd startworkflow -sv S -d D -u u -p p -w WF_MISSING"},
        ]
        inv_wfs = [{"name": "WF_DAILY_SALES_LOAD"}]
        linkage = mod.link_jobs_to_workflows(jobs, inv_wfs)
        assert linkage["J1"]["linked"] is False


# ═════════════════════════════════════════════
#  Pipeline Generation (Fabric)
# ═════════════════════════════════════════════

class TestFabricPipelineGeneration:

    def test_generates_pipeline(self):
        jobs = [
            {"name": "CMD_A", "job_type": "CMD", "command": "pmcmd startworkflow -sv S -d D -u u -p p -w WF_X", "description": "Run WF", "alarm_if_fail": "1"},
            {"name": "CMD_B", "job_type": "CMD", "command": "/opt/scripts/test.sh", "description": "Post step"},
        ]
        dep_graph = {"CMD_A": [], "CMD_B": [{"job": "CMD_A", "status": "success"}]}
        linkage = {"CMD_A": {"workflow": "WF_X", "folder": None, "linked": True}}

        pipeline = mod.generate_fabric_pipeline("TEST_BOX", jobs, dep_graph, linkage, {})
        assert pipeline["name"] == "PL_AUTOSYS_TEST_BOX"
        acts = pipeline["properties"]["activities"]
        assert len(acts) == 2
        assert acts[0]["type"] == "Notebook"
        assert "WF_X" in acts[0]["description"]
        assert acts[1]["type"] == "Script"

    def test_pipeline_with_schedule(self):
        pipeline = mod.generate_fabric_pipeline("BOX_S", [], {}, {}, {"cron": "0 2 * * 1,2,3,4,5", "note": "test"})
        assert "trigger" in pipeline["properties"]
        assert pipeline["properties"]["trigger"]["typeProperties"]["recurrence"]["cron"] == "0 2 * * 1,2,3,4,5"

    def test_pipeline_annotations(self):
        pipeline = mod.generate_fabric_pipeline("BOX_A", [], {}, {}, {})
        annotations = pipeline["properties"]["annotations"]
        assert "MigratedFromAutoSys" in annotations
        assert "OriginalBox:BOX_A" in annotations

    def test_file_watcher_activity(self):
        jobs = [{"name": "FW_TEST", "job_type": "FW", "watch_file": "/data/*.csv", "description": "Watch"}]
        dep_graph = {"FW_TEST": []}
        pipeline = mod.generate_fabric_pipeline("FW_BOX", jobs, dep_graph, {}, {})
        act = pipeline["properties"]["activities"][0]
        assert act["type"] == "GetMetadata"
        assert "AutoSysFileWatcher" in act["annotations"][0]

    def test_dependson_structure(self):
        jobs = [
            {"name": "A", "job_type": "CMD", "command": "echo a", "description": ""},
            {"name": "B", "job_type": "CMD", "command": "echo b", "description": ""},
        ]
        dep_graph = {"A": [], "B": [{"job": "A", "status": "success"}]}
        pipeline = mod.generate_fabric_pipeline("DEP_BOX", jobs, dep_graph, {}, {})
        act_b = [a for a in pipeline["properties"]["activities"] if a["name"] == "B"][0]
        assert act_b["dependsOn"][0]["activity"] == "A"
        assert act_b["dependsOn"][0]["dependencyConditions"] == ["Succeeded"]

    def test_failure_dependson(self):
        jobs = [
            {"name": "A", "job_type": "CMD", "command": "echo a", "description": ""},
            {"name": "ERR", "job_type": "CMD", "command": "echo err", "description": ""},
        ]
        dep_graph = {"A": [], "ERR": [{"job": "A", "status": "failure"}]}
        pipeline = mod.generate_fabric_pipeline("FAIL_BOX", jobs, dep_graph, {}, {})
        act_err = [a for a in pipeline["properties"]["activities"] if a["name"] == "ERR"][0]
        assert act_err["dependsOn"][0]["dependencyConditions"] == ["Failed"]


# ═════════════════════════════════════════════
#  Workflow Generation (Databricks)
# ═════════════════════════════════════════════

class TestDatabricksWorkflowGeneration:

    def test_generates_workflow(self):
        jobs = [
            {"name": "CMD_A", "job_type": "CMD", "command": "pmcmd startworkflow -sv S -d D -u u -p p -w WF_Y", "description": ""},
        ]
        dep_graph = {"CMD_A": []}
        linkage = {"CMD_A": {"workflow": "WF_Y", "folder": None, "linked": True}}
        wf = mod.generate_databricks_workflow("TEST_WF", jobs, dep_graph, linkage, {})
        assert wf["name"] == "AUTOSYS_TEST_WF"
        assert len(wf["tasks"]) == 1
        assert "notebook_task" in wf["tasks"][0]
        assert "WF_Y" in wf["tasks"][0]["notebook_task"]["notebook_path"]

    def test_workflow_with_schedule(self):
        wf = mod.generate_databricks_workflow("WF_S", [], {}, {}, {"cron": "0 6 * * 1,2,3,4,5"})
        assert "schedule" in wf
        assert "quartz_cron_expression" in wf["schedule"]

    def test_workflow_tags(self):
        wf = mod.generate_databricks_workflow("WF_T", [], {}, {}, {})
        assert wf["tags"]["source"] == "autosys"


# ═════════════════════════════════════════════
#  Cron to Quartz Conversion
# ═════════════════════════════════════════════

class TestCronToQuartz:

    def test_weekday_cron(self):
        quartz = mod._cron_to_quartz("0 2 * * 1,2,3,4,5")
        assert quartz == "0 0 2 ? * 1,2,3,4,5 *"

    def test_all_days_cron(self):
        quartz = mod._cron_to_quartz("30 12 * * *")
        assert quartz == "0 30 12 * * ? *"


# ═════════════════════════════════════════════
#  Grouping & Chains
# ═════════════════════════════════════════════

class TestGroupingAndChains:

    def test_group_by_box(self):
        jobs = [
            {"name": "BOX_A", "job_type": "BOX"},
            {"name": "CMD_1", "job_type": "CMD", "box_name": "BOX_A"},
            {"name": "CMD_2", "job_type": "CMD", "box_name": "BOX_A"},
            {"name": "CMD_STANDALONE", "job_type": "CMD"},
        ]
        boxes, box_jobs, standalone = mod.group_jobs_by_box(jobs)
        assert "BOX_A" in boxes
        assert len(box_jobs["BOX_A"]) == 2
        assert len(standalone) == 1
        assert standalone[0]["name"] == "CMD_STANDALONE"

    def test_standalone_chains(self):
        standalone = [
            {"name": "ROOT"},
            {"name": "CHILD", "condition": "s(ROOT)"},
            {"name": "ORPHAN"},
        ]
        dep_graph = {
            "ROOT": [],
            "CHILD": [{"job": "ROOT", "status": "success"}],
            "ORPHAN": [],
        }
        chains = mod.build_standalone_chains(standalone, dep_graph)
        # ROOT → CHILD should be one chain, ORPHAN is separate
        assert len(chains) == 2
        chain_names = [[j["name"] for j in c] for c in chains]
        root_chain = [c for c in chain_names if "ROOT" in c][0]
        assert "CHILD" in root_chain


# ═════════════════════════════════════════════
#  E2E: Write Output
# ═════════════════════════════════════════════

class TestE2EOutput:

    @pytest.fixture
    def sample_jobs(self):
        jil = """
insert_job: BOX_SALES
job_type: BOX
date_conditions: 1
days_of_week: mo,tu,we,th,fr
start_times: "02:00"

insert_job: CMD_RUN_WF
job_type: CMD
box_name: BOX_SALES
command: pmcmd startworkflow -sv SVC -d DOM -u u -p p -f SALES -w WF_DAILY_SALES_LOAD -wait
alarm_if_fail: 1
description: "Run sales workflow"

insert_job: CMD_VALIDATE
job_type: CMD
box_name: BOX_SALES
condition: s(CMD_RUN_WF)
command: /opt/scripts/validate.sh
description: "Validate"
"""
        return mod.parse_jil(jil)

    def test_e2e_fabric(self, tmp_path, sample_jobs):
        jobs, cals = sample_jobs
        dep_graph = mod.build_dependency_graph(jobs)
        linkage = mod.link_jobs_to_workflows(jobs, [{"name": "WF_DAILY_SALES_LOAD"}])

        summary = mod.write_autosys_output(jobs, cals, dep_graph, linkage, "fabric", str(tmp_path))

        assert summary["total_jil_jobs"] == 3
        assert summary["job_types"]["BOX"] == 1
        assert summary["job_types"]["CMD"] == 2
        assert summary["pipelines_generated"] == 1
        assert summary["informatica_linkage"]["linked_to_inventory"] == 1

        # Check generated pipeline file
        pl_file = tmp_path / "PL_AUTOSYS_BOX_SALES.json"
        assert pl_file.exists()
        pipeline = json.loads(pl_file.read_text(encoding="utf-8"))
        assert pipeline["name"] == "PL_AUTOSYS_BOX_SALES"
        assert len(pipeline["properties"]["activities"]) == 2
        assert "trigger" in pipeline["properties"]

    def test_e2e_databricks(self, tmp_path, sample_jobs):
        jobs, cals = sample_jobs
        dep_graph = mod.build_dependency_graph(jobs)
        linkage = mod.link_jobs_to_workflows(jobs, [{"name": "WF_DAILY_SALES_LOAD"}])

        summary = mod.write_autosys_output(jobs, cals, dep_graph, linkage, "databricks", str(tmp_path))

        pl_file = tmp_path / "PL_AUTOSYS_BOX_SALES.json"
        assert pl_file.exists()
        wf = json.loads(pl_file.read_text(encoding="utf-8"))
        assert wf["name"] == "AUTOSYS_BOX_SALES"
        assert len(wf["tasks"]) == 2
        assert "schedule" in wf

    def test_e2e_summary_json(self, tmp_path, sample_jobs):
        jobs, cals = sample_jobs
        dep_graph = mod.build_dependency_graph(jobs)
        linkage = mod.link_jobs_to_workflows(jobs, None)

        mod.write_autosys_output(jobs, cals, dep_graph, linkage, "fabric", str(tmp_path))

        summary_path = tmp_path / "autosys_summary.json"
        assert summary_path.exists()
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert "pipelines" in summary

    def test_e2e_unlinked_detection(self, tmp_path, sample_jobs):
        jobs, cals = sample_jobs
        dep_graph = mod.build_dependency_graph(jobs)
        # Provide inventory that doesn't contain WF_DAILY_SALES_LOAD
        linkage = mod.link_jobs_to_workflows(jobs, [{"name": "WF_OTHER"}])

        summary = mod.write_autosys_output(jobs, cals, dep_graph, linkage, "fabric", str(tmp_path))
        assert summary["informatica_linkage"]["linked_to_inventory"] == 0
        assert len(summary["informatica_linkage"]["unlinked"]) == 1


# ═════════════════════════════════════════════
#  Real JIL File Parsing
# ═════════════════════════════════════════════

class TestRealJILFiles:
    """Parse the sample JIL fixtures shipped with the project."""

    @pytest.fixture
    def jil_dir(self):
        return PROJECT_ROOT / "input" / "autosys"

    def test_daily_sales_jil(self, jil_dir):
        jil = (jil_dir / "daily_sales_load.jil").read_text(encoding="utf-8")
        jobs, cals = mod.parse_jil(jil)
        names = [j["name"] for j in jobs]
        assert "BOX_DAILY_SALES_LOAD" in names
        assert "CMD_RUN_WF_DAILY_SALES" in names
        # Verify pmcmd linkage
        cmd_job = [j for j in jobs if j["name"] == "CMD_RUN_WF_DAILY_SALES"][0]
        info = mod.extract_informatica_workflow(cmd_job["command"])
        assert info["workflow"] == "WF_DAILY_SALES_LOAD"

    def test_finance_etl_jil(self, jil_dir):
        jil = (jil_dir / "finance_etl.jil").read_text(encoding="utf-8")
        jobs, cals = mod.parse_jil(jil)
        names = [j["name"] for j in jobs]
        assert "FW_FINANCE_FILE" in names
        assert "BOX_FINANCE_ETL" in names
        # Check compound condition
        consolidate = [j for j in jobs if j["name"] == "CMD_CONSOLIDATE_FINANCE"][0]
        deps = mod.parse_condition(consolidate["condition"])
        assert len(deps) == 2

    def test_standalone_jil(self, jil_dir):
        jil = (jil_dir / "standalone_jobs.jil").read_text(encoding="utf-8")
        jobs, cals = mod.parse_jil(jil)
        names = [j["name"] for j in jobs]
        assert "CMD_CUSTOMER_SYNC" in names
        assert len(cals) == 1
        assert cals[0]["name"] == "BUSINESS_DAYS_US"

    def test_all_jil_files_parse(self, jil_dir):
        """All .jil files should parse without errors."""
        for jil_file in jil_dir.glob("*.jil"):
            text = jil_file.read_text(encoding="utf-8")
            jobs, cals = mod.parse_jil(text)
            assert isinstance(jobs, list)
            assert isinstance(cals, list)


# ═════════════════════════════════════════════
#  CLI / PHASES Integration
# ═════════════════════════════════════════════

class TestCLIIntegration:

    def test_phases_includes_autosys(self):
        import run_migration
        phase_names = [p["name"] for p in run_migration.PHASES]
        assert "AutoSys Migration" in phase_names

    def test_phases_module_correct(self):
        import run_migration
        autosys_phase = [p for p in run_migration.PHASES if p["name"] == "AutoSys Migration"][0]
        assert autosys_phase["module"] == "run_autosys_migration"

    def test_autosys_phase_id(self):
        import run_migration
        autosys_phase = [p for p in run_migration.PHASES if p["name"] == "AutoSys Migration"][0]
        assert isinstance(autosys_phase["id"], int)
