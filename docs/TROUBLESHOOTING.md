# Troubleshooting Guide

## Common Issues

### 1. "inventory.json not found"

**Error:** `ERROR: output/inventory/inventory.json not found. Run run_assessment.py first.`

**Cause:** Phases 1–4 depend on the assessment output.

**Fix:** Run assessment first:
```bash
python run_assessment.py
# Then run the failing phase
```

### 2. No Mappings/Workflows Found

**Symptoms:** `Total mappings found: 0` or `Total workflows found: 0`

**Possible causes:**
- XML files are not in the correct directory (`input/mappings/` or `input/workflows/`)
- Files don't have `.xml` extension
- XML is malformed or from an unsupported Informatica version

**Fix:**
1. Verify files exist: `ls input/mappings/*.xml`
2. Check XML validity: open in a browser or XML editor
3. Ensure the XML root element is `<POWERMART>`, `<REPOSITORY>`, or contains IICS elements

### 3. Unicode/Encoding Errors

**Error:** `UnicodeDecodeError` when parsing XML files

**Fix:** The tool uses `utf-8` with `errors="replace"`. If you see garbled characters in output:
1. Re-export from Informatica with UTF-8 encoding
2. Or convert the file: `iconv -f ISO-8859-1 -t UTF-8 input.xml > output.xml`

### 4. SQL Conversion Incomplete

**Symptoms:** Many `-- TODO:` comments in converted SQL

**Expected:** Complex constructs that have no direct Spark SQL equivalent are flagged with TODO markers. These require manual review:

| Oracle Construct | Spark Equivalent | Manual Action |
|---|---|---|
| `DBMS_OUTPUT.PUT_LINE` | `print()` in PySpark | Move to notebook cell |
| `(+)` outer join | `LEFT/RIGHT JOIN` | Rewrite join syntax |
| `@dblink` | `spark.read.jdbc()` | Configure JDBC connection |
| `CREATE MATERIALIZED VIEW` | Delta table + pipeline | Create scheduled refresh |
| `CREATE GLOBAL TEMPORARY TABLE` | `createOrReplaceTempView()` | Auto-converted |

### 5. Empty Notebooks or Pipelines

**Symptoms:** Generated `.py` or `.json` files are nearly empty

**Possible cause:** Mapping/workflow metadata wasn't fully extracted during assessment.

**Fix:**
1. Check `output/inventory/inventory.json` — verify the mapping/workflow has data
2. Look for warnings in the assessment output
3. Re-run assessment with verbose logging

### 6. Pipeline Dependencies Wrong

**Symptoms:** Pipeline JSON has incorrect `dependsOn` chains

**Possible cause:** Workflow link conditions or session-to-mapping references are ambiguous.

**Fix:**
1. Review `output/inventory/dependency_dag.json`
2. Check the original workflow XML link elements
3. Manually adjust `PL_*.json` activity dependencies

### 7. Test Failures After Code Changes

```bash
# Run the full test suite
py -m pytest --tb=short -v

# Run only gap tests
py -m pytest tests/test_gaps.py -v

# Run with coverage
py -m pytest --cov=. --cov-report=term-missing
```

**If tests fail after your changes:**
1. Read the failure message — it usually points to the exact assertion
2. Check if you modified a function signature that tests depend on
3. Run `py -m pytest tests/test_coverage.py -v` for unit tests
4. Run `py -m pytest tests/test_e2e.py -v` for integration tests

### 8. Dashboard Won't Open

**Error:** `ModuleNotFoundError: No module named 'dash'`

**Fix:** The dashboard is optional. Install extra dependencies:
```bash
pip install dash plotly
python dashboard.py --open
```

### 9. Deploy Script Authentication Error

**Error:** 401/403 when running `deploy_to_fabric.py`

**Fix:**
1. Ensure you have `azure-identity` installed: `pip install azure-identity`
2. Run `az login` to authenticate
3. Verify your account has Contributor access to the Fabric workspace

### 10. IICS XML Not Recognized

**Symptoms:** IICS exports parsed as PowerCenter format

**Fix:** The tool uses `detect_xml_format()` which checks for IICS-specific elements. Ensure your export:
- Contains `<dTemplate>` or `<connection>` elements (IICS format markers)
- Isn't a partial export — include the full taskflow XML

### 11. Databricks: Unity Catalog Table Not Found

**Error:** `AnalysisException: Table or view not found: catalog.schema.table`

**Cause:** Generated notebooks use 3-level namespace (`catalog.schema.table`) which requires Unity Catalog.

**Fix:**
1. Verify Unity Catalog is enabled on your Databricks workspace
2. Check the catalog and schema exist: `SHOW SCHEMAS IN my_catalog`
3. If using Hive metastore, switch table references to 2-level (`schema.table`)

### 12. Databricks: dbutils.widgets.get() Fails

**Error:** `InputWidgetNotDefined: No input widget named 'load_date'`

**Cause:** Job parameters not passed to the notebook.

**Fix:**
1. When using Databricks Jobs API, set `base_parameters` in the notebook task config
2. For interactive runs, add a `dbutils.widgets.text("load_date", "")` default cell
3. Check that `--target databricks` was used during migration (produces `dbutils` code, not `notebookutils`)

### 13. Databricks: Workflow JSON Rejected by Jobs API

**Error:** `400 Bad Request` when creating a job via `databricks jobs create`

**Cause:** Generated JSON may contain Fabric-specific fields.

**Fix:**
1. Ensure migration ran with `--target databricks`
2. Check for `type: "NotebookActivity"` (Fabric) vs `notebook_task` (Databricks) in the JSON
3. Re-run pipeline migration: `python run_pipeline_migration.py --target databricks`

### 14. Databricks: Secret Scope Not Configured

**Error:** `SecretDoesNotExistException: Secret does not exist with scope: ... and key: ...`

**Cause:** Notebooks use `dbutils.secrets.get(scope=..., key=...)` but the secret scope isn't configured.

**Fix:**
1. Create the secret scope: `databricks secrets create-scope --scope my-scope`
2. Add secrets: `databricks secrets put --scope my-scope --key my-secret`
3. Or switch to Key Vault-backed scope for Azure-native integration

### 15. AutoSys JIL Parsing Errors

**Symptoms:** `Total AutoSys jobs: 0` or missing jobs in output

**Possible causes:**
- `.jil` files are not in the expected directory (`input/autosys/`)
- JIL syntax uses unsupported extensions or non-standard keywords
- File encoding is not UTF-8

**Fix:**
1. Verify files exist: `ls input/autosys/*.jil`
2. Check that each job starts with `insert_job:` on its own line
3. Ensure each job has a `job_type:` attribute (b = BOX, c = CMD, fw = FW, ft = FT)
4. Re-export from AutoSys: `autorep -J ALL -q > all_jobs.jil`

### 16. AutoSys Conditions Not Resolved

**Symptoms:** Pipeline dependencies are missing or `dependsOn` is empty

**Cause:** Condition expressions reference jobs that aren't in the parsed JIL files.

**Fix:**
1. Check `output/autosys/autosys_summary.json` for `unresolved_conditions`
2. Ensure all referenced jobs are included in the JIL export
3. For cross-BOX dependencies, export the parent BOX jobs as well

### 17. AutoSys pmcmd Linkage Failed

**Symptoms:** `"linked": false` in `autosys_summary.json` for jobs that call Informatica

**Cause:** The workflow name in `pmcmd startworkflow -w WF_NAME` doesn't match inventory entries.

**Fix:**
1. Check the exact `command:` in the JIL — it must contain `pmcmd startworkflow -w <workflow_name>`
2. Verify `output/inventory/inventory.json` contains the referenced workflow
3. Run assessment first: `python run_assessment.py` then re-run AutoSys migration

### 18. DBT Model Generation Errors

**Symptoms:** Empty or malformed `.sql` files in `output/dbt/models/`

**Possible causes:**
- Mapping is too complex for SQL-only representation (requires PySpark)
- Missing source table metadata in inventory

**Fix:**
1. Use `--target auto` instead of `--target dbt` — complex mappings will route to PySpark notebooks automatically
2. Check `output/dbt/models/` for `-- TODO:` markers indicating manual review needed
3. Verify source tables are present in `output/inventory/inventory.json`

### 19. DBT Project Won't Compile

**Error:** `dbt compile` fails with schema or reference errors

**Fix:**
1. Ensure `profiles.yml` has the correct Databricks connection details
2. Check `dbt_project.yml` model paths match the generated structure
3. Install the Databricks adapter: `pip install dbt-databricks`
4. Verify catalog/schema: `dbt debug --target dev`

---

## Getting Help

1. Check `output/inventory/parse_issues.json` for structured error details
2. Review `output/migration_issues.md` for flagged migration issues
3. Open an issue at https://github.com/cyphou/InformaticaToFabric/issues
