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

## Getting Help

1. Check `output/inventory/parse_issues.json` for structured error details
2. Review `output/migration_issues.md` for flagged migration issues
3. Open an issue at https://github.com/cyphou/InformaticaToFabric/issues
