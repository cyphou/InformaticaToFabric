# Enterprise Migration Playbook

## 1. Executive Summary

This playbook guides enterprise-scale migration from **Informatica PowerCenter / IICS / IDMC** (Intelligent Data Management Cloud — 12 services: CDI, CDGC, CDQ, MDM, DI, B2B, API Manager, Connector, EDC, Axon, Market, Test Data) to **Microsoft Fabric** or **Azure Databricks**. It covers planning, execution, validation, and go-live for organizations with 50+ mappings and multiple environments.

---

## 2. Migration Phases

### Phase 1: Discovery & Planning (Week 1-2)

**Goal:** Understand the full scope of the Informatica estate.

1. **Export all Informatica artifacts** as XML
   - PowerCenter: Repository Manager → Export
   - IICS: Export task definitions via REST API or UI

2. **Run automated assessment**
   ```bash
   informatica-to-fabric run --only 0
   ```

3. **Review outputs:**
   - `inventory.json` — full object inventory
   - `complexity_report.md` — Simple/Medium/Complex/Custom breakdown
   - `wave_plan.json` — recommended migration wave sequence
   - `dependency_dag.json` — cross-object dependencies

4. **Estimate effort:**
   - Simple mappings: 0.5 hours each
   - Medium mappings: 2 hours each
   - Complex mappings: 8 hours each
   - Custom mappings: 16+ hours each

### Phase 2: Environment Setup (Week 2-3)

1. **Fabric workspace creation** (if targeting Fabric)
   - Create Dev / Test / Prod workspaces
   - Configure Git integration for CI/CD
   - Set up Lakehouse (Bronze / Silver / Gold)

   **Databricks alternative** (if targeting Databricks):
   - Create Dev / Test / Prod Databricks workspaces
   - Configure Unity Catalog with 3-level namespace (`catalog.schema.table`)
   - Set up Delta Lake storage (Bronze / Silver / Gold)

2. **Network connectivity**
   - On-premises data gateway for source DBs
   - VNet data gateway for private endpoints
   - Azure Key Vault for credential storage

3. **Security**
   - Entra ID service principal for deployment
   - Workspace roles (Admin, Member, Contributor, Viewer)
   - Row-Level Security (RLS) rules migration

### Phase 3: Automated Conversion (Week 3-5)

1. **Full migration run:**
   ```bash
   # Fabric target (default)
   informatica-to-fabric run --config migration.yaml --manifest

   # Databricks target
   informatica-to-fabric run --config migration.yaml --target databricks --manifest
   ```

2. **Review generated artifacts:**
   - Notebooks: `output/notebooks/NB_*.py`
   - Pipelines: `output/pipelines/PL_*.json`
   - SQL: `output/sql/SQL_*.sql`
   - Schema: `output/schema/`

3. **Address TODO markers:**
   - Search all outputs for `TODO`
   - Fill in column names, join conditions, filter logic
   - Replace placeholder JDBC URLs with actual connections

### Phase 4: Manual Remediation (Week 5-7)

1. **Complex objects** requiring manual work:
   - Stored procedures with business logic
   - Java Transformations (JTX) → rewrite in Python
   - External Procedures (EP/AEP) → rewrite or wrap
   - Custom Transformations (CT) → pandas UDF

2. **PII/DQ findings:**
   - Review `pii_findings` in inventory.json
   - Apply data masking for Highly Confidential columns
   - Implement DQ checks using Great Expectations or PySpark

### Phase 5: Testing & Validation (Week 7-9)

1. **Run validation notebooks:**
   ```bash
   informatica-to-fabric run --only 5
   ```

2. **Validation levels:**
   | Level | Check | Tool |
   |-------|-------|------|
   | L1 | Row count match | VAL notebook |
   | L2 | Column checksum | VAL notebook |
   | L3 | Sample data diff | VAL notebook |
   | L4 | Key sampling | VAL notebook |
   | L5 | Aggregate validation | VAL notebook |

3. **UAT sign-off:**
   - Business users validate report outputs
   - Performance benchmarks meet SLA
   - Data quality metrics pass thresholds

### Phase 6: Deployment (Week 9-10)

1. **Generate deployment manifest:**
   ```bash
   informatica-to-fabric run --manifest
   ```

2. **Deploy via CI/CD pipeline:**
   - **Fabric:** Push to Fabric Git-linked repo, or use `deploy_to_fabric.py`, or upload via Fabric portal
   - **Databricks:** Import notebooks via `databricks workspace import_dir`, create jobs via `databricks jobs create --json-file`, or use Databricks Repos

3. **Configure schedules:**
   - Map Informatica scheduler definitions to Fabric pipeline triggers
   - Set up alerting for pipeline failures

### Phase 7: Go-Live & Cutover (Week 10-11)

1. **Parallel run period:**
   - Run both Informatica and Fabric in parallel for 1-2 weeks
   - Compare outputs daily
   - Monitor performance and errors

2. **Cutover checklist:**
   - [ ] All validation levels pass
   - [ ] Business sign-off received
   - [ ] Rollback plan documented
   - [ ] Monitoring configured
   - [ ] On-call schedule set

3. **Decommission Informatica:**
   - Disable Informatica workflows
   - Archive XML exports
   - Update documentation

### Phase 8: Hypercare (Week 11-14)

1. **Daily monitoring:**
   - Pipeline completion rates
   - Data freshness
   - Error rates

2. **Incident response:**
   - See [RUNBOOK.md](RUNBOOK.md) for troubleshooting
   - Escalation path defined

---

## 3. Wave Strategy

The migration wave planner (`wave_plan.json`) groups objects by dependency depth:

| Wave | Contents | Criteria |
|------|----------|----------|
| 1 | No dependencies | Simple mappings, standalone SQL |
| 2 | Depends on Wave 1 | Mappings referencing Wave 1 targets |
| 3 | Depends on Wave 2 | Complex multi-source mappings |
| N | Critical path objects | Highest-dependency objects |

**Parallel execution:** Within each wave, objects can be migrated in parallel.

---

## 4. Risk Register

| Risk | Impact | Mitigation |
|------|--------|------------|
| Complex PL/SQL not convertible | High | Manual rewrite; allocate extra sprint |
| Source DB connectivity issues | Medium | Pre-test VNet/gateway in Week 2 |
| Performance regression | Medium | Benchmark during parallel run |
| PII exposure in migration | High | Apply masking; use PII scanner (Sprint 39) |
| Team unfamiliar with PySpark | Medium | Training in Week 1-2 |
| Databricks Unity Catalog misconfigured | Medium | Validate 3-level namespace before conversion |

---

## 5. Governance

### Data Classification

| Level | Examples | Handling |
|-------|----------|----------|
| Highly Confidential | SSN, Credit Card, Passport | SHA-256 hash or encrypt |
| Confidential | Email, Phone, DOB | MD5 hash or tokenize |
| Internal | Name, Address, IP | Audit trail required |
| Public | Product names, categories | No special handling |

### Compliance Checklist
- [ ] Data residency requirements met (Azure region)
- [ ] GDPR/CCPA opt-out columns preserved
- [ ] PII columns masked or encrypted
- [ ] Audit logging enabled (`output/audit_log.json`)
- [ ] Access control configured per workspace role
- [ ] Purview catalog integration (optional)

---

## 6. Tools Reference

| Tool | Purpose | Command |
|------|---------|---------|
| CLI | Full migration | `informatica-to-fabric run` |
| CLI (Databricks) | Full migration to Databricks | `informatica-to-fabric run --target databricks` |
| Web wizard | Guided migration | `streamlit run web/app.py` |
| Assessment | Parse & inventory | `python run_assessment.py` |
| Dashboard | Visual status | `python dashboard.py` |
| Fabric deployment | Push to Fabric | `python deploy_to_fabric.py` |
| Databricks deployment | Import to Databricks | `databricks workspace import_dir` |
| Tests | Validate code | `py -m pytest tests/` |
