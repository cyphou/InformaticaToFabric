# ADR-006: AutoSys JIL Migration Support

## Status
Accepted

## Context
Many Informatica deployments are orchestrated by CA AutoSys (now Broadcom), not by Informatica Workflow Manager. AutoSys JIL (Job Information Language) files define BOX, CMD, and FW jobs that call `pmcmd` to start Informatica workflows. Without migrating the AutoSys layer, the orchestration chain is broken.

## Decision
Add AutoSys JIL parsing as Phase 5 (Sprints 61–65). Parse `.jil` files, build dependency DAGs, and convert to Fabric Pipeline JSON or Databricks Workflow JSON.

## AutoSys → Fabric Mapping

| AutoSys Concept | Fabric Equivalent |
|-----------------|-------------------|
| BOX job | ExecutePipeline (parent pipeline) |
| CMD job (`pmcmd`) | Notebook Activity (linked to migrated workflow) |
| CMD job (shell) | Script Activity or Notebook |
| FW job (file watcher) | GetMetadata Activity + Until loop |
| FT job (file transfer) | Copy Activity |
| `condition: s(job_a)` | `dependsOn: [Succeeded]` |
| `condition: f(job_a)` | `dependsOn: [Failed]` |
| `date_conditions: 1` + `days_of_week` | ScheduleTrigger with cron |
| `std_out_file` / `std_err_file` | Pipeline run logs (automatic) |
| `alarm_if_fail: 1` | Web Activity → Teams/Slack webhook |
| `profile` / `machine` | Spark pool / cluster config |

## Rationale
- AutoSys is the most common external scheduler for Informatica in enterprise
- `pmcmd startworkflow` calls are directly linkable to migrated Informatica workflows
- BOX→Pipeline nesting preserves the original orchestration hierarchy
- Calendar/date conditions map naturally to Fabric schedule triggers

## Consequences
- JIL parser handles `insert_job`, `box_name`, `command`, `condition`, `date_conditions`
- `pmcmd` commands are parsed to extract workflow/folder/service references
- Cross-reference with `inventory.json` links AutoSys jobs to migrated pipelines
- Alarm/notification settings generate Web Activity alerts
- Machine/profile mappings produce cluster configuration recommendations
