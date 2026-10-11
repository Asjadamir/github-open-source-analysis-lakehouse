# Phase 2 Pipeline Execution Guide

## Overview

This document explains how to execute the GitHub Open Source Analytics Lakehouse pipeline in Databricks.

The Phase 2 pipeline processes data through the following flow:

```text
GitHub REST API
       |
       v
Raw / Staging Files
       |
       v
Bronze Delta Tables
       |
       v
Silver Delta Tables
```

Operational processing is also recorded in:

- `pipeline_execution_logs`
- `quarantine_records`

The pipeline supports:

- Full loads
- Incremental loads
- Historical backfills
- Idempotent reruns
- Schema validation
- Quarantine handling
- Execution auditing

---

# 1. Required Databricks Notebooks

The project contains the following notebooks:

```text
databricks/
├── 00_setup
├── 01_raw_to_bronze
├── 02_bronze_to_silver
├── 03_run_pipeline
└── 04_validation_tests
```

## 00_setup

Creates the required Delta tables and operational tables.

## 01_raw_to_bronze

Reads raw `.json.gz` source files using explicitly defined PySpark schemas and writes validated records into Bronze Delta tables.

## 02_bronze_to_silver

Reads a selected Bronze batch, performs cleaning, casting, PII handling, validation, and Delta MERGE operations into Silver tables.

## 03_run_pipeline

Main orchestration notebook. It executes Raw-to-Bronze followed by Bronze-to-Silver.

## 04_validation_tests

Performs validation checks on row uniqueness, metadata completeness, audit logs, and pipeline results.

---

# 2. Raw Data Directory Structure

Raw files are stored in immutable batch directories.

Example:

```text
/Volumes/<catalog>/github_analytics/raw_data/
│
├── full_load/
│   └── 2026-10-09_184448/
│       ├── repositories.json.gz
│       ├── commits.json.gz
│       ├── issues.json.gz
│       ├── pull_requests.json.gz
│       └── run_metadata.json
│
└── incremental_load/
    └── <incremental_batch_id>/
        ├── repositories.json.gz
        ├── commits.json.gz
        ├── issues.json.gz
        ├── pull_requests.json.gz
        └── run_metadata.json
```

Raw batch folders must not be overwritten or modified after ingestion.

Keeping historical raw batches allows the pipeline to support:

- Backfills
- Reprocessing
- Debugging
- Auditability
- Fault recovery

---

# 3. Pipeline Parameters

The main pipeline uses four parameters.

| Parameter | Description | Example |
|---|---|---|
| `input_path` | Path to the raw batch folder | `/Volumes/<catalog>/github_analytics/raw_data/incremental_load/2026-10-10_210500` |
| `batch_id` | Unique identifier for the batch | `2026-10-10_210500` |
| `source_load_type` | Indicates source extraction type | `full` or `incremental` |
| `execution_mode` | Indicates normal or historical execution | `standard` or `backfill` |

These parameters are provided using Databricks notebook widgets.

No processing date or batch folder is hardcoded in the pipeline logic.

---

# 4. Initial Setup

The setup notebook only needs to be executed when the environment or required tables need to be created.

Open:

`databricks/00_setup`

Run all cells.

The following tables should be created:

```text
bronze_repositories
bronze_commits
bronze_issues
bronze_pull_requests

silver_repositories
silver_commits
silver_issues
silver_pull_requests

pipeline_execution_logs
quarantine_records
```

Verify the tables using:

```sql
SHOW TABLES IN <catalog>.github_analytics;
```

---

# 5. Standard Full-Load Execution

A full load is normally used for the initial historical baseline.

Open:

`databricks/03_run_pipeline`

Provide the following parameters.

## input_path

Example:

```text
/Volumes/<catalog>/github_analytics/raw_data/full_load/2026-10-09_184448
```

## batch_id

```text
2026-10-09_184448
```

## source_load_type

```text
full
```

## execution_mode

```text
standard
```

Run the notebook.

The pipeline will execute:

```text
Raw Files
   |
   v
Raw-to-Bronze
   |
   v
Bronze-to-Silver
```

The full-load batch remains permanently stored in the raw staging area.

---

# 6. Standard Incremental Load

An incremental load processes a new source batch created after the previous extraction.

Open:

`databricks/03_run_pipeline`

Provide the incremental batch parameters.

Example:

## input_path

```text
/Volumes/<catalog>/github_analytics/raw_data/incremental_load/2026-10-10_210500
```

## batch_id

```text
2026-10-10_210500
```

## source_load_type

```text
incremental
```

## execution_mode

```text
standard
```

Run the notebook.

The Raw-to-Bronze pipeline loads only records belonging to the selected batch.

Bronze retains the new batch as historical data.

Silver then merges the new and changed records into the current-state Silver tables.

---

# 7. Historical Backfill Execution

A backfill reprocesses an existing historical raw batch.

No source-code changes are required to run a backfill.

Open:

`databricks/03_run_pipeline`

Provide the historical batch path and batch ID.

Example:

## input_path

```text
/Volumes/<catalog>/github_analytics/raw_data/full_load/2026-10-09_184448
```

## batch_id

```text
2026-10-09_184448
```

## source_load_type

```text
full
```

## execution_mode

```text
backfill
```

Run the notebook.

Because Bronze uses the combination of business key and `batch_id`, already-loaded Bronze records will not be duplicated.

Because Silver uses business-key MERGE operations with `record_hash` change detection, unchanged records will not be inserted or updated again.

Therefore, the same historical batch can safely be rerun.

---

# 8. Idempotency Test

To demonstrate idempotency, execute the exact same batch twice.

For example, execute:

```text
input_path =
/Volumes/<catalog>/github_analytics/raw_data/incremental_load/2026-10-10_210500

batch_id =
2026-10-10_210500

source_load_type =
incremental

execution_mode =
standard
```

Run the pipeline once.

Record the Silver table counts.

Example:

```sql
SELECT COUNT(*)
FROM <catalog>.github_analytics.silver_commits;
```

Run the exact same batch again without changing any input.

Run the count again.

The count must remain unchanged.

For an unchanged rerun, the pipeline logs should show approximately:

```text
Bronze:
rows_inserted = 0

Silver:
rows_inserted = 0
rows_updated = 0
```

This proves the pipeline is idempotent.

---

# 9. Bronze Idempotency Strategy

Bronze uses Delta `MERGE` with the following composite keys:

```text
bronze_repositories:
repo_id + batch_id

bronze_commits:
commit_sha + batch_id

bronze_issues:
issue_id + batch_id

bronze_pull_requests:
pr_id + batch_id
```

Bronze uses insert-only MERGE behavior.

If a matching business key and batch ID already exist, the record is not inserted again.

---

# 10. Silver Idempotency Strategy

Silver uses Delta `MERGE` based on the business key.

```text
silver_repositories:
repo_id

silver_commits:
commit_sha

silver_issues:
issue_id

silver_pull_requests:
pr_id
```

Each Silver source record receives a deterministic `record_hash`.

If:

```text
business key does not exist
```

the record is inserted.

If:

```text
business key exists
AND
record_hash changed
```

the existing record is updated.

If:

```text
business key exists
AND
record_hash is unchanged
```

no operation is performed.

---

# 11. Schema Enforcement

Raw JSON files are always read using explicit PySpark schemas constructed using:

```text
StructType
StructField
```

Schema inference is not used.

The pipeline does not use:

```text
inferSchema=True
```

This provides strict control over expected source contracts.

---

# 12. Schema Drift Handling

Unexpected or non-conforming source records must not terminate the entire batch.

Raw-to-Bronze processing uses permissive parsing with an explicitly defined corrupt-record field.

Records that violate required Bronze contracts can be written to:

```text
quarantine_records
```

The quarantine table contains:

```text
dataset
layer
batch_id
source_file
business_key
error_reason
record_json
load_timestamp
```

Silver uses safe casting such as `try_cast`.

For example, if a value expected to be numeric unexpectedly contains:

```text
NOT_A_NUMBER
```

the cast returns `NULL` instead of crashing the complete Spark job.

The record can then be identified as non-conforming and quarantined.

---

# 13. PII Handling

Commit API responses can contain contributor email addresses and usernames.

Raw identifying values may exist in controlled Bronze storage.

Before Silver:

- Author email addresses are hashed.
- Author GitHub usernames are hashed.
- Issue creator usernames are hashed.
- Pull-request creator usernames are hashed.
- Raw contributor email addresses are not propagated to Silver.

Hashing uses SHA-256.

---

# 14. Execution Logging

Every dataset processing operation writes a row to:

```text
pipeline_execution_logs
```

The log contains:

```text
run_id
pipeline_name
layer
dataset
batch_id
input_parameter
source_load_type
execution_mode
execution_start
execution_end
status
rows_read
rows_inserted
rows_updated
rows_quarantined
error_message
```

Both successful and failed executions are logged.

To inspect the latest pipeline runs:

```sql
SELECT *
FROM <catalog>.github_analytics.pipeline_execution_logs
ORDER BY execution_start DESC;
```

---

# 15. Failure Logging Test

To demonstrate failure auditing, execute the pipeline once using an invalid source path.

Example:

```text
input_path =
/Volumes/<catalog>/github_analytics/raw_data/does_not_exist
```

Allow the notebook to fail.

Then query:

```sql
SELECT *
FROM <catalog>.github_analytics.pipeline_execution_logs
WHERE status = 'FAILURE'
ORDER BY execution_start DESC;
```

The log should contain the failed execution and associated error message.

---

# 16. Quarantine Validation

To inspect quarantined records:

```sql
SELECT *
FROM <catalog>.github_analytics.quarantine_records
ORDER BY load_timestamp DESC;
```

The quarantine table provides evidence that non-conforming records can be isolated without corrupting the valid Bronze and Silver datasets.

---

# 17. Validate Bronze Record Counts

Use:

```sql
SELECT batch_id, COUNT(*) AS row_count
FROM <catalog>.github_analytics.bronze_repositories
GROUP BY batch_id
ORDER BY batch_id;
```

Similar queries can be executed for:

```text
bronze_commits
bronze_issues
bronze_pull_requests
```

This demonstrates that Bronze stores historical batches separately.

---

# 18. Validate Silver Uniqueness

The following logical keys must remain unique:

```text
silver_repositories.repo_id
silver_commits.commit_sha
silver_issues.issue_id
silver_pull_requests.pr_id
```

Example:

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT repo_id) AS unique_rows
FROM <catalog>.github_analytics.silver_repositories;
```

`total_rows` and `unique_rows` should be equal.

---

# 19. Validate load_timestamp

Every Bronze and Silver record must contain a processing timestamp.

Example:

```sql
SELECT COUNT(*) AS missing_load_timestamp
FROM <catalog>.github_analytics.silver_repositories
WHERE load_timestamp IS NULL;
```

Expected result:

```text
0
```

The same validation should be performed for all Bronze and Silver tables.

---

# 20. Recommended Execution Order

For the initial full-load setup:

```text
1. Run 00_setup
2. Run 03_run_pipeline with the full-load batch
3. Run 04_validation_tests
```

For a normal incremental batch:

```text
1. Upload the new immutable incremental raw folder
2. Open 03_run_pipeline
3. Set the new input_path
4. Set the new batch_id
5. Set source_load_type = incremental
6. Set execution_mode = standard
7. Run the pipeline
8. Run 04_validation_tests
```

For a historical backfill:

```text
1. Open 03_run_pipeline
2. Select the historical raw input_path
3. Enter the historical batch_id
4. Set the original source_load_type
5. Set execution_mode = backfill
6. Run the pipeline
7. Check pipeline_execution_logs
8. Confirm that duplicate Silver records were not created
```

---

# 21. Important Operational Rules

1. Raw batch folders are immutable and must not be overwritten.
2. Every batch must have a unique `batch_id`.
3. Historical source files must be retained for replay and debugging.
4. Raw files must always be read using explicit schemas.
5. Bronze writes must be idempotent.
6. Silver writes must use Delta MERGE.
7. Every Bronze and Silver record must contain `load_timestamp`.
8. Every processing operation must create an audit-log entry.
9. Invalid records must be quarantined rather than silently discarded.
10. Backfills must be performed using parameters and must not require source-code changes.