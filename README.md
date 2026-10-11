# GitHub Open Source Analysis Lakehouse

## Project Overview

This project implements an end-to-end data engineering pipeline for analyzing activity across 50 public open-source GitHub repositories.

The pipeline uses the GitHub REST API for data ingestion and Apache Spark on Databricks Free Edition for distributed data processing.

The project follows the Medallion Architecture:

```text
GitHub REST API
        |
        v
Raw / Staging Layer
Immutable batch-based JSON files
        |
        v
Bronze Layer
Historical Delta tables
        |
        v
Silver Layer
Cleaned and deduplicated Delta tables
        |
        v
Gold Layer
Business-ready analytics
        |
        v
Power BI Dashboard
```

Phase 2 focuses on implementing the Raw, Bronze, and Silver layers while enforcing explicit schemas, idempotency, parameterized backfills, schema-drift handling, and execution logging.

---

# Technology Stack

- GitHub REST API
- Python
- PySpark
- Apache Spark
- Databricks Free Edition
- Delta Lake
- Unity Catalog
- Git
- GitHub
- Power BI

---

# Data Source

**Data Source:** GitHub REST API

**Base URL:**

```text
https://api.github.com
```

**Tracked Repositories:** 50 public open-source repositories.

The project collects:

- Repository metadata
- Commit history
- Issues
- Pull requests

---

# Tracked Repositories

## Big Data and Data Engineering

1. `apache/spark`
2. `apache/kafka`
3. `apache/flink`
4. `apache/airflow`
5. `apache/beam`
6. `apache/arrow`
7. `apache/iceberg`
8. `apache/hadoop`
9. `apache/superset`

## AI, Machine Learning, and Data Science

10. `tensorflow/tensorflow`
11. `pytorch/pytorch`
12. `huggingface/transformers`
13. `scikit-learn/scikit-learn`
14. `numpy/numpy`
15. `pandas-dev/pandas`

## Programming Languages and Developer Tools

16. `microsoft/vscode`
17. `nodejs/node`
18. `rust-lang/rust`
19. `golang/go`
20. `microsoft/TypeScript`
21. `llvm/llvm-project`

## Frontend Development

22. `facebook/react`
23. `angular/angular`
24. `vuejs/core`
25. `sveltejs/svelte`
26. `vercel/next.js`

## Backend Development

27. `django/django`
28. `spring-projects/spring-boot`
29. `expressjs/express`
30. `nestjs/nest`
31. `fastapi/fastapi`
32. `pallets/flask`
33. `laravel/laravel`
34. `dotnet/runtime`

## Cloud, Infrastructure, and DevOps

35. `kubernetes/kubernetes`
36. `hashicorp/terraform`
37. `ansible/ansible`
38. `moby/moby`
39. `helm/helm`
40. `istio/istio`
41. `envoyproxy/envoy`

## Databases and Monitoring

42. `elastic/elasticsearch`
43. `grafana/grafana`
44. `prometheus/prometheus`
45. `redis/redis`
46. `mongodb/mongo`
47. `ClickHouse/ClickHouse`
48. `postgres/postgres`

## Other Developer Platforms

49. `denoland/deno`
50. `oven-sh/bun`

---

# GitHub REST API Endpoints

## 1. Repository Metadata

**Endpoint:**

```text
GET /repos/{owner}/{repo}
```

**Example:**

```text
https://api.github.com/repos/apache/spark
```

**Purpose:**

Collect repository information including:

- Repository ID
- Repository name
- Owner
- Description
- Programming language
- Stars
- Forks
- Open issues
- Creation timestamp
- Update timestamp
- Push timestamp
- Default branch
- Repository status

---

# 2. Commit History

**Endpoint:**

```text
GET /repos/{owner}/{repo}/commits
```

**Full-load parameters:**

```text
per_page=100
```

**Incremental parameters:**

```text
since={WATERMARK_UTC}
```

**Purpose:**

Analyze:

- Development activity
- Commit frequency
- Contributor activity
- Commit timestamps
- Commit messages

---

# 3. Issues

**Endpoint:**

```text
GET /repos/{owner}/{repo}/issues
```

**Full-load parameters:**

```text
state=all&sort=updated&direction=desc&per_page=100
```

**Incremental parameters:**

```text
since={WATERMARK_UTC}
```

**Purpose:**

Analyze:

- Issue creation
- Issue status
- Issue updates
- Issue resolution
- Repository maintenance activity

The GitHub Issues endpoint may also return pull-request records.

Records containing the `pull_request` field are identified during processing so that pull requests are not duplicated in the final Silver issue table.

---

# 4. Pull Requests

**Endpoint:**

```text
GET /repos/{owner}/{repo}/pulls
```

**Full-load parameters:**

```text
state=all&sort=updated&direction=desc&per_page=100
```

**Incremental strategy:**

The pull-request endpoint does not provide the same `since` parameter used by commits and issues.

The pipeline retrieves pull requests ordered by `updated_at` and compares them against the previous ingestion watermark.

**Purpose:**

Analyze:

- Pull-request activity
- Collaboration
- Merge activity
- Contribution trends
- Repository development workflow

---

# Ingestion Strategy

## Full Load

The initial full load retrieves:

- Current metadata for all 50 repositories
- Up to 100 recent commits per repository
- Up to 100 recent issue-endpoint records per repository
- Up to 100 recent pull requests per repository

The full-load files establish the historical baseline for the Lakehouse pipeline.

---

## Incremental Load

Incremental loads retrieve records created or updated after the previous successful ingestion watermark.

The incremental process collects:

- New commits
- New or updated issues
- New or updated pull requests
- A new repository metadata snapshot

A watermark is maintained in:

```text
state/last_watermark.json
```

This allows subsequent API calls to retrieve only records that have changed since the previous successful run.

---

# Raw / Staging Layer

Raw data is stored as immutable, batch-based files.

Each extraction receives a unique `batch_id`.

Example:

```text
data/
├── full_load/
│   └── 2026-10-09_184448/
│       ├── repositories.json.gz
│       ├── commits.json.gz
│       ├── issues.json.gz
│       ├── pull_requests.json.gz
│       └── run_metadata.json
│
└── incremental_load/
    └── <batch_id>/
        ├── repositories.json.gz
        ├── commits.json.gz
        ├── issues.json.gz
        ├── pull_requests.json.gz
        └── run_metadata.json
```

Raw batches are never overwritten.

Keeping historical raw batches allows:

- Pipeline replay
- Backfills
- Debugging
- Recovery from processing failures
- Auditing
- Schema-drift investigation

The raw JSON files are gzip-compressed to reduce storage usage and remain below GitHub file-size limits.

---

# Databricks Raw Storage

Raw files are uploaded to a Unity Catalog Volume in Databricks Free Edition.

Example structure:

```text
/Volumes/<catalog>/github_analytics/raw_data/
├── full_load/
│   └── <batch_id>/
│
└── incremental_load/
    └── <batch_id>/
```

The Databricks notebooks receive the raw folder path as a parameter instead of using hardcoded dates.

---

# Medallion Architecture

## Bronze Layer

The Bronze layer stores structured historical representations of the raw GitHub API records.

Bronze preserves batch history while adding operational metadata.

Bronze tables:

```text
bronze_repositories
bronze_commits
bronze_issues
bronze_pull_requests
```

Every Bronze record contains:

```text
batch_id
source_load_type
execution_mode
source_file
schema_version
load_timestamp
```

The Bronze layer preserves records from different ingestion batches.

---

# Bronze Data Model

## bronze_repositories

**Primary Key:**

```text
repo_id + batch_id
```

| Column | Type | Description |
|---|---|---|
| repo_id | STRING | GitHub repository ID |
| source_repository | STRING | Repository in owner/repository format |
| name | STRING | Repository name |
| full_name | STRING | Full GitHub repository name |
| owner_login | STRING | Repository owner's GitHub login |
| description | STRING | Repository description |
| language | STRING | Primary programming language |
| stargazers_count | STRING | Raw star count |
| forks_count | STRING | Raw fork count |
| open_issues_count | STRING | Raw open issue count |
| default_branch | STRING | Default branch |
| archived | BOOLEAN | Archive status |
| disabled | BOOLEAN | Disabled status |
| fork | BOOLEAN | Indicates whether repository is a fork |
| created_at_raw | STRING | Raw creation timestamp |
| updated_at_raw | STRING | Raw update timestamp |
| pushed_at_raw | STRING | Raw push timestamp |
| fetched_at_utc_raw | STRING | API extraction timestamp |
| batch_id | STRING | Ingestion batch identifier |
| source_load_type | STRING | Full or incremental |
| execution_mode | STRING | Standard or backfill |
| source_file | STRING | Source file path |
| schema_version | STRING | Data contract version |
| load_timestamp | TIMESTAMP | Bronze processing timestamp |

---

## bronze_commits

**Primary Key:**

```text
commit_sha + batch_id
```

| Column | Type | Description |
|---|---|---|
| commit_sha | STRING | Git commit SHA |
| source_repository | STRING | Source repository |
| author_login | STRING | GitHub author login |
| author_name | STRING | Commit author name |
| author_email | STRING | Commit author email |
| author_date_raw | STRING | Raw author timestamp |
| committer_login | STRING | GitHub committer login |
| committer_name | STRING | Committer name |
| committer_email | STRING | Committer email |
| committer_date_raw | STRING | Raw committer timestamp |
| message | STRING | Commit message |
| fetched_at_utc_raw | STRING | API extraction timestamp |
| batch_id | STRING | Batch identifier |
| source_load_type | STRING | Full or incremental |
| execution_mode | STRING | Standard or backfill |
| source_file | STRING | Source file |
| schema_version | STRING | Data contract version |
| load_timestamp | TIMESTAMP | Bronze ingestion timestamp |

---

## bronze_issues

**Primary Key:**

```text
issue_id + batch_id
```

| Column | Type | Description |
|---|---|---|
| issue_id | STRING | GitHub issue ID |
| issue_number | STRING | Repository issue number |
| source_repository | STRING | Source repository |
| state | STRING | Open or closed |
| state_reason | STRING | Reason for current state |
| title | STRING | Issue title |
| user_login | STRING | Issue creator |
| comments_raw | STRING | Raw comment count |
| created_at_raw | STRING | Creation timestamp |
| updated_at_raw | STRING | Update timestamp |
| closed_at_raw | STRING | Closure timestamp |
| is_pull_request | BOOLEAN | Identifies PR records returned by Issues API |
| fetched_at_utc_raw | STRING | API extraction timestamp |
| batch_id | STRING | Batch identifier |
| source_load_type | STRING | Full or incremental |
| execution_mode | STRING | Standard or backfill |
| source_file | STRING | Source file |
| schema_version | STRING | Data contract version |
| load_timestamp | TIMESTAMP | Bronze ingestion timestamp |

---

## bronze_pull_requests

**Primary Key:**

```text
pr_id + batch_id
```

| Column | Type | Description |
|---|---|---|
| pr_id | STRING | GitHub pull-request ID |
| pr_number | STRING | Repository pull-request number |
| source_repository | STRING | Source repository |
| state | STRING | Pull-request state |
| title | STRING | Pull-request title |
| user_login | STRING | Creator login |
| draft | BOOLEAN | Draft status |
| created_at_raw | STRING | Creation timestamp |
| updated_at_raw | STRING | Update timestamp |
| closed_at_raw | STRING | Closure timestamp |
| merged_at_raw | STRING | Merge timestamp |
| merge_commit_sha | STRING | Merge commit identifier |
| fetched_at_utc_raw | STRING | API extraction timestamp |
| batch_id | STRING | Batch identifier |
| source_load_type | STRING | Full or incremental |
| execution_mode | STRING | Standard or backfill |
| source_file | STRING | Source file |
| schema_version | STRING | Data contract version |
| load_timestamp | TIMESTAMP | Bronze ingestion timestamp |

---

# Strict Schema Enforcement

Spark schema inference is not used anywhere in the Raw-to-Bronze pipeline.

Schemas are explicitly defined using:

```python
StructType
StructField
StringType
BooleanType
```

Raw JSON files are read using:

```python
spark.read.schema(explicit_schema)
```

The pipeline does not use:

```python
inferSchema=True
```

This establishes an explicit data contract between the GitHub API source and the Bronze layer.

---

# Silver Layer

The Silver layer contains cleaned, validated, deduplicated, strongly typed records.

Silver transformations include:

- Converting IDs to numeric data types
- Converting timestamps from strings to `TIMESTAMP`
- Converting counts to `BIGINT`
- Removing invalid records
- Separating issue records from pull-request records
- Hashing sensitive contributor identifiers
- Deduplicating records
- Upserting changed records with Delta Lake `MERGE`
- Adding `load_timestamp`

Silver tables:

```text
silver_repositories
silver_commits
silver_issues
silver_pull_requests
```

---

# Silver Data Model

## silver_repositories

**Primary Key:**

```text
repo_id
```

| Column | Type | Description |
|---|---|---|
| repo_id | BIGINT | GitHub repository ID |
| source_repository | STRING | owner/repository identifier |
| name | STRING | Repository name |
| full_name | STRING | Full repository name |
| owner_login | STRING | Repository owner |
| description | STRING | Repository description |
| language | STRING | Primary programming language |
| stars | BIGINT | Star count |
| forks | BIGINT | Fork count |
| open_issues | BIGINT | Open issue count |
| default_branch | STRING | Default branch |
| archived | BOOLEAN | Archive status |
| disabled | BOOLEAN | Disabled status |
| fork | BOOLEAN | Fork status |
| created_at | TIMESTAMP | Repository creation time |
| updated_at | TIMESTAMP | Repository update time |
| pushed_at | TIMESTAMP | Latest push timestamp |
| record_hash | STRING | Hash used to detect changed records |
| source_batch_id | STRING | Batch producing latest state |
| load_timestamp | TIMESTAMP | Silver processing timestamp |

---

## silver_commits

**Primary Key:**

```text
commit_sha
```

| Column | Type | Description |
|---|---|---|
| commit_sha | STRING | Unique Git commit SHA |
| repo_id | BIGINT | Repository ID |
| source_repository | STRING | Source repository |
| author_login_hash | STRING | SHA-256 hashed contributor login |
| author_email_hash | STRING | SHA-256 hashed email |
| author_date | TIMESTAMP | Author timestamp |
| committer_date | TIMESTAMP | Commit timestamp |
| message | STRING | Commit message |
| record_hash | STRING | Change-detection hash |
| source_batch_id | STRING | Source ingestion batch |
| load_timestamp | TIMESTAMP | Silver processing time |

---

## silver_issues

**Primary Key:**

```text
issue_id
```

| Column | Type | Description |
|---|---|---|
| issue_id | BIGINT | GitHub issue ID |
| repo_id | BIGINT | Repository ID |
| source_repository | STRING | Repository |
| issue_number | BIGINT | Issue number |
| state | STRING | Current state |
| state_reason | STRING | Reason for issue state |
| title | STRING | Issue title |
| user_login_hash | STRING | Hashed creator identifier |
| comments | BIGINT | Number of comments |
| created_at | TIMESTAMP | Issue creation time |
| updated_at | TIMESTAMP | Latest update time |
| closed_at | TIMESTAMP | Closure time |
| record_hash | STRING | Change-detection hash |
| source_batch_id | STRING | Source ingestion batch |
| load_timestamp | TIMESTAMP | Silver processing time |

---

## silver_pull_requests

**Primary Key:**

```text
pr_id
```

| Column | Type | Description |
|---|---|---|
| pr_id | BIGINT | GitHub pull-request ID |
| repo_id | BIGINT | Repository ID |
| source_repository | STRING | Repository |
| pr_number | BIGINT | Pull-request number |
| state | STRING | Current state |
| title | STRING | Pull-request title |
| user_login_hash | STRING | Hashed creator identifier |
| draft | BOOLEAN | Draft indicator |
| created_at | TIMESTAMP | Creation timestamp |
| updated_at | TIMESTAMP | Update timestamp |
| closed_at | TIMESTAMP | Closure timestamp |
| merged_at | TIMESTAMP | Merge timestamp |
| merge_commit_sha | STRING | Merge commit SHA |
| record_hash | STRING | Change-detection hash |
| source_batch_id | STRING | Source batch |
| load_timestamp | TIMESTAMP | Silver processing timestamp |

---

# PII and Sensitive Data Handling

GitHub API responses may expose information such as:

- Usernames
- Contributor names
- Commit author emails
- Committer emails

Raw data is retained in the controlled Raw and Bronze layers for traceability.

Before data enters the Silver layer:

- Contributor emails are removed.
- Contributor emails are converted to SHA-256 hashes where identification is needed.
- Contributor usernames used for analytics are hashed.
- Unnecessary personal fields are not propagated into Silver.

This reduces exposure of personally identifiable information while still allowing contributor-level analytics.

---

# Idempotency Strategy

The pipeline is designed to be fully idempotent.

Running the exact same batch multiple times does not create duplicate records.

## Bronze Idempotency

Bronze tables use Delta Lake `MERGE`.

Composite identifiers are:

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

Bronze only inserts records when the same business key and batch ID do not already exist.

Therefore, rerunning the same raw batch results in:

```text
rows_inserted = 0
```

instead of creating duplicates.

---

## Silver Idempotency

Silver tables use the business primary key together with a deterministic `record_hash`.

The hash is generated from the business attributes of each record.

Example:

```text
repo_id = same
record_hash = same
```

Result:

```text
No INSERT
No UPDATE
```

If the repository exists but its attributes changed:

```text
repo_id = same
record_hash = different
```

Result:

```text
UPDATE
```

If the business key does not exist:

```text
INSERT
```

This supports true incremental upsert processing.

---

# Parameterized Pipeline

Pipeline execution does not depend on today's date or hardcoded file paths.

The Raw-to-Bronze notebook accepts:

```text
input_path
batch_id
source_load_type
execution_mode
```

The Bronze-to-Silver notebook accepts:

```text
batch_id
execution_mode
```

This allows the same pipeline code to process:

- Full loads
- Incremental loads
- Historical backfills
- Reprocessing attempts

---

# Standard Incremental Execution

Example parameters:

```text
input_path =
/Volumes/<catalog>/github_analytics/raw_data/incremental_load/<batch_id>

batch_id =
<batch_id>

source_load_type =
incremental

execution_mode =
standard
```

The pipeline performs:

```text
Raw Incremental Batch
        |
        v
Bronze MERGE
        |
        v
Silver Transformation
        |
        v
Silver MERGE
```

---

# Historical Backfill

The same pipeline can reprocess any historical batch.

Example:

```text
input_path =
/Volumes/<catalog>/github_analytics/raw_data/full_load/2026-10-09_184448

batch_id =
2026-10-09_184448

source_load_type =
full

execution_mode =
backfill
```

No source-code modification is required.

Because the pipeline uses Delta `MERGE`, replaying an already processed batch does not introduce duplicate records.

---

# Schema Drift Handling

The pipeline uses an explicit data contract for Raw-to-Bronze processing.

Unexpected or invalid records are separated from valid records instead of crashing the entire pipeline.

A dedicated table is used:

```text
quarantine_records
```

The quarantine table stores:

| Column | Description |
|---|---|
| dataset | Source dataset |
| layer | Pipeline layer |
| batch_id | Batch identifier |
| source_file | Original source file |
| business_key | Record identifier when available |
| error_reason | Validation failure |
| record_json | Problematic record |
| load_timestamp | Quarantine processing timestamp |

Examples of records that may be quarantined include:

- Missing primary keys
- Malformed JSON
- Unexpected type changes
- Values that fail strict casting
- Non-conforming records

Raw files remain unchanged, allowing affected batches to be corrected and reprocessed later.

---

# Audit and Execution Logging

Every dataset processed by the pipeline writes an execution record to:

```text
pipeline_execution_logs
```

The logging table contains:

| Column | Description |
|---|---|
| run_id | Unique pipeline execution identifier |
| pipeline_name | Pipeline name |
| layer | Raw-to-Bronze or Bronze-to-Silver |
| dataset | repositories, commits, issues, or pull_requests |
| batch_id | Batch being processed |
| input_parameter | Source input |
| source_load_type | Full or incremental |
| execution_mode | Standard or backfill |
| execution_start | Start timestamp |
| execution_end | End timestamp |
| status | SUCCESS or FAILURE |
| rows_read | Number of input records |
| rows_inserted | Number of inserted records |
| rows_updated | Number of updated records |
| rows_quarantined | Number of rejected records |
| error_message | Failure reason when applicable |

Logging is performed separately for each dataset and each processing layer.

Example:

```text
Raw-to-Bronze | repositories  | SUCCESS
Raw-to-Bronze | commits       | SUCCESS
Raw-to-Bronze | issues        | SUCCESS
Raw-to-Bronze | pull_requests | SUCCESS

Bronze-to-Silver | repositories  | SUCCESS
Bronze-to-Silver | commits       | SUCCESS
Bronze-to-Silver | issues        | SUCCESS
Bronze-to-Silver | pull_requests | SUCCESS
```

Failures are also written to the logging table before the exception is propagated.

---

# Databricks Notebooks

Phase 2 uses the following Databricks notebooks:

```text
databricks/
├── 00_setup
├── 01_raw_to_bronze
├── 02_bronze_to_silver
├── 03_run_pipeline
└── 04_validation_tests
```

## 00_setup

Responsible for:

- Project schema configuration
- Bronze Delta table creation
- Silver Delta table creation
- Logging-table creation
- Quarantine-table creation

## 01_raw_to_bronze

Responsible for:

- Reading raw `.json.gz` files
- Applying explicit `StructType` schemas
- Flattening nested GitHub API payloads
- Validating primary keys
- Quarantining invalid records
- Adding ingestion metadata
- Performing idempotent Bronze MERGE operations
- Writing execution logs

## 02_bronze_to_silver

Responsible for:

- Reading specific Bronze batches
- Strict type casting
- Timestamp conversion
- PII hashing
- Removing pull requests from the issue dataset
- Joining records to repositories
- Calculating record hashes
- Performing Silver Delta MERGE operations
- Logging inserts and updates

## 03_run_pipeline

Main orchestration notebook.

Executes:

```text
Raw-to-Bronze
      |
      v
Bronze-to-Silver
```

using parameterized notebook execution.

## 04_validation_tests

Used to verify:

- Silver primary-key uniqueness
- No duplicate records
- Non-null `load_timestamp`
- Execution logs
- Quarantined records
- Idempotent reruns

---

# Project Structure

```text
github-open-source-analysis-lakehouse/
│
├── README.md
├── requirements.txt
├── .gitignore
│
├── scripts/
│   └── collect_github_data.py
│
├── data/
│   ├── full_load/
│   └── incremental_load/
│
├── state/
│   └── last_watermark.json
│
├── databricks/
│   ├── 00_setup
│   ├── 01_raw_to_bronze
│   ├── 02_bronze_to_silver
│   ├── 03_run_pipeline
│   └── 04_validation_tests
│
├── docs/
│   ├── bronze_data_dictionary.md
│   ├── silver_data_dictionary.md
│   └── execution_guide.md
│
└── dashboard/
```

---

# Data Quality Validation

The pipeline validates that:

- Silver primary keys are unique.
- `load_timestamp` is populated in every Bronze and Silver record.
- Invalid business keys do not reach Silver.
- Pull-request objects are excluded from `silver_issues`.
- Sensitive contributor identifiers are hashed before entering Silver.
- The same batch can be rerun without creating duplicates.

Example idempotency test:

```text
First execution:
rows_inserted = 500

Second execution of same batch:
rows_inserted = 0
rows_updated = 0
```

---

# Databricks Free Edition and FinOps

The project is designed to remain practical within Databricks Free Edition limitations.

Cost and resource-control measures include:

- Limiting the source scope to 50 repositories
- Limiting the initial baseline to 100 recent records per activity endpoint
- Using incremental ingestion instead of repeated full downloads
- Storing raw files in compressed `.json.gz` format
- Testing individual datasets before executing the complete pipeline
- Using Delta `MERGE` instead of rewriting entire Silver tables
- Processing specific batches through parameters
- Avoiding unnecessary full-history API extraction
- Retaining reusable raw batches for backfills instead of repeatedly downloading them

---

# Current Phase

## Phase 1

Completed:

- GitHub REST API source selected
- 50 repositories selected
- Exact API endpoints documented
- Full-load sample extracted
- Incremental-load strategy implemented
- Batch watermarking implemented
- Sample data stored in GitHub

## Phase 2

Implementation includes:

- Databricks Free Edition environment
- Raw batch storage using Unity Catalog Volumes
- Explicit PySpark schemas
- Bronze Delta tables
- Silver Delta tables
- Strict data-type casting
- `load_timestamp` metadata
- Delta MERGE idempotency
- Parameterized incremental loads
- Historical backfills
- Schema-drift quarantine
- Execution logging
- Insert/update audit metrics
- Data-quality validation