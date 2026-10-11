# Bronze Layer Data Dictionary

## Overview

The Bronze layer stores source-oriented data ingested from the GitHub REST API. Data is loaded from immutable raw batch folders and written to Delta tables using explicit PySpark schemas.

The Bronze layer preserves historical batches and supports replay, auditing, incremental processing, and backfills.

Every Bronze record contains ingestion metadata including:

- `batch_id`
- `source_load_type`
- `execution_mode`
- `source_file`
- `schema_version`
- `load_timestamp`

The Bronze layer is idempotent. Records are merged using the business key together with `batch_id`, preventing duplicate records when the same raw batch is processed multiple times.

> Note: Primary keys listed in this document are logical keys used by the pipeline. Delta Lake does not physically enforce primary-key constraints.

---

# 1. bronze_repositories

## Purpose

Stores repository metadata collected from the GitHub Repository API.

Source endpoint:

`GET /repos/{owner}/{repo}`

## Logical Primary Key

`repo_id + batch_id`

| Column | Data Type | Nullable | Description |
|---|---|---|---|
| repo_id | STRING | No | Unique GitHub identifier for the repository. Stored as a string in Bronze before strict casting in Silver. |
| source_repository | STRING | No | Repository identifier in `owner/repository` format. |
| name | STRING | Yes | Repository name. |
| full_name | STRING | Yes | Complete GitHub repository name in `owner/repository` format. |
| owner_login | STRING | Yes | GitHub login of the repository owner or organization. |
| description | STRING | Yes | Repository description returned by GitHub. |
| language | STRING | Yes | Primary programming language reported by GitHub. |
| stargazers_count | STRING | Yes | Number of repository stars. Stored as STRING in Bronze and converted to BIGINT in Silver. |
| forks_count | STRING | Yes | Number of repository forks. Stored as STRING in Bronze and converted to BIGINT in Silver. |
| open_issues_count | STRING | Yes | Number of open issues reported by GitHub. Stored as STRING in Bronze and converted to BIGINT in Silver. |
| default_branch | STRING | Yes | Default branch of the repository. |
| archived | BOOLEAN | Yes | Indicates whether the repository has been archived. |
| disabled | BOOLEAN | Yes | Indicates whether the repository has been disabled. |
| fork | BOOLEAN | Yes | Indicates whether the repository is a fork of another repository. |
| created_at_raw | STRING | Yes | Repository creation timestamp as received from the source. |
| updated_at_raw | STRING | Yes | Repository update timestamp as received from the source. |
| pushed_at_raw | STRING | Yes | Timestamp of the latest push as received from the source. |
| fetched_at_utc_raw | STRING | No | UTC timestamp when the API record was downloaded. |
| batch_id | STRING | No | Unique identifier for the ingestion batch. Usually based on the extraction timestamp. |
| source_load_type | STRING | No | Indicates whether the source batch was `full` or `incremental`. |
| execution_mode | STRING | No | Indicates whether the pipeline execution was `standard` or `backfill`. |
| source_file | STRING | No | Path of the raw file from which the record was loaded. |
| schema_version | STRING | No | Version of the Bronze schema used to process the record. |
| load_timestamp | TIMESTAMP | No | Timestamp when the record was successfully written to the Bronze layer. |

---

# 2. bronze_commits

## Purpose

Stores commit-level activity collected from tracked GitHub repositories.

Source endpoint:

`GET /repos/{owner}/{repo}/commits`

## Logical Primary Key

`commit_sha + batch_id`

| Column | Data Type | Nullable | Description |
|---|---|---|---|
| commit_sha | STRING | No | Unique SHA identifier of the Git commit. |
| source_repository | STRING | No | Repository in `owner/repository` format. |
| author_login | STRING | Yes | GitHub username associated with the commit author. |
| author_name | STRING | Yes | Author name contained in the Git commit metadata. |
| author_email | STRING | Yes | Author email contained in the Git commit metadata. This field is retained only in Bronze and is hashed before Silver. |
| author_date_raw | STRING | Yes | Commit author timestamp as received from GitHub. |
| committer_login | STRING | Yes | GitHub username associated with the committer. |
| committer_name | STRING | Yes | Committer name contained in the Git commit metadata. |
| committer_email | STRING | Yes | Committer email contained in Git commit metadata. This field is not propagated to Silver. |
| committer_date_raw | STRING | Yes | Committer timestamp as received from GitHub. |
| message | STRING | Yes | Git commit message. |
| fetched_at_utc_raw | STRING | No | UTC timestamp when the API record was downloaded. |
| batch_id | STRING | No | Unique identifier for the ingestion batch. |
| source_load_type | STRING | No | Indicates `full` or `incremental` source extraction. |
| execution_mode | STRING | No | Indicates `standard` or `backfill` execution. |
| source_file | STRING | No | Path of the raw source file. |
| schema_version | STRING | No | Version of the Bronze schema. |
| load_timestamp | TIMESTAMP | No | Timestamp when the record was written to Bronze. |

---

# 3. bronze_issues

## Purpose

Stores records returned by the GitHub Issues API.

The GitHub Issues API can also return pull-request objects. The `is_pull_request` field is therefore retained so actual issues can be separated from pull requests in the Silver layer.

Source endpoint:

`GET /repos/{owner}/{repo}/issues`

## Logical Primary Key

`issue_id + batch_id`

| Column | Data Type | Nullable | Description |
|---|---|---|---|
| issue_id | STRING | No | Unique GitHub identifier for the issue record. |
| issue_number | STRING | Yes | Repository-specific issue number. Converted to BIGINT in Silver. |
| source_repository | STRING | No | Repository in `owner/repository` format. |
| state | STRING | Yes | Current issue state, such as `open` or `closed`. |
| state_reason | STRING | Yes | GitHub-provided reason associated with the issue state. |
| title | STRING | Yes | Issue title. |
| user_login | STRING | Yes | GitHub username of the issue creator. Hashed before Silver. |
| comments_raw | STRING | Yes | Number of comments as received from the source. Converted to BIGINT in Silver. |
| created_at_raw | STRING | Yes | Issue creation timestamp from the API. |
| updated_at_raw | STRING | Yes | Issue update timestamp from the API. |
| closed_at_raw | STRING | Yes | Issue closure timestamp from the API. |
| is_pull_request | BOOLEAN | No | `true` if the record returned by the Issues API actually represents a pull request. |
| fetched_at_utc_raw | STRING | No | UTC timestamp when the API record was downloaded. |
| batch_id | STRING | No | Unique identifier for the ingestion batch. |
| source_load_type | STRING | No | Indicates `full` or `incremental` source extraction. |
| execution_mode | STRING | No | Indicates `standard` or `backfill` execution. |
| source_file | STRING | No | Path of the raw source file. |
| schema_version | STRING | No | Version of the Bronze schema. |
| load_timestamp | TIMESTAMP | No | Timestamp when the record was written to Bronze. |

---

# 4. bronze_pull_requests

## Purpose

Stores pull-request records collected from tracked GitHub repositories.

Source endpoint:

`GET /repos/{owner}/{repo}/pulls`

## Logical Primary Key

`pr_id + batch_id`

| Column | Data Type | Nullable | Description |
|---|---|---|---|
| pr_id | STRING | No | Unique GitHub identifier for the pull request. |
| pr_number | STRING | Yes | Repository-specific pull-request number. Converted to BIGINT in Silver. |
| source_repository | STRING | No | Repository in `owner/repository` format. |
| state | STRING | Yes | Current pull-request state. |
| title | STRING | Yes | Pull-request title. |
| user_login | STRING | Yes | GitHub username of the pull-request creator. Hashed before Silver. |
| draft | BOOLEAN | Yes | Indicates whether the pull request is currently a draft. |
| created_at_raw | STRING | Yes | Pull-request creation timestamp as received from GitHub. |
| updated_at_raw | STRING | Yes | Pull-request update timestamp as received from GitHub. |
| closed_at_raw | STRING | Yes | Pull-request closure timestamp as received from GitHub. |
| merged_at_raw | STRING | Yes | Timestamp when the pull request was merged, when applicable. |
| merge_commit_sha | STRING | Yes | SHA of the merge commit associated with the pull request. |
| fetched_at_utc_raw | STRING | No | UTC timestamp when the API record was downloaded. |
| batch_id | STRING | No | Unique identifier for the ingestion batch. |
| source_load_type | STRING | No | Indicates `full` or `incremental` source extraction. |
| execution_mode | STRING | No | Indicates `standard` or `backfill` execution. |
| source_file | STRING | No | Path of the raw source file. |
| schema_version | STRING | No | Version of the Bronze schema. |
| load_timestamp | TIMESTAMP | No | Timestamp when the record was written to Bronze. |

---

# Bronze Layer Idempotency

Bronze tables preserve historical ingestion batches.

The pipeline uses Delta Lake `MERGE` operations rather than unconditional append operations.

The following logical merge keys are used:

| Table | Merge Key |
|---|---|
| bronze_repositories | `repo_id + batch_id` |
| bronze_commits | `commit_sha + batch_id` |
| bronze_issues | `issue_id + batch_id` |
| bronze_pull_requests | `pr_id + batch_id` |

If the exact same batch is processed more than once, previously loaded records are matched and are not inserted again.

---

# Bronze Schema Enforcement

Raw JSON files are read using explicitly defined PySpark schemas created with:

- `StructType`
- `StructField`

Spark schema inference is not used.

The pipeline does not use:

`inferSchema=True`

This ensures that the expected data contract is explicitly controlled by the pipeline.

---

# Bronze Schema Drift Handling

Malformed or non-conforming records are not allowed to terminate the entire batch.

Invalid records can be written to the operational table:

`quarantine_records`

The quarantine table stores:

- dataset
- processing layer
- batch ID
- source file
- business key
- error reason
- raw record representation
- load timestamp

Unexpected additive source fields remain available in the immutable raw files and can be incorporated into a later approved schema version.