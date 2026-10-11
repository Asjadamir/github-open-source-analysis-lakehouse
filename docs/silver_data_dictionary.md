# Silver Layer Data Dictionary

## Overview

The Silver layer contains cleaned, validated, typed, and analytics-ready records produced from the Bronze layer.

Major Silver transformations include:

- Strict type casting
- Timestamp conversion
- Removal of duplicate records
- Separation of issues and pull requests
- PII hashing
- Validation of business keys
- Repository ID enrichment
- Record-level change detection
- Delta Lake MERGE-based upserts

Every Silver table includes:

- `record_hash`
- `source_batch_id`
- `load_timestamp`

The Silver layer represents the latest cleansed state of each logical record.

> Note: Primary keys listed below are logical pipeline keys. Delta Lake does not physically enforce these primary-key constraints.

---

# 1. silver_repositories

## Purpose

Stores the latest validated and cleansed state of each tracked GitHub repository.

## Logical Primary Key

`repo_id`

| Column | Data Type | Nullable | Description |
|---|---|---|---|
| repo_id | BIGINT | No | Unique GitHub repository identifier. |
| source_repository | STRING | No | Repository name in `owner/repository` format. |
| name | STRING | Yes | Repository name. |
| full_name | STRING | Yes | Full repository name including owner. |
| owner_login | STRING | Yes | GitHub login of the repository owner or organization. |
| description | STRING | Yes | Repository description. |
| language | STRING | Yes | Primary programming language. |
| stars | BIGINT | Yes | Current number of GitHub stars. |
| forks | BIGINT | Yes | Current number of repository forks. |
| open_issues | BIGINT | Yes | Current number of open issues reported by GitHub. |
| default_branch | STRING | Yes | Repository default branch. |
| archived | BOOLEAN | Yes | Indicates whether the repository has been archived. |
| disabled | BOOLEAN | Yes | Indicates whether the repository has been disabled. |
| fork | BOOLEAN | Yes | Indicates whether the repository is a fork. |
| created_at | TIMESTAMP | Yes | Repository creation timestamp. |
| updated_at | TIMESTAMP | Yes | Repository update timestamp. |
| pushed_at | TIMESTAMP | Yes | Timestamp of the most recent repository push. |
| record_hash | STRING | No | SHA-256 hash of business attributes used to identify whether the record changed between batches. |
| source_batch_id | STRING | No | Batch from which the latest Silver representation was produced. |
| load_timestamp | TIMESTAMP | No | Timestamp when the record was inserted or updated in Silver. |

---

# 2. silver_commits

## Purpose

Stores cleaned Git commit records for tracked repositories.

Raw author email addresses and usernames are not retained in readable form in Silver.

## Logical Primary Key

`commit_sha`

## Relationship

`repo_id` references the corresponding repository in `silver_repositories`.

| Column | Data Type | Nullable | Description |
|---|---|---|---|
| commit_sha | STRING | No | Unique Git commit SHA. |
| repo_id | BIGINT | No | GitHub repository identifier obtained by joining to `silver_repositories`. |
| source_repository | STRING | No | Repository in `owner/repository` format. |
| author_login_hash | STRING | Yes | SHA-256 hash of the normalized GitHub author login. |
| author_email_hash | STRING | Yes | SHA-256 hash of the normalized commit author email address. |
| author_date | TIMESTAMP | Yes | Timestamp when the commit was authored. |
| committer_date | TIMESTAMP | Yes | Timestamp when the commit was committed. |
| message | STRING | Yes | Git commit message. |
| record_hash | STRING | No | SHA-256 hash of the business attributes used for change detection. |
| source_batch_id | STRING | No | Batch from which the Silver record was produced. |
| load_timestamp | TIMESTAMP | No | Timestamp when the Silver record was inserted or updated. |

---

# 3. silver_issues

## Purpose

Stores cleaned GitHub issue records.

Records returned by the GitHub Issues API that represent pull requests are excluded from this table.

## Logical Primary Key

`issue_id`

## Relationship

`repo_id` references the corresponding repository in `silver_repositories`.

| Column | Data Type | Nullable | Description |
|---|---|---|---|
| issue_id | BIGINT | No | Unique GitHub identifier for the issue. |
| repo_id | BIGINT | No | GitHub repository identifier. |
| source_repository | STRING | No | Repository in `owner/repository` format. |
| issue_number | BIGINT | Yes | Repository-specific issue number. |
| state | STRING | Yes | Current issue state, such as `open` or `closed`. |
| state_reason | STRING | Yes | GitHub-provided explanation for the issue state where available. |
| title | STRING | Yes | Issue title. |
| user_login_hash | STRING | Yes | SHA-256 hash of the normalized GitHub username of the issue creator. |
| comments | BIGINT | Yes | Number of comments associated with the issue. |
| created_at | TIMESTAMP | Yes | Issue creation timestamp. |
| updated_at | TIMESTAMP | Yes | Issue last-update timestamp. |
| closed_at | TIMESTAMP | Yes | Issue closure timestamp, if closed. |
| record_hash | STRING | No | SHA-256 hash of business attributes used for change detection. |
| source_batch_id | STRING | No | Batch from which the latest Silver record was produced. |
| load_timestamp | TIMESTAMP | No | Timestamp when the record was inserted or updated in Silver. |

---

# 4. silver_pull_requests

## Purpose

Stores cleaned GitHub pull-request records.

## Logical Primary Key

`pr_id`

## Relationship

`repo_id` references the corresponding repository in `silver_repositories`.

| Column | Data Type | Nullable | Description |
|---|---|---|---|
| pr_id | BIGINT | No | Unique GitHub identifier for the pull request. |
| repo_id | BIGINT | No | GitHub repository identifier. |
| source_repository | STRING | No | Repository in `owner/repository` format. |
| pr_number | BIGINT | Yes | Repository-specific pull-request number. |
| state | STRING | Yes | Current pull-request state. |
| title | STRING | Yes | Pull-request title. |
| user_login_hash | STRING | Yes | SHA-256 hash of the normalized pull-request creator login. |
| draft | BOOLEAN | Yes | Indicates whether the pull request is a draft. |
| created_at | TIMESTAMP | Yes | Pull-request creation timestamp. |
| updated_at | TIMESTAMP | Yes | Pull-request last-update timestamp. |
| closed_at | TIMESTAMP | Yes | Pull-request closure timestamp, if applicable. |
| merged_at | TIMESTAMP | Yes | Timestamp when the pull request was merged, if applicable. |
| merge_commit_sha | STRING | Yes | SHA of the merge commit associated with the pull request. |
| record_hash | STRING | No | SHA-256 hash of business attributes used for change detection. |
| source_batch_id | STRING | No | Batch from which the latest Silver record was produced. |
| load_timestamp | TIMESTAMP | No | Timestamp when the record was inserted or updated in Silver. |

---

# Silver Data Type Enforcement

Bronze values are explicitly converted into their required Silver data types.

Examples include:

- Repository IDs: `STRING → BIGINT`
- Star counts: `STRING → BIGINT`
- Fork counts: `STRING → BIGINT`
- Issue numbers: `STRING → BIGINT`
- Comment counts: `STRING → BIGINT`
- Timestamp strings: `STRING → TIMESTAMP`

The pipeline uses safe casting logic such as `try_cast` so an invalid source value does not terminate the complete batch.

Records that violate required Silver contracts can be sent to the quarantine framework for further investigation.

---

# PII Handling

Some GitHub commit payloads can contain identifiable information, including:

- Author email addresses
- GitHub usernames
- Committer information

Raw values may be retained in Bronze for controlled source-level processing.

Before Silver, identifying attributes are removed or transformed.

The following values are hashed using SHA-256:

- Commit author email
- Commit author login
- Issue creator login
- Pull-request creator login

Raw email addresses are not exposed in Silver.

---

# Issue and Pull-Request Separation

The GitHub Issues API can return pull-request objects.

Bronze therefore stores the field:

`is_pull_request`

During Silver processing, only records satisfying:

`is_pull_request = false`

are written to `silver_issues`.

Pull requests are independently processed from `bronze_pull_requests` into `silver_pull_requests`.

This prevents duplication of pull-request records across the Silver issue and pull-request tables.

---

# Silver Idempotency

Silver uses Delta Lake `MERGE` operations.

The logical merge keys are:

| Table | Merge Key |
|---|---|
| silver_repositories | `repo_id` |
| silver_commits | `commit_sha` |
| silver_issues | `issue_id` |
| silver_pull_requests | `pr_id` |

Each source record also receives a deterministic `record_hash`.

The hash is generated from business attributes and does not include:

- `source_batch_id`
- `load_timestamp`

The MERGE strategy is:

1. If the business key does not exist, insert the record.
2. If the business key exists and the `record_hash` changed, update the record.
3. If the business key exists and the `record_hash` is unchanged, perform no insert or update.

Therefore, rerunning the exact same source batch does not create duplicate rows and does not create unnecessary updates.

---

# Silver Metadata

Every Silver record contains:

## source_batch_id

Identifies the raw/Bronze batch responsible for the latest Silver version of the record.

## load_timestamp

Records the timestamp when the record was successfully inserted or updated in the Silver layer.

## record_hash

Supports deterministic change detection and prevents unnecessary updates during idempotent reruns.