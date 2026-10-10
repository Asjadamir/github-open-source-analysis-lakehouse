# github-open-source-analysis

## Data Source and API Endpoints

**Data source:** GitHub REST API  
**Base URL:** https://api.github.com  
**Tracked repositories:** 50 public repositories

### 1. Repository Metadata

- **Endpoint:** `GET /repos/{owner}/{repo}`
- **Purpose:** Collect repository ID, name, description, programming language, stars, forks, creation date, and last update date.
- **Example:** https://api.github.com/repos/apache/spark

### 2. Commit History

- **Endpoint:** `GET /repos/{owner}/{repo}/commits`
- **Full-load parameters:** `per_page=100`
- **Incremental parameter:** `since={WATERMARK_UTC}`
- **Purpose:** Analyze commit activity and development trends.

### 3. Issues

- **Endpoint:** `GET /repos/{owner}/{repo}/issues`
- **Full-load parameters:** `state=all&sort=updated&direction=desc&per_page=100`
- **Incremental parameter:** `since={WATERMARK_UTC}`
- **Purpose:** Analyze issue creation, updates, status, and maintenance activity.

Note: The issues endpoint may include pull requests. Records containing a `pull_request` field will be identified and handled separately during Silver-layer processing.

### 4. Pull Requests

- **Endpoint:** `GET /repos/{owner}/{repo}/pulls`
- **Full-load parameters:** `state=all&sort=updated&direction=desc&per_page=100`
- **Incremental strategy:** Retrieve recently updated pull requests and filter using `updated_at` and the previous watermark.
- **Purpose:** Analyze collaboration, pull-request activity, and merge trends.

### Ingestion Strategy

**Full Load:** Fetch current repository metadata and up to 100 recent commits, 100 issue-endpoint records, and 100 pull requests per repository.

**Incremental Load:** On a later date, fetch commits and issues since the previous watermark, identify updated pull requests, and retrieve a fresh snapshot of repository metadata.

The pipeline will use pagination, timestamps, and record identifiers to support incremental processing and deduplication.
