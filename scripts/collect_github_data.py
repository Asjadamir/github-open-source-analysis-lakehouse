import time
import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import gzip

import requests


BASE_URL = "https://api.github.com"
API_VERSION = "2022-11-28"
PAGE_SIZE = 100

STATE_FILE = Path("state/last_watermark.json")

REPOSITORIES = [
    # Big Data and Data Engineering
    "apache/spark",
    "apache/kafka",
    "apache/flink",
    "apache/airflow",
    "apache/beam",
    "apache/arrow",
    "apache/iceberg",
    "apache/hadoop",
    "apache/superset",

    # AI, Machine Learning and Data Science
    "tensorflow/tensorflow",
    "pytorch/pytorch",
    "huggingface/transformers",
    "scikit-learn/scikit-learn",
    "numpy/numpy",
    "pandas-dev/pandas",

    # Programming Languages and Developer Tools
    "microsoft/vscode",
    "nodejs/node",
    "rust-lang/rust",
    "golang/go",
    "microsoft/TypeScript",
    "llvm/llvm-project",

    # Frontend Development
    "facebook/react",
    "angular/angular",
    "vuejs/core",
    "sveltejs/svelte",
    "vercel/next.js",

    # Backend Development
    "django/django",
    "spring-projects/spring-boot",
    "expressjs/express",
    "nestjs/nest",
    "fastapi/fastapi",
    "pallets/flask",
    "laravel/laravel",
    "dotnet/runtime",

    # Cloud, Infrastructure and DevOps
    "kubernetes/kubernetes",
    "hashicorp/terraform",
    "ansible/ansible",
    "moby/moby",
    "helm/helm",
    "istio/istio",
    "envoyproxy/envoy",

    # Databases and Monitoring
    "elastic/elasticsearch",
    "grafana/grafana",
    "prometheus/prometheus",
    "redis/redis",
    "mongodb/mongo",
    "ClickHouse/ClickHouse",
    "postgres/postgres",

    # Other Developer Platforms
    "denoland/deno",
    "oven-sh/bun",
]

# --------------------------------------------------
# API SESSION
# --------------------------------------------------

token = os.getenv("GITHUB_TOKEN")

if not token:
    raise SystemExit(
        "ERROR: GITHUB_TOKEN is not set.\n"
        "Set your GitHub personal access token as an "
        "environment variable and try again."
    )

session = requests.Session()

session.headers.update({
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {token}",
    "X-GitHub-Api-Version": API_VERSION,
    "User-Agent": "github-open-source-analytics",
})


# --------------------------------------------------
# HELPER FUNCTIONS
# --------------------------------------------------

def utc_now():
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc).replace(microsecond=0)


def format_timestamp(value):
    """Format a UTC timestamp for GitHub API requests."""
    return value.isoformat().replace("+00:00", "Z")


def api_get(endpoint, params=None, max_attempts=5):
    """Call GitHub API with timeout handling and automatic retries."""

    url = f"{BASE_URL}{endpoint}"

    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(
                url,
                params=params,
                timeout=(10, 90),
            )

            # Handle rate limits and access errors.
            if response.status_code in (403, 429):
                remaining = response.headers.get(
                    "X-RateLimit-Remaining", "unknown"
                )
                reset = response.headers.get(
                    "X-RateLimit-Reset", "unknown"
                )

                raise RuntimeError(
                    f"GitHub API rate-limit/access error: {endpoint}\n"
                    f"Remaining requests: {remaining}\n"
                    f"Reset timestamp: {reset}\n"
                    f"Response: {response.text[:300]}"
                )

            # Retry temporary GitHub server errors.
            if response.status_code in (500, 502, 503, 504):
                if attempt < max_attempts:
                    delay = min(2 ** attempt, 30)
                    print(
                        f"Server error {response.status_code}. "
                        f"Retrying in {delay}s "
                        f"(attempt {attempt}/{max_attempts})..."
                    )
                    time.sleep(delay)
                    continue

            response.raise_for_status()
            return response.json()

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as error:
            if attempt == max_attempts:
                raise

            delay = min(2 ** attempt, 30)

            print(
                f"Connection problem: {error}\n"
                f"Retrying in {delay}s "
                f"(attempt {attempt}/{max_attempts})..."
            )

            time.sleep(delay)

    raise RuntimeError(
        f"GitHub API request failed after {max_attempts} attempts: "
        f"{endpoint}"
    )


def api_get_list_or_empty(endpoint, params=None):
    """
    Fetch an activity endpoint.
    If GitHub returns 404, warn and skip that endpoint.
    Other errors are still raised.
    """
    try:
        result = api_get(endpoint, params)

    except requests.exceptions.HTTPError as error:
        if (
            error.response is not None
            and error.response.status_code == 404
        ):
            print(
                f"WARNING: Endpoint unavailable (404): "
                f"{endpoint}. Skipping this dataset."
            )
            return []

        raise

    if not isinstance(result, list):
        raise RuntimeError(
            f"Expected a list response from {endpoint}"
        )

    return result


def wrap_record(repository, fetched_at, payload):
    """
    Preserve the API record and attach ingestion metadata.
    The original API record remains inside payload.
    """
    return {
        "source_repository": repository,
        "fetched_at_utc": fetched_at,
        "payload": payload,
    }


def fetch_first_page(repository, endpoint, params, fetched_at):
    """Fetch at most 100 records for the bounded full load."""

    query = {
        **params,
        "per_page": PAGE_SIZE,
        "page": 1,
    }

    response = api_get_list_or_empty(
        f"/repos/{repository}/{endpoint}",
        query,
    )

    if not isinstance(response, list):
        raise RuntimeError(
            f"Expected a list from {endpoint} for {repository}"
        )

    return [
        wrap_record(repository, fetched_at, item)
        for item in response
    ]


def fetch_incremental_pages(
    repository,
    endpoint,
    params,
    fetched_at,
    stop_at=None,
):
    """
    Fetch paginated incremental records.

    For commits and issues, GitHub applies the since filter.
    For pull requests, stop_at filters updated_at locally
    because the pull requests endpoint has no since parameter.
    """

    records = []
    page = 1

    while True:
        query = {
            **params,
            "per_page": PAGE_SIZE,
            "page": page,
        }

        batch = api_get_list_or_empty(
            f"/repos/{repository}/{endpoint}",
            query,
        )

        if not isinstance(batch, list):
            raise RuntimeError(
                f"Expected a list from {endpoint} "
                f"for {repository}"
            )

        if not batch:
            break

        reached_watermark = False

        for item in batch:
            updated_at = item.get("updated_at")

            if (
                stop_at
                and updated_at
                and updated_at <= stop_at
            ):
                reached_watermark = True
                continue

            records.append(
                wrap_record(repository, fetched_at, item)
            )

        # Pull requests are sorted by most recently updated.
        # Stop when the previous watermark is reached.
        if stop_at and reached_watermark:
            break

        if len(batch) < PAGE_SIZE:
            break

        page += 1

    return records


def save_json(path, data):
    """Save JSON using UTF-8 encoding."""

    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(
            data,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def save_json_gzip(path, data):
    """Save JSON as a compressed .json.gz file."""

    compressed_path = Path(str(path) + ".gz")
    compressed_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with gzip.open(
        compressed_path,
        "wt",
        encoding="utf-8",
    ) as file:
        json.dump(
            data,
            file,
            ensure_ascii=False,
        )

    size_mb = compressed_path.stat().st_size / (1024 * 1024)

    if compressed_path.stat().st_size >= 100 * 1024 * 1024:
        compressed_path.unlink()
        raise RuntimeError(
            f"Compressed file still exceeds GitHub's limit: "
            f"{compressed_path} ({size_mb:.2f} MB)"
        )

    print(f"Saved {compressed_path} ({size_mb:.2f} MB)")
    return compressed_path


def load_watermark():
    """Load the timestamp from the previous successful run."""

    if not STATE_FILE.exists():
        raise SystemExit(
            "ERROR: No watermark found.\n"
            "Run full extraction before incremental extraction."
        )

    state = json.loads(
        STATE_FILE.read_text(encoding="utf-8")
    )

    return state["watermark_utc"]


# --------------------------------------------------
# MAIN EXTRACTION PIPELINE
# --------------------------------------------------

def collect_data(mode):
    if len(REPOSITORIES) != 50:
        raise RuntimeError(
            f"Expected 50 repositories, "
            f"found {len(REPOSITORIES)}."
        )

    # Capture the starting timestamp for this extraction.
    started = utc_now()
    fetched_at = format_timestamp(started)

    previous_watermark = None

    if mode == "incremental":
        previous_watermark = load_watermark()

    output_type = (
        "full_load"
        if mode == "full"
        else "incremental_load"
    )

    output_dir = (
        Path("data")
        / output_type
        / started.strftime("%Y-%m-%d_%H%M%S")
    )

    repositories_data = []
    commits_data = []
    issues_data = []
    pull_requests_data = []

    print("=" * 60)
    print(f"GitHub Open Source Analytics: {mode.upper()}")
    print(f"Repositories: {len(REPOSITORIES)}")
    print(f"Started at UTC: {fetched_at}")
    print("=" * 60)

    for index, repository in enumerate(REPOSITORIES, start=1):
        print(
            f"[{index}/{len(REPOSITORIES)}] "
            f"Collecting {repository}"
        )

        # 1. Repository metadata
        repository_payload = api_get(
            f"/repos/{repository}"
        )

        repositories_data.append(
            wrap_record(
                repository,
                fetched_at,
                repository_payload,
            )
        )

        if mode == "full":

            # 2. Recent commits
            commits_data.extend(
                fetch_first_page(
                    repository,
                    "commits",
                    {},
                    fetched_at,
                )
            )

            # 3. Recent issues, including any PRs returned
            # by the GitHub issues endpoint
            issues_data.extend(
                fetch_first_page(
                    repository,
                    "issues",
                    {
                        "state": "all",
                        "sort": "updated",
                        "direction": "desc",
                    },
                    fetched_at,
                )
            )

            # 4. Recent pull requests
            pull_requests_data.extend(
                fetch_first_page(
                    repository,
                    "pulls",
                    {
                        "state": "all",
                        "sort": "updated",
                        "direction": "desc",
                    },
                    fetched_at,
                )
            )

        else:

            # 2. Commits after the previous watermark
            commits_data.extend(
                fetch_incremental_pages(
                    repository,
                    "commits",
                    {"since": previous_watermark},
                    fetched_at,
                )
            )

            # 3. Issues updated after the watermark
            issues_data.extend(
                fetch_incremental_pages(
                    repository,
                    "issues",
                    {
                        "since": previous_watermark,
                        "state": "all",
                        "sort": "updated",
                        "direction": "desc",
                    },
                    fetched_at,
                )
            )

            # 4. Pull requests updated after the watermark.
            # This endpoint does not support since directly.
            pull_requests_data.extend(
                fetch_incremental_pages(
                    repository,
                    "pulls",
                    {
                        "state": "all",
                        "sort": "updated",
                        "direction": "desc",
                    },
                    fetched_at,
                    stop_at=previous_watermark,
                )
            )

    # --------------------------------------------------
    # SAVE DATASETS
    # --------------------------------------------------

    datasets = {
        "repositories.json": repositories_data,
        "commits.json": commits_data,
        "issues.json": issues_data,
        "pull_requests.json": pull_requests_data,
    }

    for filename, records in datasets.items():
        save_json_gzip(output_dir / filename, records)

    finished_at = format_timestamp(utc_now())

    metadata = {
        "mode": mode,
        "started_at_utc": fetched_at,
        "finished_at_utc": finished_at,
        "previous_watermark_utc": previous_watermark,
        "next_watermark_utc": fetched_at,
        "tracked_repository_count": len(REPOSITORIES),
        "tracked_repositories": REPOSITORIES,
        "record_counts": {
            filename: len(records)
            for filename, records in datasets.items()
        },
        "output_directory": str(output_dir),
    }

    save_json(
        output_dir / "run_metadata.json",
        metadata,
    )

    # Update the watermark only after all extraction and
    # output-writing steps have completed successfully.
    STATE_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    save_json(
        STATE_FILE,
        {
            "watermark_utc": fetched_at,
            "last_mode": mode,
            "last_output_directory": str(output_dir),
        },
    )

    print("\nExtraction completed successfully.")
    print(f"Output directory: {output_dir}")
    print(f"Finished at UTC: {finished_at}")
    print("\nRecord counts:")

    for filename, count in metadata["record_counts"].items():
        print(f"  {filename}: {count}")

    print(f"\nWatermark saved to: {STATE_FILE}")


# --------------------------------------------------
# COMMAND-LINE ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Extract GitHub repository data for "
            "the Open Source Analytics Lakehouse."
        )
    )

    parser.add_argument(
        "mode",
        choices=["full", "incremental"],
        help="Choose full or incremental extraction.",
    )

    arguments = parser.parse_args()

    try:
        collect_data(arguments.mode)
    except requests.RequestException as error:
        raise SystemExit(
            f"GitHub API request failed: {error}"
        )
    except (RuntimeError, KeyError, ValueError) as error:
        raise SystemExit(str(error))
