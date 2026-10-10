
import gzip
import shutil
from pathlib import Path

folder = Path(
    "data/full_load/2026-10-09_184448"
)

filenames = [
    "repositories.json",
    "commits.json",
    "issues.json",
    "pull_requests.json",
]

for filename in filenames:
    source = folder / filename

    if not source.exists():
        print(f"Skipping missing file: {source}")
        continue

    destination = Path(str(source) + ".gz")

    print(f"Compressing {source}...")

    with source.open("rb") as input_file:
        with gzip.open(destination, "wb") as output_file:
            shutil.copyfileobj(input_file, output_file)

    size_mb = destination.stat().st_size / (1024 * 1024)

    if destination.stat().st_size >= 100 * 1024 * 1024:
        destination.unlink()
        raise SystemExit(
            f"Compressed file is still too large: "
            f"{source} ({size_mb:.2f} MB)"
        )

    # Remove the original only after successful compression.
    source.unlink()

    print(f"Saved {destination} ({size_mb:.2f} MB)")

print("Compression completed.")
