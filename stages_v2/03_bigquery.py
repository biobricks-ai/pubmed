#!/usr/bin/env python3
"""
Load pubmed parquet into BigQuery and write a manifest.
Uploads to GCS staging, then loads into BigQuery with year partitioning
and a search index over Title + Abstract.

Run from the pubmed brick root:
    python3 stages_v2/03_bigquery.py
"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT     = "toxindex"
DATASET     = "pubmed"
TABLE       = "articles"
GCS_STAGING = f"gs://biobricks-ai/pubmed"
BRICK_DIR   = Path("brick")
PARQUET_DIR = BRICK_DIR / "pubmed.parquet"
MANIFEST_PATH = BRICK_DIR / "bigquery_manifest.json"


def run(cmd: list, check=True) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip(), file=sys.stderr)
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return result


def ensure_dataset() -> None:
    r = run(["bq", "show", "--project_id", PROJECT, DATASET], check=False)
    if r.returncode != 0:
        print(f"Creating dataset {PROJECT}:{DATASET}")
        run(["bq", "mk", "--dataset",
             "--location=US",
             "--description=PubMed articles (PMID, Title, Abstract, Authors, MeSH, etc.)",
             f"{PROJECT}:{DATASET}"])
    else:
        print(f"Dataset {PROJECT}:{DATASET} already exists")


def get_row_count() -> int:
    r = run(["bq", "query", "--nouse_legacy_sql", "--format=csv",
             f"SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.{TABLE}`"], check=False)
    lines = r.stdout.strip().split('\n')
    try:
        return int(lines[-1])
    except (ValueError, IndexError):
        return -1


def main():
    if not PARQUET_DIR.exists():
        print(f"ERROR: {PARQUET_DIR} not found", file=sys.stderr)
        sys.exit(1)

    ensure_dataset()

    gcs_dir = f"{GCS_STAGING}/pubmed.parquet"

    # ── 1. Upload parquet dir to GCS ─────────────────────────────────────────
    print(f"\nUploading {PARQUET_DIR}/ -> {gcs_dir}/")
    run(["gsutil", "-m", "-q", "rsync", "-r", str(PARQUET_DIR), gcs_dir])

    # ── 2. Load into BigQuery (replace, year-partitioned) ────────────────────
    print(f"\nLoading into {PROJECT}:{DATASET}.{TABLE}")
    run([
        "bq", "load",
        "--source_format=PARQUET",
        "--replace",
        "--hive_partitioning_mode=AUTO",
        f"--hive_partitioning_source_uri_prefix={gcs_dir}",
        f"{PROJECT}:{DATASET}.{TABLE}",
        f"{gcs_dir}/*",
    ])

    rows = get_row_count()
    print(f"  {rows:,} rows loaded")

    # ── 3. Create search index on Title + Abstract ───────────────────────────
    print(f"\nCreating search index on Title and Abstract ...")
    run([
        "bq", "query", "--nouse_legacy_sql",
        f"""
        CREATE SEARCH INDEX IF NOT EXISTS pubmed_text_idx
        ON `{PROJECT}.{DATASET}.{TABLE}` (Title, Abstract)
        """
    ])
    print("Search index created.")

    # ── 4. Write manifest ────────────────────────────────────────────────────
    size_gb = sum(f.stat().st_size for f in PARQUET_DIR.rglob("*.parquet")) / 1e9
    manifest = {
        "project": PROJECT,
        "dataset": DATASET,
        "table": TABLE,
        "gcs_staging": gcs_dir,
        "rows": rows,
        "size_gb": round(size_gb, 3),
        "partitioned_by": "Year",
        "search_index": "Title, Abstract",
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "example_query": (
            f"SELECT PMID, Title, Abstract, Year FROM `{PROJECT}.{DATASET}.{TABLE}` "
            f"WHERE SEARCH(articles, 'naphthalene') LIMIT 100"
        ),
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))
    print(f"\nManifest written to {MANIFEST_PATH}")

    print("\n=== BigQuery load complete ===")
    print(f"Dataset: https://console.cloud.google.com/bigquery?project={PROJECT}&ws=!1m4!1m3!3m2!1s{PROJECT}!2s{DATASET}")
    print(f"\nExample query:")
    print(f"  SELECT PMID, Title, Abstract, Year")
    print(f"  FROM `{PROJECT}.{DATASET}.{TABLE}`")
    print(f"  WHERE SEARCH(articles, 'naphthalene')")
    print(f"  LIMIT 100")


if __name__ == "__main__":
    main()
