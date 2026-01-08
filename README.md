# pubmed
<a href="https://github.com/biobricks-ai/pubmed/actions"><img src="https://github.com/biobricks-ai/pubmed/actions/workflows/bricktools-check.yaml/badge.svg?branch=main"/></a>

## Description

> PubMed comprises more than 38 million citations for biomedical literature from MEDLINE, life science journals, and online books.

## Data

| Field | Description |
|-------|-------------|
| **Records** | 38,201,553 |
| **Baseline** | pubmed25 (January 2025) |
| **Last Updated** | 2025-12-30 |
| **Schema Version** | v2 (extracted columns) |

### Schema

The v2 schema extracts commonly-searched fields as columns for fast querying:

| Column | Type | Description |
|--------|------|-------------|
| PMID | INT64 | PubMed ID (primary key) |
| Title | STRING | Article title |
| Abstract | STRING | Full abstract text |
| DOI | STRING | Digital Object Identifier |
| Year | INT32 | Publication year |
| Authors | STRING | Semicolon-separated author names |
| Journal | STRING | Journal title |
| MeSH_Terms | STRING | Semicolon-separated MeSH headings |
| Keywords | STRING | Author keywords |
| Pub_Types | STRING | Publication types (e.g., "Review", "Clinical Trial") |
| Date_Created | STRING | When added to PubMed (YYYY-MM-DD) |
| Date_Revised | STRING | Last revision date (YYYY-MM-DD) |
| JSON | STRING | Full record for edge cases (simplified) |

### File Structure

Data is partitioned by year for efficient queries:

```
brick/
├── metadata.json
└── pubmed.parquet/
    ├── year=1950/
    │   └── pubmed25n0001.parquet
    ├── year=1951/
    │   └── ...
    └── year=2025/
        └── ...
```

## Usage

### R
```r
biobricks::brick_install("pubmed")
biobricks::brick_pull("pubmed")
biobricks::brick_load("pubmed")
```

### Python (DuckDB)
```python
import duckdb

db = duckdb.connect()

# Fast title/abstract search (~2-7 seconds for 38M records)
results = db.execute("""
    SELECT PMID, Title, Year, DOI
    FROM 'brick/pubmed.parquet/**/*.parquet'
    WHERE Title ILIKE '%toxicity%' OR Abstract ILIKE '%toxicity%'
    LIMIT 100
""").fetchall()

# Year-filtered search (uses partition pruning, very fast)
results = db.execute("""
    SELECT PMID, Title, Year
    FROM 'brick/pubmed.parquet/**/*.parquet'
    WHERE Year >= 2020
      AND MeSH_Terms ILIKE '%Endocrine Disruptors%'
""").fetchall()
```

## Performance

| Query Type | Time | Notes |
|------------|------|-------|
| Title search | ~2s | Direct column scan |
| Year-filtered search | ~1-3s | Partition pruning |
| Full abstract search | ~7s | Scans all 38M records |
| Old JSON parsing | ~50s | v1 schema (deprecated) |

## Rebuilding

To rebuild from the NCBI baseline:

```bash
# Download and process 2025 baseline (~2-3 hours)
python stages_v2/download_and_process_baseline.py --workers 4

# Run tests
python tests/test_brick_v2.py
```

## Source

- NCBI PubMed: https://pubmed.ncbi.nlm.nih.gov/
- Baseline FTP: https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/

## Build History

| Date | Baseline | Records | Notes |
|------|----------|---------|-------|
| 2025-12-30 | pubmed25 | 38,201,553 | v2 schema with extracted columns |
| 2023-08-30 | pubmed23 | ~35,000,000 | v1 schema (PMID, JSON only) |
