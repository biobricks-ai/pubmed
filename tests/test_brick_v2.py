#!/usr/bin/env python3
"""
Tests for PubMed brick v2 data integrity.

These tests validate that the local parquet data matches PubMed API records.

Usage:
    python tests/test_brick_v2.py

    # Or with pytest:
    pytest tests/test_brick_v2.py -v
"""

import re
import time
from pathlib import Path

import duckdb

# pytest is optional - tests can run standalone
try:
    import pytest
    HAS_PYTEST = True
except ImportError:
    HAS_PYTEST = False
    # Create a dummy pytest module for decorators
    class pytest:
        @staticmethod
        def skip(msg):
            print(f"SKIP: {msg}")
            return None
        @staticmethod
        def mark():
            pass
    pytest.mark = type('mark', (), {'skipif': lambda *a, **k: lambda f: f})()

# Try to import Entrez for API comparison tests
try:
    from Bio import Entrez
    Entrez.email = 'test@example.com'
    HAS_ENTREZ = True
except ImportError:
    HAS_ENTREZ = False

# Path to brick parquet (v2 schema is now in standard brick/ location)
BRICK_V2_PATH = Path(__file__).parent.parent / "brick" / "pubmed.parquet"


def get_db_connection():
    """Get DuckDB connection to brick v2."""
    if not BRICK_V2_PATH.exists():
        raise FileNotFoundError(f"Brick v2 not found at {BRICK_V2_PATH}")
    return duckdb.connect()


class TestBrickSchema:
    """Test that the brick has the expected schema."""

    def test_schema_columns(self):
        """Verify all expected columns exist."""
        db = get_db_connection()

        result = db.execute(f'''
            DESCRIBE SELECT * FROM "{BRICK_V2_PATH}/**/*.parquet" LIMIT 1
        ''').fetchall()

        columns = {row[0] for row in result}

        expected_columns = {
            'PMID', 'Title', 'Abstract', 'DOI', 'Year',
            'Authors', 'Journal', 'MeSH_Terms', 'Keywords',
            'Pub_Types', 'Date_Created', 'Date_Revised', 'JSON'
        }

        assert expected_columns.issubset(columns), \
            f"Missing columns: {expected_columns - columns}"

    def test_pmid_is_integer(self):
        """Verify PMIDs are stored as integers."""
        db = get_db_connection()

        result = db.execute(f'''
            SELECT typeof(PMID)
            FROM "{BRICK_V2_PATH}/**/*.parquet"
            LIMIT 1
        ''').fetchone()

        assert result[0] in ('BIGINT', 'INTEGER'), \
            f"PMID should be integer, got {result[0]}"

    def test_year_is_integer(self):
        """Verify Year is stored as integer."""
        db = get_db_connection()

        result = db.execute(f'''
            SELECT typeof(Year)
            FROM "{BRICK_V2_PATH}/**/*.parquet"
            WHERE Year IS NOT NULL
            LIMIT 1
        ''').fetchone()

        assert result[0] in ('BIGINT', 'INTEGER'), \
            f"Year should be integer, got {result[0]}"


class TestBrickData:
    """Test data integrity and completeness."""

    def test_record_count(self):
        """Verify we have a reasonable number of records."""
        db = get_db_connection()

        result = db.execute(f'''
            SELECT COUNT(*) FROM "{BRICK_V2_PATH}/**/*.parquet"
        ''').fetchone()

        count = result[0]

        # Should have at least 35 million records
        assert count >= 35_000_000, \
            f"Expected at least 35M records, got {count:,}"

        print(f"Total records: {count:,}")

    def test_year_distribution(self):
        """Verify we have records across multiple years."""
        db = get_db_connection()

        result = db.execute(f'''
            SELECT Year, COUNT(*) as cnt
            FROM "{BRICK_V2_PATH}/**/*.parquet"
            WHERE Year BETWEEN 1950 AND 2025
            GROUP BY Year
            ORDER BY Year
        ''').fetchall()

        years = {row[0] for row in result}

        # Should have records from at least 50 different years
        assert len(years) >= 50, \
            f"Expected at least 50 years of data, got {len(years)}"

        # Should have recent data (2024)
        assert 2024 in years, "Missing 2024 data"

        # Should have older data (1990)
        assert 1990 in years, "Missing 1990 data"

    def test_titles_not_empty(self):
        """Verify most records have titles."""
        db = get_db_connection()

        result = db.execute(f'''
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN Title IS NOT NULL AND length(Title) > 0 THEN 1 END) as with_title
            FROM "{BRICK_V2_PATH}/**/*.parquet"
        ''').fetchone()

        total, with_title = result
        pct_with_title = with_title / total * 100

        # At least 95% should have titles
        assert pct_with_title >= 95, \
            f"Only {pct_with_title:.1f}% of records have titles"

    def test_sample_pmids_exist(self):
        """Test that some known PMIDs exist and have data."""
        db = get_db_connection()

        # These are well-known papers that should be in PubMed
        known_pmids = [
            25383517,  # CRISPR paper
            29618526,  # A highly cited paper
            32283116,  # COVID-related paper
        ]

        for pmid in known_pmids:
            result = db.execute(f'''
                SELECT PMID, Title, Year
                FROM "{BRICK_V2_PATH}/**/*.parquet"
                WHERE PMID = {pmid}
            ''').fetchone()

            assert result is not None, f"PMID {pmid} not found in brick"
            assert result[1] is not None, f"PMID {pmid} has no title"
            print(f"PMID {pmid}: {result[1][:60]}... ({result[2]})")


class TestBrickQuery:
    """Test query functionality."""

    def test_simple_title_search(self):
        """Test that title search works."""
        db = get_db_connection()

        result = db.execute(f'''
            SELECT COUNT(*)
            FROM "{BRICK_V2_PATH}/**/*.parquet"
            WHERE Title ILIKE '%cancer%'
        ''').fetchone()

        count = result[0]

        # Should find many cancer-related papers
        assert count >= 1_000_000, \
            f"Expected at least 1M cancer papers, got {count:,}"

    def test_year_filter_uses_partitions(self):
        """Test that year filtering is efficient (uses partition pruning)."""
        db = get_db_connection()

        # Query with year filter should be fast
        import time
        start = time.time()

        result = db.execute(f'''
            SELECT COUNT(*)
            FROM "{BRICK_V2_PATH}/**/*.parquet"
            WHERE Year = 2023
        ''').fetchone()

        elapsed = time.time() - start

        # Should complete in under 5 seconds with partition pruning
        assert elapsed < 5, \
            f"Year-filtered query took {elapsed:.1f}s, expected < 5s"

        print(f"Year 2023 count: {result[0]:,} in {elapsed:.2f}s")

    def test_combined_search(self):
        """Test combining year and text search."""
        db = get_db_connection()

        result = db.execute(f'''
            SELECT PMID, Title, Year
            FROM "{BRICK_V2_PATH}/**/*.parquet"
            WHERE Year >= 2020
              AND (Title ILIKE '%toxicity%' OR Abstract ILIKE '%toxicity%')
            LIMIT 10
        ''').fetchall()

        assert len(result) == 10, "Should find 10 toxicity papers from 2020+"

        for pmid, title, year in result:
            assert year >= 2020, f"PMID {pmid} has year {year}, expected >= 2020"


@pytest.mark.skipif(not HAS_ENTREZ, reason="Biopython not installed")
class TestAPIComparison:
    """Compare brick data against PubMed API."""

    def _strip_html(self, text):
        """Remove HTML tags from text."""
        return re.sub(r'<[^>]+>', '', text) if text else ''

    def test_random_records_match_api(self):
        """Test that random records match PubMed API."""
        db = get_db_connection()

        # Get 3 random records with good data
        brick_records = db.execute(f'''
            SELECT PMID, Title, DOI
            FROM "{BRICK_V2_PATH}/**/*.parquet"
            WHERE Year BETWEEN 2020 AND 2024
              AND Title IS NOT NULL
              AND DOI IS NOT NULL
              AND length(Title) > 20
            ORDER BY random()
            LIMIT 3
        ''').fetchall()

        matches = 0

        for pmid, brick_title, brick_doi in brick_records:
            try:
                with Entrez.efetch(db='pubmed', id=str(pmid),
                                   rettype='medline', retmode='xml') as h:
                    records = Entrez.read(h)

                if records['PubmedArticle']:
                    api_record = records['PubmedArticle'][0]
                    api_title = api_record['MedlineCitation']['Article'].get('ArticleTitle', '')
                    api_title_clean = self._strip_html(api_title)

                    # Get DOI
                    api_doi = ''
                    for aid in api_record.get('PubmedData', {}).get('ArticleIdList', []):
                        if aid.attributes.get('IdType') == 'doi':
                            api_doi = str(aid)
                            break

                    # Compare (allow for HTML tag differences in title)
                    title_match = (brick_title.strip() == api_title.strip() or
                                   brick_title.strip() == api_title_clean.strip())
                    doi_match = (brick_doi or '') == (api_doi or '')

                    if title_match and doi_match:
                        matches += 1
                    else:
                        print(f"Mismatch for PMID {pmid}:")
                        if not title_match:
                            print(f"  Brick: {brick_title[:60]}")
                            print(f"  API:   {api_title[:60]}")
                        if not doi_match:
                            print(f"  Brick DOI: {brick_doi}")
                            print(f"  API DOI:   {api_doi}")

                time.sleep(0.5)  # Rate limit

            except Exception as e:
                print(f"Error checking PMID {pmid}: {e}")

        # At least 2/3 should match
        assert matches >= 2, \
            f"Only {matches}/3 records matched API"


def run_all_tests():
    """Run all tests without pytest."""
    print("=" * 60)
    print("PubMed Brick v2 Tests")
    print("=" * 60)

    # Schema tests
    print("\n--- Schema Tests ---")
    schema_tests = TestBrickSchema()
    schema_tests.test_schema_columns()
    print("✓ Schema columns OK")
    schema_tests.test_pmid_is_integer()
    print("✓ PMID type OK")
    schema_tests.test_year_is_integer()
    print("✓ Year type OK")

    # Data tests
    print("\n--- Data Tests ---")
    data_tests = TestBrickData()
    data_tests.test_record_count()
    print("✓ Record count OK")
    data_tests.test_year_distribution()
    print("✓ Year distribution OK")
    data_tests.test_titles_not_empty()
    print("✓ Titles OK")
    data_tests.test_sample_pmids_exist()
    print("✓ Sample PMIDs OK")

    # Query tests
    print("\n--- Query Tests ---")
    query_tests = TestBrickQuery()
    query_tests.test_simple_title_search()
    print("✓ Title search OK")
    query_tests.test_year_filter_uses_partitions()
    print("✓ Year filter OK")
    query_tests.test_combined_search()
    print("✓ Combined search OK")

    # API comparison (if available)
    if HAS_ENTREZ:
        print("\n--- API Comparison Tests ---")
        api_tests = TestAPIComparison()
        api_tests.test_random_records_match_api()
        print("✓ API comparison OK")
    else:
        print("\n--- API Comparison Tests ---")
        print("⚠ Skipped (Biopython not installed)")

    print("\n" + "=" * 60)
    print("All tests passed!")
    print("=" * 60)


if __name__ == '__main__':
    run_all_tests()
