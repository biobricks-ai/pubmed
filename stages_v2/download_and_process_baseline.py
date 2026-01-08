#!/usr/bin/env python3
"""
Download the latest PubMed baseline and process directly into v2 schema.

This script:
1. Downloads baseline XML files from NCBI FTP
2. Processes each file into the v2 parquet schema
3. Partitions output by year
4. Cleans up XML files after processing to save disk space

Usage:
    python stages_v2/download_and_process_baseline.py [--workers 8] [--keep-xml]
"""

import argparse
import gzip
import json
import os
import re
import urllib.request
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Optional
import xml.etree.ElementTree as ET

import pyarrow as pa
import pyarrow.parquet as pq

# FTP base URL for PubMed baseline
BASELINE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"

# v2 schema with extracted columns
SCHEMA = pa.schema([
    ('PMID', pa.int64()),
    ('Title', pa.string()),
    ('Abstract', pa.string()),
    ('DOI', pa.string()),
    ('Year', pa.int32()),
    ('Authors', pa.string()),
    ('Journal', pa.string()),
    ('MeSH_Terms', pa.string()),
    ('Keywords', pa.string()),
    ('Pub_Types', pa.string()),
    ('Date_Created', pa.string()),
    ('Date_Revised', pa.string()),
    ('JSON', pa.string()),
])


def get_baseline_file_list() -> list[str]:
    """Get list of baseline XML files from NCBI FTP."""
    print("Fetching baseline file list from NCBI...")

    with urllib.request.urlopen(BASELINE_URL) as response:
        html = response.read().decode('utf-8')

    # Parse filenames from HTML directory listing
    files = re.findall(r'(pubmed\d+n\d+\.xml\.gz)', html)
    files = sorted(set(files))

    print(f"Found {len(files)} baseline files")
    return files


def download_file(url: str, output_path: Path) -> bool:
    """Download a file from URL to output_path."""
    try:
        urllib.request.urlretrieve(url, output_path)
        return True
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False


def extract_article_fields(article_elem) -> dict:
    """Extract searchable fields from a PubmedArticle XML element."""
    try:
        medline = article_elem.find('MedlineCitation')
        if medline is None:
            return None

        pubmed_data = article_elem.find('PubmedData')
        article = medline.find('Article')

        # PMID
        pmid_elem = medline.find('PMID')
        pmid = int(pmid_elem.text) if pmid_elem is not None and pmid_elem.text else 0

        # Title
        title_elem = article.find('ArticleTitle') if article is not None else None
        title = ''.join(title_elem.itertext()) if title_elem is not None else ''

        # Abstract - handle structured abstracts
        abstract_parts = []
        if article is not None:
            abstract_elem = article.find('Abstract')
            if abstract_elem is not None:
                for text_elem in abstract_elem.findall('AbstractText'):
                    label = text_elem.get('Label', '')
                    content = ''.join(text_elem.itertext())
                    if label:
                        abstract_parts.append(f"{label}: {content}")
                    else:
                        abstract_parts.append(content)
        abstract = ' '.join(abstract_parts)

        # DOI
        doi = ''
        if pubmed_data is not None:
            for aid in pubmed_data.findall('.//ArticleId'):
                if aid.get('IdType') == 'doi':
                    doi = aid.text or ''
                    break

        # Year
        year = None
        if article is not None:
            pub_date = article.find('.//PubDate')
            if pub_date is not None:
                year_elem = pub_date.find('Year')
                if year_elem is not None and year_elem.text:
                    try:
                        year = int(year_elem.text)
                    except ValueError:
                        pass
                # Fallback to MedlineDate
                if year is None:
                    medline_date = pub_date.find('MedlineDate')
                    if medline_date is not None and medline_date.text:
                        match = re.match(r'(\d{4})', medline_date.text)
                        if match:
                            year = int(match.group(1))

        # Authors
        authors = []
        if article is not None:
            for author in article.findall('.//Author'):
                last = author.findtext('LastName') or ''
                first = author.findtext('ForeName') or ''
                if last:
                    authors.append(f"{last}, {first}".strip(', '))
        authors_str = '; '.join(authors)

        # Journal
        journal = ''
        if article is not None:
            journal_elem = article.find('.//Journal/Title')
            if journal_elem is not None:
                journal = journal_elem.text or ''

        # MeSH Terms
        mesh_terms = []
        for mesh in medline.findall('.//MeshHeading/DescriptorName'):
            if mesh.text:
                mesh_terms.append(mesh.text)
        mesh_str = '; '.join(mesh_terms)

        # Keywords
        keywords = []
        for kw in medline.findall('.//Keyword'):
            if kw.text:
                keywords.append(kw.text)
        keywords_str = '; '.join(keywords)

        # Publication Types
        pub_types = []
        if article is not None:
            for pt in article.findall('.//PublicationType'):
                if pt.text:
                    pub_types.append(pt.text)
        pub_types_str = '; '.join(pub_types)

        # Dates
        date_created = ''
        dc = medline.find('DateCreated')
        if dc is not None:
            y = dc.findtext('Year') or ''
            m = dc.findtext('Month') or '01'
            d = dc.findtext('Day') or '01'
            if y:
                date_created = f"{y}-{m}-{d}"

        date_revised = ''
        dr = medline.find('DateRevised')
        if dr is not None:
            y = dr.findtext('Year') or ''
            m = dr.findtext('Month') or '01'
            d = dr.findtext('Day') or '01'
            if y:
                date_revised = f"{y}-{m}-{d}"

        # Convert to JSON for the JSON column (simplified)
        # Note: For full fidelity, you'd use xmltodict, but that's slower
        json_str = json.dumps({
            'PMID': pmid,
            'Title': title,
            'Abstract': abstract,
            'DOI': doi,
            'Year': year,
            'Authors': authors_str,
            'Journal': journal,
            'MeSH_Terms': mesh_str,
            'Keywords': keywords_str,
            'Pub_Types': pub_types_str,
        })

        return {
            'PMID': pmid,
            'Title': title,
            'Abstract': abstract,
            'DOI': doi,
            'Year': year,
            'Authors': authors_str,
            'Journal': journal,
            'MeSH_Terms': mesh_str,
            'Keywords': keywords_str,
            'Pub_Types': pub_types_str,
            'Date_Created': date_created,
            'Date_Revised': date_revised,
            'JSON': json_str,
        }
    except Exception as e:
        print(f"Error extracting article: {e}")
        return None


def process_xml_file(xml_gz_path: Path, output_dir: Path) -> dict:
    """Process a single gzipped XML file and write partitioned parquet output."""
    records_by_year = {}
    total_records = 0

    try:
        with gzip.open(xml_gz_path, 'rb') as f:
            # Use iterparse for memory efficiency
            context = ET.iterparse(f, events=('end',))
            for event, elem in context:
                if elem.tag == 'PubmedArticle':
                    record = extract_article_fields(elem)
                    if record:
                        year = record['Year'] or 0
                        if year not in records_by_year:
                            records_by_year[year] = []
                        records_by_year[year].append(record)
                        total_records += 1
                    elem.clear()
    except Exception as e:
        return {'error': str(e), 'file': str(xml_gz_path), 'records': 0}

    # Write partitioned output
    stats = {'file': str(xml_gz_path), 'records': total_records, 'years': {}}

    for year, records in records_by_year.items():
        year_dir = output_dir / f'year={year}'
        year_dir.mkdir(parents=True, exist_ok=True)

        # Output filename based on input
        out_name = xml_gz_path.stem.replace('.xml', '') + '.parquet'
        out_path = year_dir / out_name

        # Build table
        arrays = {col: [] for col in SCHEMA.names}
        for rec in records:
            for col in SCHEMA.names:
                arrays[col].append(rec.get(col))

        table = pa.table(arrays, schema=SCHEMA)
        pq.write_table(table, out_path, compression='snappy')

        stats['years'][year] = len(records)

    return stats


def download_and_process_file(args: tuple) -> dict:
    """Download and process a single baseline file."""
    filename, download_dir, output_dir, keep_xml = args

    xml_path = download_dir / filename
    url = BASELINE_URL + filename

    # Download if not exists
    if not xml_path.exists():
        if not download_file(url, xml_path):
            return {'error': f'Download failed for {filename}', 'records': 0}

    # Process
    stats = process_xml_file(xml_path, output_dir)

    # Clean up XML if not keeping
    if not keep_xml and xml_path.exists():
        xml_path.unlink()

    return stats


def main():
    parser = argparse.ArgumentParser(description='Download and process PubMed baseline')
    parser.add_argument('--download-dir', default='download/baseline',
                        help='Directory for downloaded XML files')
    parser.add_argument('--output-dir', default='brick_v2/pubmed.parquet',
                        help='Output directory for parquet files')
    parser.add_argument('--workers', type=int, default=4,
                        help='Number of parallel workers (default: 4)')
    parser.add_argument('--keep-xml', action='store_true',
                        help='Keep XML files after processing')
    parser.add_argument('--start-from', type=int, default=1,
                        help='Start from file number N (for resuming)')
    args = parser.parse_args()

    download_dir = Path(args.download_dir)
    output_dir = Path(args.output_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get file list
    files = get_baseline_file_list()

    # Filter to start from specified file
    if args.start_from > 1:
        files = [f for f in files if int(re.search(r'n(\d+)', f).group(1)) >= args.start_from]
        print(f"Starting from file {args.start_from}, {len(files)} files remaining")

    # Process files
    total_records = 0
    year_totals = {}

    # Use fewer workers for download-heavy task
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        # Create task arguments
        tasks = [(f, download_dir, output_dir, args.keep_xml) for f in files]

        futures = {executor.submit(download_and_process_file, task): task[0] for task in tasks}

        for i, future in enumerate(as_completed(futures), 1):
            filename = futures[future]
            try:
                stats = future.result()
                if 'error' in stats:
                    print(f"[{i}/{len(files)}] ERROR {filename}: {stats['error']}")
                else:
                    total_records += stats['records']
                    for year, count in stats.get('years', {}).items():
                        year_totals[year] = year_totals.get(year, 0) + count
                    print(f"[{i}/{len(files)}] Processed {filename}: {stats['records']:,} records")
            except Exception as e:
                print(f"[{i}/{len(files)}] EXCEPTION {filename}: {e}")

    print(f"\n=== Complete ===")
    print(f"Total records: {total_records:,}")
    print(f"Year distribution (sample):")
    for year in sorted(year_totals.keys())[-10:]:
        print(f"  {year}: {year_totals[year]:,}")

    # Write metadata
    metadata = {
        'total_records': total_records,
        'year_distribution': {str(k): v for k, v in year_totals.items()},
        'schema_version': '2.0',
        'source': 'pubmed25 baseline',
        'baseline_date': '2025-01-08',
    }
    with open(output_dir.parent / 'metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2)

    print(f"\nMetadata written to {output_dir.parent / 'metadata.json'}")


if __name__ == '__main__':
    main()
