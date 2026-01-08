#!/usr/bin/env python3
"""
Reprocess existing PubMed parquet files to extract searchable columns.

This reads the existing {PMID, JSON} parquet files and creates new
partitioned parquet files with extracted columns for fast querying.

No download required - uses existing data.

Usage:
    python stages_v2/reprocess_parquet.py [--workers 8] [--output-dir brick_v2/pubmed.parquet]
"""

import argparse
import json
import os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Optional
import pyarrow as pa
import pyarrow.parquet as pq

# New schema with extracted columns
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


def safe_get(d: dict, *keys, default=''):
    """Safely navigate nested dict."""
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key, {})
        else:
            return default
    return d if d and d != {} else default


def extract_text(obj) -> str:
    """Extract text from various JSON structures."""
    if obj is None:
        return ''
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        return obj.get('#text', str(obj))
    if isinstance(obj, list):
        return ' '.join(extract_text(item) for item in obj)
    return str(obj)


def extract_fields(pmid: str, json_str: str) -> dict:
    """Extract searchable fields from JSON blob."""
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        return {
            'PMID': int(pmid) if pmid.isdigit() else 0,
            'Title': '',
            'Abstract': '',
            'DOI': '',
            'Year': None,
            'Authors': '',
            'Journal': '',
            'MeSH_Terms': '',
            'Keywords': '',
            'Pub_Types': '',
            'Date_Created': '',
            'Date_Revised': '',
            'JSON': json_str,
        }

    medline = data.get('MedlineCitation', {})
    article = medline.get('Article', {})
    pubmed_data = data.get('PubmedData', {})

    # PMID
    pmid_val = safe_get(medline, 'PMID', '#text') or pmid
    pmid_int = int(pmid_val) if str(pmid_val).isdigit() else 0

    # Title
    title = extract_text(article.get('ArticleTitle', ''))

    # Abstract - handle structured abstracts
    abstract_parts = []
    abstract_data = safe_get(article, 'Abstract', 'AbstractText')
    if abstract_data:
        if isinstance(abstract_data, list):
            for item in abstract_data:
                if isinstance(item, dict):
                    label = item.get('@Label', '')
                    text = item.get('#text', str(item))
                    if label:
                        abstract_parts.append(f"{label}: {text}")
                    else:
                        abstract_parts.append(text)
                else:
                    abstract_parts.append(str(item))
        elif isinstance(abstract_data, dict):
            abstract_parts.append(abstract_data.get('#text', str(abstract_data)))
        else:
            abstract_parts.append(str(abstract_data))
    abstract = ' '.join(abstract_parts)

    # DOI
    doi = ''
    article_ids = safe_get(pubmed_data, 'ArticleIdList', 'ArticleId')
    if article_ids:
        if isinstance(article_ids, list):
            for aid in article_ids:
                if isinstance(aid, dict) and aid.get('@IdType') == 'doi':
                    doi = aid.get('#text', '')
                    break
        elif isinstance(article_ids, dict) and article_ids.get('@IdType') == 'doi':
            doi = article_ids.get('#text', '')

    # Year
    year = None
    pub_date = safe_get(article, 'Journal', 'JournalIssue', 'PubDate')
    if isinstance(pub_date, dict):
        year_str = pub_date.get('Year', '')
        if year_str and str(year_str).isdigit():
            year = int(year_str)
    # Fallback to MedlineDate
    if not year and isinstance(pub_date, dict):
        medline_date = pub_date.get('MedlineDate', '')
        if medline_date and len(medline_date) >= 4:
            year_str = medline_date[:4]
            if year_str.isdigit():
                year = int(year_str)

    # Authors
    authors = []
    author_list = safe_get(article, 'AuthorList', 'Author')
    if author_list:
        if isinstance(author_list, list):
            for author in author_list:
                if isinstance(author, dict):
                    last = author.get('LastName', '')
                    first = author.get('ForeName', '')
                    if last:
                        authors.append(f"{last}, {first}".strip(', '))
        elif isinstance(author_list, dict):
            last = author_list.get('LastName', '')
            first = author_list.get('ForeName', '')
            if last:
                authors.append(f"{last}, {first}".strip(', '))
    authors_str = '; '.join(authors)

    # Journal
    journal = extract_text(safe_get(article, 'Journal', 'Title'))

    # MeSH Terms
    mesh_terms = []
    mesh_list = safe_get(medline, 'MeshHeadingList', 'MeshHeading')
    if mesh_list:
        if isinstance(mesh_list, list):
            for mesh in mesh_list:
                if isinstance(mesh, dict):
                    desc = mesh.get('DescriptorName', {})
                    if isinstance(desc, dict):
                        mesh_terms.append(desc.get('#text', ''))
                    elif isinstance(desc, str):
                        mesh_terms.append(desc)
        elif isinstance(mesh_list, dict):
            desc = mesh_list.get('DescriptorName', {})
            if isinstance(desc, dict):
                mesh_terms.append(desc.get('#text', ''))
    mesh_str = '; '.join(mesh_terms)

    # Keywords
    keywords = []
    kw_list = safe_get(medline, 'KeywordList', 'Keyword')
    if kw_list:
        if isinstance(kw_list, list):
            for kw in kw_list:
                if isinstance(kw, dict):
                    keywords.append(kw.get('#text', ''))
                elif isinstance(kw, str):
                    keywords.append(kw)
        elif isinstance(kw_list, dict):
            keywords.append(kw_list.get('#text', ''))
        elif isinstance(kw_list, str):
            keywords.append(kw_list)
    keywords_str = '; '.join(keywords)

    # Publication Types
    pub_types = []
    pt_list = safe_get(article, 'PublicationTypeList', 'PublicationType')
    if pt_list:
        if isinstance(pt_list, list):
            for pt in pt_list:
                if isinstance(pt, dict):
                    pub_types.append(pt.get('#text', ''))
                elif isinstance(pt, str):
                    pub_types.append(pt)
        elif isinstance(pt_list, dict):
            pub_types.append(pt_list.get('#text', ''))
    pub_types_str = '; '.join(pub_types)

    # Dates
    date_created = ''
    dc = medline.get('DateCreated', {})
    if isinstance(dc, dict) and dc.get('Year'):
        date_created = f"{dc.get('Year', '')}-{dc.get('Month', '01')}-{dc.get('Day', '01')}"

    date_revised = ''
    dr = medline.get('DateRevised', {})
    if isinstance(dr, dict) and dr.get('Year'):
        date_revised = f"{dr.get('Year', '')}-{dr.get('Month', '01')}-{dr.get('Day', '01')}"

    return {
        'PMID': pmid_int,
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


def process_parquet_file(input_path: str, output_dir: str) -> dict:
    """Process a single parquet file and write partitioned output."""
    input_path = Path(input_path)
    output_dir = Path(output_dir)

    # Read existing parquet
    table = pq.read_table(input_path)
    pmids = table.column('PMID').to_pylist()
    jsons = table.column('JSON').to_pylist()

    # Extract fields for each record
    records_by_year = {}
    for pmid, json_str in zip(pmids, jsons):
        record = extract_fields(str(pmid), json_str)
        year = record['Year'] or 0
        if year not in records_by_year:
            records_by_year[year] = []
        records_by_year[year].append(record)

    # Write partitioned output
    stats = {'input_file': str(input_path), 'records': len(pmids), 'years': {}}

    for year, records in records_by_year.items():
        year_dir = output_dir / f'year={year}'
        year_dir.mkdir(parents=True, exist_ok=True)

        # Create output filename based on input
        out_name = input_path.stem + '.parquet'
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


def main():
    parser = argparse.ArgumentParser(description='Reprocess PubMed parquet files')
    parser.add_argument('--input-dir', default='brick/pubmed.parquet',
                        help='Input directory with existing parquet files')
    parser.add_argument('--output-dir', default='brick_v2/pubmed.parquet',
                        help='Output directory for new partitioned parquet')
    parser.add_argument('--workers', type=int, default=8,
                        help='Number of parallel workers')
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all input parquet files
    input_files = sorted(input_dir.glob('*.parquet'))
    print(f"Found {len(input_files)} parquet files to process")

    # Process in parallel
    total_records = 0
    year_totals = {}

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_parquet_file, str(f), str(output_dir)): f
            for f in input_files
        }

        for i, future in enumerate(as_completed(futures), 1):
            input_file = futures[future]
            try:
                stats = future.result()
                total_records += stats['records']
                for year, count in stats['years'].items():
                    year_totals[year] = year_totals.get(year, 0) + count
                print(f"[{i}/{len(input_files)}] Processed {input_file.name}: {stats['records']} records")
            except Exception as e:
                print(f"[{i}/{len(input_files)}] ERROR processing {input_file.name}: {e}")

    print(f"\n=== Complete ===")
    print(f"Total records: {total_records:,}")
    print(f"Year distribution:")
    for year in sorted(year_totals.keys()):
        print(f"  {year}: {year_totals[year]:,}")

    # Write metadata
    import json as json_module
    metadata = {
        'total_records': total_records,
        'year_distribution': year_totals,
        'schema_version': '2.0',
        'source': 'reprocessed from v1 parquet',
    }
    with open(output_dir.parent / 'metadata.json', 'w') as f:
        json_module.dump(metadata, f, indent=2, default=str)

    print(f"\nMetadata written to {output_dir.parent / 'metadata.json'}")


if __name__ == '__main__':
    main()
