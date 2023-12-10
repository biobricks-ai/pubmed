import gzip
import xmltodict
import json
import pandas as pd
import os
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

def unzip_and_process(gz_file):
    with gzip.open(gz_file, 'rb') as f:
        xml_content = f.read()
    articles = xmltodict.parse(xml_content)['PubmedArticleSet']['PubmedArticle']
    
    article_data = []
    for article in articles:
        pmid = article.get('MedlineCitation', {}).get('PMID', {}).get('#text', '')
        article_json = json.dumps(article)
        article_data.append({'PMID': pmid, 'JSON': article_json})
    
    return pd.DataFrame(article_data, columns=['PMID', 'JSON'])

def process_file(gz_file):
    df = unzip_and_process(gz_file)
    outpath = 'brick/pubmed.parquet/' + gz_file.split('/')[-1].replace('.xml.gz', '.parquet')
    df.to_parquet(outpath, index=False)

dl_path = 'download/ftp.ncbi.nlm.nih.gov/pubmed/baseline/'
gz_files = [dl_path + f for f in os.listdir(dl_path) if f.endswith('.gz')]

os.makedirs('brick/pubmed.parquet', exist_ok=True)

# Process files in parallel using ThreadPoolExecutor
with ProcessPoolExecutor(max_workers=15) as executor:
    list(tqdm(executor.map(process_file, gz_files), total=len(gz_files)))
