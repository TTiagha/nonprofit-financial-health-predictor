import boto3
import botocore
import requests
import zipfile
import io
import os
import logging
from logging.handlers import RotatingFileHandler
from lxml import etree
import pyarrow as pa
import pyarrow.parquet as pq
from collections import Counter
import time
import xml.etree.ElementTree as ET

# Configure logging
log_filename = 'health.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        RotatingFileHandler(log_filename, maxBytes=10000000, backupCount=5),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

# ... [Keep all the existing code] ...

def download_and_extract_xml_files(url):
    logger.info(f'Downloading zip file from {url}')
    response = requests.get(url)
    response.raise_for_status()
    logger.info('Download complete.')

    logger.info('Extracting all XML files from zip archive.')
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    xml_files = {}
    for filename in zip_file.namelist():
        if filename.endswith('.xml'):
            with zip_file.open(filename) as file:
                xml_files[filename] = file.read()
                logger.info(f'Extracted {filename}')
    logger.info('Extraction complete.')
    return xml_files

def main():
    # Get user input for state
    state = input("Enter the state abbreviation to filter (e.g., GA): ").upper()
    
    # Get user input for URLs
    urls_input = input("Enter the URL(s) to process (separate multiple URLs with commas): ")
    urls = [url.strip() for url in urls_input.split(',')]
    
    all_records = []
    
    for url in urls:
        logger.info(f"Processing URL: {url}")
        xml_files = download_and_extract_xml_files(url)
        records = process_xml_files(xml_files, state)
        all_records.extend(records)
        
        logger.info(f"Total files processed from {url}: {len(xml_files)}")
        logger.info(f"Number of files containing {state} nonprofits: {len(set(r.get('_source_file', '') for r in records))}")
    
    # Log overall statistics
    form_types = [r['FormType'] for r in all_records]
    logger.info(f"Form type distribution: {dict(Counter(form_types))}")
    
    # ... [Keep all the existing logging code] ...

    save_to_s3_parquet(all_records, s3_bucket, s3_folder)
    analyze_field_coverage(all_records)
    analyze_path_usage(all_records)

if __name__ == '__main__':
    main()
