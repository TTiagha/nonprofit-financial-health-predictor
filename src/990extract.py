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

# ... [Keep all the existing code until the is_georgia_nonprofit function] ...

def is_nonprofit_in_state(data, state):
    return data.get('State') == state

# ... [Keep all the existing code until the process_xml_files function] ...

def process_xml_files(xml_files, state):
    records = []
    state_files = set()
    no_revenue_files = set()
    no_exp_files = set()
    no_ass_files = set()
    no_nass_files = set()
    start_time = time.time()

    for filename, xml_content in xml_files.items():
        file_start_time = time.time()
        logger.info(f'Processing {filename}')
        try:
            tree = etree.fromstring(xml_content)
            ns = tree.nsmap.copy()
            irs_namespace = ns.get('ns0') or ns.get(None)
            if irs_namespace:
                ns = {'irs': irs_namespace}
            else:
                logger.warning(f'IRS namespace not found in {filename}')
                ns = {}

            Returns = tree.xpath('//irs:Return', namespaces=ns)
            if not Returns:
                logger.warning(f'No Return elements found in {filename}')
                continue

            file_missing_revenue = False
            file_missing_expenses = False
            file_missing_assets = False
            file_missing_net_assets = False

            for i, Return in enumerate(Returns):
                logger.info(f'Processing Return {i+1} in {filename}')
                try:
                    data = parse_return(Return, ns, filename)
                    if data and is_nonprofit_in_state(data, state):
                        records.append(data)
                        state_files.add(filename)
                        logger.info(f"{state} nonprofit found in {filename}, Return {i+1}")

                        if data['FormType'] != '990T' and 'TotalRevenue' not in data:
                            logger.warning(f"Missing 'TotalRevenue' in {filename}, Return {i+1}, FormType: {data['FormType']}")
                            file_missing_revenue = True

                        if 'TotalExpenses' not in data:
                            logger.warning(f"Missing 'TotalExpenses' in {filename}, Return {i+1}, FormType: {data['FormType']}")
                            file_missing_expenses = True

                        if 'TotalAssets' not in data:
                            logger.warning(f"Missing 'TotalAssets' in {filename}, Return {i+1}, FormType: {data['FormType']}")
                            file_missing_assets = True

                        if 'TotalNetAssets' not in data:
                            logger.warning(f"Missing 'TotalNetAssets' in {filename}, Return {i+1}, FormType: {data['FormType']}")
                            file_missing_net_assets = True

                except Exception as e:
                    logger.error(f'Error processing Return {i+1} in {filename}: {e}')

            if file_missing_revenue:
                no_revenue_files.add(filename)
                s3_client.put_object(Bucket=s3_bucket, Key=f'{s3_norev_folder}/{filename}', Body=xml_content)
                logger.info(f"Uploaded {filename} to NoRev folder due to missing TotalRevenue")

            if file_missing_expenses:
                no_exp_files.add(filename)
                s3_client.put_object(Bucket=s3_bucket, Key=f'{s3_noexp_folder}/{filename}', Body=xml_content)
                logger.info(f"Uploaded {filename} to NoExp folder due to missing TotalExpenses")

            if file_missing_assets:
                no_ass_files.add(filename)
                s3_client.put_object(Bucket=s3_bucket, Key=f'{s3_noass_folder}/{filename}', Body=xml_content)
                logger.info(f"Uploaded {filename} to NoAss folder due to missing TotalAssets")

            if file_missing_net_assets:
                no_nass_files.add(filename)
                s3_client.put_object(Bucket=s3_bucket, Key=f'{s3_nonass_folder}/{filename}', Body=xml_content)
                logger.info(f"Uploaded {filename} to NoNAss folder due to missing TotalNetAssets")

        except Exception as e:
            logger.error(f'Error processing {filename}: {e}')

        file_end_time = time.time()
        logger.info(f"Processed {filename} in {file_end_time - file_start_time:.2f} seconds")

    end_time = time.time()
    logger.info(f'Processed {len(records)} {state} nonprofit records from {len(xml_files)} files in {end_time - start_time:.2f} seconds')
    logger.info(f'Files without TotalRevenue: {len(no_revenue_files)}')
    logger.info(f'Files without TotalExpenses: {len(no_exp_files)}')
    logger.info(f'Files without TotalAssets: {len(no_ass_files)}')
    logger.info(f'Files without TotalNetAssets: {len(no_nass_files)}')

    if records:
        logger.info(f"Average fields per record: {sum(len(r) for r in records) / len(records):.2f}")
    else:
        logger.warning(f"No valid {state} nonprofit records processed.")

    return records

# ... [Keep all the existing code until the main function] ...

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
    
    # Log TotalNetAssets statistics
    total_net_assets = [r.get('TotalNetAssets') for r in all_records if 'TotalNetAssets' in r]
    logger.info(f"TotalNetAssets: found in {len(total_net_assets)}/{len(all_records)} records")
    valid_net_assets = [x for x in total_net_assets if x is not None and isinstance(x, (int, float))]
    if valid_net_assets:
        logger.info(f"TotalNetAssets: min={min(valid_net_assets)}, max={max(valid_net_assets)}, avg={sum(valid_net_assets)/len(valid_net_assets)}")
    else:
        logger.warning("No valid TotalNetAssets values found")

    # Log MissionStatement statistics
    mission_statements = [r.get('MissionStatement') for r in all_records if 'MissionStatement' in r]
    logger.info(f"MissionStatement: found in {len(mission_statements)}/{len(all_records)} records")
    if mission_statements:
        valid_statements = [ms for ms in mission_statements if ms]
        if valid_statements:
            avg_length = sum(len(ms) for ms in valid_statements) / len(valid_statements)
            logger.info(f"Average MissionStatement length: {avg_length:.2f} characters")
        else:
            logger.warning("No valid MissionStatement values found")

    # Log TotalAssets statistics
    total_assets = [r.get('TotalAssets') for r in all_records if 'TotalAssets' in r]
    logger.info(f"TotalAssets: found in {len(total_assets)}/{len(all_records)} records")
    valid_assets = [x for x in total_assets if x is not None and isinstance(x, (int, float))]
    if valid_assets:
        logger.info(f"TotalAssets: min={min(valid_assets)}, max={max(valid_assets)}, avg={sum(valid_assets)/len(valid_assets)}")
    else:
        logger.warning("No valid TotalAssets values found")
    
    # Log TotalRevenue statistics
    total_revenue = [r.get('TotalRevenue') for r in all_records if 'TotalRevenue' in r]
    logger.info(f"TotalRevenue: found in {len(total_revenue)}/{len(all_records)} records")
    valid_revenue = [x for x in total_revenue if x is not None and isinstance(x, (int, float))]
    if valid_revenue:
        logger.info(f"TotalRevenue: min={min(valid_revenue)}, max={max(valid_revenue)}, avg={sum(valid_revenue)/len(valid_revenue)}")
    else:
        logger.warning("No valid TotalRevenue values found")

    # Log TotalExpenses statistics
    total_expenses = [r.get('TotalExpenses') for r in all_records if 'TotalExpenses' in r]
    logger.info(f"TotalExpenses: found in {len(total_expenses)}/{len(all_records)} records")
    valid_expenses = [x for x in total_expenses if x is not None and isinstance(x, (int, float))]
    if valid_expenses:
        logger.info(f"TotalExpenses: min={min(valid_expenses)}, max={max(valid_expenses)}, avg={sum(valid_expenses)/len(valid_expenses)}")
    else:
        logger.warning("No valid TotalExpenses values found")

    save_to_s3_parquet(all_records, s3_bucket, s3_folder)
    analyze_field_coverage(all_records)
    analyze_path_usage(all_records)

if __name__ == '__main__':
    main()
