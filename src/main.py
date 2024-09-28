# main.py

from logger import logger
from xml_downloader import download_and_extract_xml_files
from data_processor import process_xml_files
from data_analyzer import analyze_field_coverage, analyze_path_usage
from s3_utils import upload_file_to_s3
from config import S3_BUCKET, S3_FOLDER
import pyarrow as pa
import pyarrow.parquet as pq
import os
from collections import Counter

def save_to_s3_parquet(records):
    if not records:
        logger.warning('No valid records to save.')
        return

    logger.info('Converting records to Parquet format.')
    # Convert records to Apache Arrow Table
    table = pa.Table.from_pylist(records)

    # Write Parquet file to local disk
    local_parquet_file = 'irs990_data.parquet'
    pq.write_table(table, local_parquet_file)

    # Upload Parquet file to S3
    s3_key = f'{S3_FOLDER}/irs990_data.parquet'
    with open(local_parquet_file, 'rb') as f:
        upload_file_to_s3(f.read(), s3_key)
    os.remove(local_parquet_file)

def get_user_input():
    state = input("Enter the state abbreviation to filter for (e.g., GA): ").upper()
    urls = input("Enter the URL(s) to process (separate multiple URLs with commas): ").split(',')
    return state, [url.strip() for url in urls]

def main():
    state_filter, urls = get_user_input()
    
    all_records = []
    total_files_processed = 0
    
    for url in urls:
        logger.info(f"Processing URL: {url}")
        xml_files = download_and_extract_xml_files(url)
        records = process_xml_files(xml_files, state_filter)
        all_records.extend(records)
        total_files_processed += len(xml_files)
        
        logger.info(f"Files processed from this URL: {len(xml_files)}")
        logger.info(f"Number of files containing {state_filter} nonprofits: {len(set(r.get('_source_file', '') for r in records))}")

    logger.info(f"Total files processed across all URLs: {total_files_processed}")
    logger.info(f"Total number of {state_filter} nonprofit records: {len(all_records)}")

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
        
    analyze_field_coverage(all_records)
    analyze_path_usage(all_records)
    save_to_s3_parquet(all_records)

if __name__ == '__main__':
    main()
