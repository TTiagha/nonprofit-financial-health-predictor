# main.py

import os
import time
import logging
from datetime import datetime
import subprocess
from collections import Counter
import pyarrow as pa
import pyarrow.parquet as pq

from xml_downloader import download_and_extract_xml_files
from data_processor import process_xml_files
from data_analyzer import analyze_field_coverage, analyze_path_usage
from s3_utils import upload_file_to_s3
from config import S3_BUCKET, S3_FOLDER

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Available URLs for IRS Form 990 data
AVAILABLE_URLS = {
    "2024": [
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_01A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_02A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_03A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_04A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_05A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_05B.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_06A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_07A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_08A.zip",
    ],
    "2023": [
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_01A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_02A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_03A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_04A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_05A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_05B.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_06A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_07A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_08A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_09A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_10A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_11A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_11B.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_11C.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_12A.zip",
    ],
    "2022": [
        "https://apps.irs.gov/pub/epostcard/990/xml/2022/2022_TEOS_XML_01A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2022/2022_TEOS_XML_01B.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2022/2022_TEOS_XML_01C.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2022/2022_TEOS_XML_01D.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2022/2022_TEOS_XML_01E.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2022/2022_TEOS_XML_01F.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2022/2022_TEOS_XML_11A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2022/2022_TEOS_XML_11B.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2022/2022_TEOS_XML_11C.zip",
    ],
    "2021": [
        "https://apps.irs.gov/pub/epostcard/990/xml/2021/2021_TEOS_XML_01A.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2021/2021_TEOS_XML_01B.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2021/2021_TEOS_XML_01C.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2021/2021_TEOS_XML_01D.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2021/2021_TEOS_XML_01E.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2021/2021_TEOS_XML_01F.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2021/2021_TEOS_XML_01G.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2021/2021_TEOS_XML_01H.zip",
    ],
    "2020": [
        "https://apps.irs.gov/pub/epostcard/990/xml/2020/2020_TEOS_XML_CT1.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2020/download990xml_2020_1.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2020/download990xml_2020_2.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2020/download990xml_2020_3.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2020/download990xml_2020_4.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2020/download990xml_2020_5.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2020/download990xml_2020_6.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2020/download990xml_2020_7.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2020/download990xml_2020_8.zip",
    ],
    "2019": [
        "https://apps.irs.gov/pub/epostcard/990/xml/2019/2019_TEOS_XML_CT1.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2019/download990xml_2019_1.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2019/download990xml_2019_2.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2019/download990xml_2019_3.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2019/download990xml_2019_4.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2019/download990xml_2019_5.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2019/download990xml_2019_6.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2019/download990xml_2019_7.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2019/download990xml_2019_8.zip",
    ],
    "2018": [
        "https://apps.irs.gov/pub/epostcard/990/xml/2018/2018_TEOS_XML_CT1.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2018/2018_TEOS_XML_CT2.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2018/2018_TEOS_XML_CT3.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2018/download990xml_2018_1.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2018/download990xml_2018_2.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2018/download990xml_2018_3.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2018/download990xml_2018_4.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2018/download990xml_2018_5.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2018/download990xml_2018_6.zip",
        "https://apps.irs.gov/pub/epostcard/990/xml/2018/download990xml_2018_7.zip",
    ]
    # Add other years as needed...
}

def run_new990_check():
    logger.info("Running new990.py to check for updates...")
    try:
        # Use the parent directory of the current file to construct the path to new990.py
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        new990_path = os.path.join(current_dir, "src", "new990.py")
        
        # Log the path being used
        logger.info(f"Attempting to run: {new990_path}")
        
        # Use subprocess.run with capture_output=True to prevent blocking
        result = subprocess.run(f"python {new990_path}", shell=True, check=False, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("Finished checking for updates.")
            logger.info(f"Output: {result.stdout}")
        else:
            logger.error(f"Error running new990.py: {result.stderr}")
    except Exception as e:
        logger.error(f"An error occurred while running new990.py: {str(e)}")
    
    logger.info("Continuing with the rest of the script...")

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
    
    print("\nAvailable years:")
    for year in AVAILABLE_URLS.keys():
        print(year)
    
    selected_years = input("Enter the year(s) you want to process (comma-separated, e.g., 2024,2023): ").split(',')
    selected_years = [year.strip() for year in selected_years]
    
    selected_urls = []
    for year in selected_years:
        if year in AVAILABLE_URLS:
            print(f"\nAvailable URLs for {year}:")
            for i, url in enumerate(AVAILABLE_URLS[year], 1):
                print(f"[ ] {i}. {url}")
            
            selections = input(f"Enter the number(s) of the URL(s) you want to process for {year} (comma-separated, or 'all'): ")
            if selections.lower() == 'all':
                selected_urls.extend(AVAILABLE_URLS[year])
                print("Selected all URLs for", year)
            else:
                indices = [int(i.strip()) - 1 for i in selections.split(',')]
                for i in indices:
                    if 0 <= i < len(AVAILABLE_URLS[year]):
                        selected_urls.append(AVAILABLE_URLS[year][i])
                        print(f"[X] {i+1}. {AVAILABLE_URLS[year][i]}")
                    else:
                        print(f"Invalid selection: {i+1}")
        else:
            print(f"Invalid year: {year}")
    
    return state, selected_urls

def main():
    logger.info(f"Starting Nonprofit Financial Health Predictor at {datetime.now()}")

    try:
        run_new990_check()

        state_filter, urls = get_user_input()
        
        all_records = []
        total_files_processed = 0
        start_time = time.time()
        
        for url in urls:
            logger.info(f"Processing URL: {url}")
            xml_files = download_and_extract_xml_files(url)
            records = process_xml_files(xml_files, state_filter)
            all_records.extend(records)
            total_files_processed += len(xml_files)
            
            logger.info(f"Files processed from this URL: {len(xml_files)}")
    
        end_time = time.time()
        processing_time = end_time - start_time
        logger.info(f"Processed {len(all_records)} {state_filter} nonprofit records from {total_files_processed} files in {processing_time:.2f} seconds")
    
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
    
        # Log files without specific data
        logger.info(f"Files without TotalRevenue: {len(all_records) - len(total_revenue)}")
        logger.info(f"Files without TotalExpenses: {len(all_records) - len(total_expenses)}")
        logger.info(f"Files without TotalAssets: {len(all_records) - len(total_assets)}")
        logger.info(f"Files without TotalNetAssets: {len(all_records) - len(total_net_assets)}")
    
        # Log average fields per record
        avg_fields = sum(len(r) for r in all_records) / len(all_records)
        logger.info(f"Average fields per record: {avg_fields:.2f}")
            
        analyze_field_coverage(all_records)
        analyze_path_usage(all_records)
        save_to_s3_parquet(all_records)

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == '__main__':
    main()
