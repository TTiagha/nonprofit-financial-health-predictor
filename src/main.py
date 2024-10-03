# main.py

import os
import time
import logging
from datetime import datetime
import subprocess
from collections import Counter
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO
import pandas as pd
import json
from openai import OpenAI
import requests

from xml_downloader import download_and_extract_xml_files
from data_processor import process_xml_files
from data_analyzer import analyze_data
from s3_utils import upload_file_to_s3, download_file_from_s3, get_s3_client
from config import S3_BUCKET, S3_FOLDER, desired_fields

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# OpenAI setup
api_key = os.environ.get('OPENAI_API_KEY')
if not api_key:
    raise ValueError("OPENAI_API_KEY environment variable is not set")

client = OpenAI(api_key=api_key)
model = 'gpt-4o-mini'

# Global counters for API calls
successful_api_calls = 0
unsuccessful_api_calls = 0
openai_extractions = 0

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
        "https://donationtransparency.org/wp-content/uploads/2024/10/Test.zip",
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
}

def get_ntee_code_from_api(ein):
    global successful_api_calls, unsuccessful_api_calls
    url = f"https://projects.propublica.org/nonprofits/api/v2/organizations/{ein}.json"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        ntee_code = data['organization'].get('ntee_code')
        if ntee_code:
            successful_api_calls += 1
            return ntee_code
        else:
            logger.warning(f"API response for EIN {ein} is missing NTEE code")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching NTEE code from API for EIN {ein}: {str(e)}")
    except (KeyError, ValueError) as e:
        logger.error(f"Error parsing API response for EIN {ein}: {str(e)}")
    unsuccessful_api_calls += 1
    return None

def get_ntee_description(ntee_code, organization_name, mission_statement):
    global openai_extractions
    prompt = f"""Given the NTEE (National Taxonomy of Exempt Entities) code and information about a nonprofit organization, provide the NTEE description.

NTEE Code: {ntee_code}
Organization Name: {organization_name}
Mission Statement: {mission_statement if mission_statement else "Not provided"}

Based on this information, especially the NTEE code, provide the official NTEE description

Output your response in JSON format, including both the NTEE code and its description. Use the following structure:

{{
  "ntee_code": "{ntee_code}",
  "ntee_description": "Description of the NTEE category"
}}
"""

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are an expert in nonprofit organizations and NTEE codes."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.0
        )
        result = json.loads(response.choices[0].message.content)
        openai_extractions += 1
        return result
    except Exception as e:
        logger.error(f"Error inferring NTEE description: {str(e)}")
        return {"ntee_code": ntee_code, "ntee_description": "Error in inference"}

def get_ntee_code_description(organization_name, mission_statement, ein):
    ntee_code = get_ntee_code_from_api(ein)
    if ntee_code:
        return get_ntee_description(ntee_code, organization_name, mission_statement)
    else:
        # If API fails to provide NTEE code, use OpenAI to infer both code and description
        prompt = f"""Determine the National Taxonomy of Exempt Entities (NTEE) code and description for a nonprofit organization based on the following information:

Organization Name: {organization_name}
Mission Statement: {mission_statement if mission_statement else "Not provided"}
EIN: {ein}

Analyze the organization's name and mission statement (if provided). Consider the main focus area of the organization (e.g., education, health, environment) and identify the specific activities or services the organization provides. Match these characteristics to the most appropriate NTEE category and subcategory.

Provide your response in JSON format using the following structure:

{{
  "ntee_code": "X##",
  "ntee_description": "Brief description of the NTEE category"
}}

Ensure that the NTEE code follows the format of a letter followed by two digits (e.g., A31, B24, C50)."""

        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are an expert in nonprofit organizations and NTEE codes."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.0
            )
            result = json.loads(response.choices[0].message.content)
            openai_extractions += 1
            return result
        except Exception as e:
            logger.error(f"Error inferring NTEE code and description: {str(e)}")
            return {"ntee_code": "Unknown", "ntee_description": "Error in inference"}

def upload_xml_content_to_s3(xml_content, s3_key):
    try:
        file_size = len(xml_content)
        logger.info(f"Attempting to upload XML content (Size: {file_size} bytes)")
        upload_file_to_s3(xml_content, s3_key)
        logger.info(f"Successfully uploaded XML content to S3: {s3_key}")
    except Exception as e:
        logger.error(f"Error uploading XML content to S3: {str(e)}")

def run_new990_check():
    logger.info("Running new990.py to check for updates...")
    try:
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        new990_path = os.path.join(current_dir, "src", "new990.py")
        
        logger.info(f"Attempting to run: {new990_path}")
        
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
    new_df = pd.DataFrame(records)

    s3_key = f'{S3_FOLDER}/irs990_data.parquet'

    s3_client = boto3.client('s3')
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
        file_exists = True
    except:
        file_exists = False

    if file_exists:
        logger.info('Existing Parquet file found. Downloading and merging data.')
        existing_data = download_file_from_s3(s3_key)
        existing_df = pd.read_parquet(BytesIO(existing_data))
        existing_df['EIN'] = existing_df['EIN'].astype(str)

        new_df['EIN'] = new_df['EIN'].astype(str)

        merged_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        merged_df.drop_duplicates(subset=['EIN', 'TaxYear'], keep='last', inplace=True)
        
        logger.info(f'Merged {len(new_df)} new or updated records with {len(existing_df)} existing records.')
        logger.info(f'After deduplication, total records: {len(merged_df)}')
    else:
        logger.info('No existing Parquet file found. Creating new file.')
        merged_df = new_df

    merged_df['EIN'] = merged_df['EIN'].astype(str)

    # Convert DataFrame to PyArrow Table
    try:
        merged_table = pa.Table.from_pandas(merged_df)
    except pa.lib.ArrowInvalid as e:
        logger.error(f"Error converting DataFrame to PyArrow Table: {str(e)}")
        logger.info("Attempting to identify problematic columns...")
        for column in merged_df.columns:
            try:
                pa.array(merged_df[column])
            except pa.lib.ArrowInvalid as col_error:
                logger.error(f"Error in column '{column}': {str(col_error)}")
                logger.info(f"Sample data for '{column}': {merged_df[column].head()}")
        return

    local_parquet_file = 'temp_irs990_data.parquet'
    pq.write_table(merged_table, local_parquet_file)

    with open(local_parquet_file, 'rb') as f:
        upload_file_to_s3(f.read(), s3_key)
    
    logger.info(f'Successfully uploaded merged data to S3: {s3_key}')
    
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
                try:
                    indices = [int(i.strip()) - 1 for i in selections.split(',')]
                    for i in indices:
                        if 0 <= i < len(AVAILABLE_URLS[year]):
                            selected_urls.append(AVAILABLE_URLS[year][i])
                            print(f"[X] {i+1}. {AVAILABLE_URLS[year][i]}")
                        else:
                            print(f"Invalid selection: {i+1}")
                except ValueError:
                    print("Invalid input. Please enter numbers separated by commas or 'all'.")
        else:
            print(f"Invalid year: {year}")
    
    return state, selected_urls

def main():
    global successful_api_calls, unsuccessful_api_calls, openai_extractions
    logger.info(f"Starting Nonprofit Financial Health Predictor at {datetime.now()}")

    try:
        run_new990_check()

        state_filter, urls = get_user_input()
        logger.info(f"User selected state filter: {state_filter}")
        logger.info(f"User selected {len(urls)} URLs to process")
        
        all_records = []
        total_files_processed = 0
        start_time = time.time()
        
        files_without_total_assets = {}
        
        for url in urls:
            logger.info(f"Processing URL: {url}")
            xml_files = download_and_extract_xml_files(url)
            logger.info(f"Downloaded and extracted {len(xml_files)} XML files from {url}")
            
            if not xml_files:
                logger.warning(f"No XML files were extracted from {url}")
                continue
            
            records, no_total_assets_files = process_xml_files(xml_files, state_filter, get_ntee_code_description)
            logger.info(f"Processed {len(records)} records from {url}")
            logger.info(f"Found {len(no_total_assets_files)} files without TotalAssets from {url}")
            
            all_records.extend(records)
            files_without_total_assets.update(no_total_assets_files)
            total_files_processed += len(xml_files)
            
            logger.info(f"Files processed from this URL: {len(xml_files)}")
            logger.info(f"Total records processed so far: {len(all_records)}")

        end_time = time.time()
        processing_time = end_time - start_time
        logger.info(f"Processed {len(all_records)} {state_filter} nonprofit records from {total_files_processed} files in {processing_time:.2f} seconds")
    
        if not all_records:
            logger.warning("No records were processed. This could be due to no matching records for the selected state or issues with data extraction.")
        
        logger.info(f"Uploading files without TotalAssets to S3 (max 20 files)")
        logger.info(f"Total files without TotalAssets: {len(files_without_total_assets)}")
        for i, (file_name, xml_content) in enumerate(files_without_total_assets.items()):
            s3_key = f"{S3_FOLDER}/NoTotalAssets/{file_name}"
            logger.info(f"Attempting to upload file {i+1}: {file_name}")
            upload_xml_content_to_s3(xml_content, s3_key)
            if i == 19:
                break

        form_types = [r['FormType'] for r in all_records]
        logger.info(f"Form type distribution: {dict(Counter(form_types))}")

        # Use the new analyze_data function
        analyze_data(all_records)

        save_to_s3_parquet(all_records)

        # Log summary of API calls and extractions
        total_api_calls = successful_api_calls + unsuccessful_api_calls
        logger.info(f"Summary of API calls and extractions:")
        logger.info(f"Total Nonprofit Explorer API calls: {total_api_calls}")
        logger.info(f"Successful Nonprofit Explorer API calls: {successful_api_calls}")
        logger.info(f"Unsuccessful Nonprofit Explorer API calls: {unsuccessful_api_calls}")
        logger.info(f"NTEE codes extracted via OpenAI API: {openai_extractions}")

        if total_api_calls > 0:
            success_rate = (successful_api_calls / total_api_calls) * 100
            logger.info(f"Nonprofit Explorer API success rate: {success_rate:.2f}%")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        logger.exception("Exception details:")

if __name__ == '__main__':
    main()