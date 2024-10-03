# main.py

# Note: This script requires openai package version 1.0.0 or later
# If you encounter any issues, please ensure you have the latest version installed

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
import requests
import csv
from openai import OpenAI
from dotenv import load_dotenv

from available_urls import AVAILABLE_URLS

from xml_downloader import download_and_extract_xml_files
from data_processor import process_xml_files
from data_analyzer import analyze_data
from s3_utils import upload_file_to_s3, download_file_from_s3, get_s3_client
from config import S3_BUCKET, S3_FOLDER, desired_fields

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Set up OpenAI API
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Global counters for API calls and NTEE code determination
successful_api_calls = 0
unsuccessful_api_calls = 0
openai_ntee_determinations = 0
no_ntee_code_found = 0

# List to store OpenAI inference attempts
openai_inference_attempts = []

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
            time.sleep(0.5)  # Add delay to avoid throttling
            return ntee_code
        else:
            logger.warning(f"API response for EIN {ein} is missing NTEE code")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching NTEE code from API for EIN {ein}: {str(e)}")
    except (KeyError, ValueError) as e:
        logger.error(f"Error parsing API response for EIN {ein}: {str(e)}")
    unsuccessful_api_calls += 1
    return None

def get_ntee_description_from_csv(ntee_code):
    csv_path = os.path.join(os.path.dirname(__file__), 'ntee_library.csv')
    try:
        with open(csv_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['NTEE Code'] == ntee_code:
                    return row['Description']
    except Exception as e:
        logger.error(f"Error reading NTEE description from CSV for code {ntee_code}: {str(e)}")
    return "Description not found"

def infer_ntee_code_with_gpt4(organization_name, mission_statement):
    prompt = f"""
    Given the following information about a nonprofit organization, infer the most appropriate NTEE (National Taxonomy of Exempt Entities) code. The NTEE code should be in the format of a letter followed by two digits (e.g., A01, B03, C30).

    Organization Name: {organization_name}
    Mission Statement: {mission_statement}

    Provide your response in the following JSON format:
    {{
        "ntee_code": "X00",
        "confidence": 0.0
    }}

    Where "ntee_code" is your inferred NTEE code, and "confidence" is a number between 0 and 1 indicating your confidence in this inference.
    """

    inference_attempt = {
        "organization": organization_name,
        "mission": mission_statement,
        "response": None,
        "error": None
    }

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are an AI assistant tasked with inferring NTEE codes for nonprofit organizations based on their name and mission statement."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=150
        )
        
        # Log the raw response content
        logger.debug(f"Raw GPT-4 response: {response.choices[0].message.content}")
        
        try:
            result = json.loads(response.choices[0].message.content)
            inference_attempt["response"] = result
            openai_inference_attempts.append(inference_attempt)
            return result["ntee_code"], result["confidence"]
        except json.JSONDecodeError as json_error:
            logger.error(f"Error parsing JSON from GPT-4 response: {str(json_error)}")
            logger.error(f"Response content: {response.choices[0].message.content}")
            inference_attempt["error"] = f"JSON parsing error: {str(json_error)}"
            openai_inference_attempts.append(inference_attempt)
            return None, 0.0
    except Exception as e:
        logger.error(f"Error inferring NTEE code with GPT-4: {str(e)}")
        inference_attempt["error"] = str(e)
        openai_inference_attempts.append(inference_attempt)
        return None, 0.0

def get_ntee_code_description(organization_name, mission_statement, ein):
    global openai_ntee_determinations, no_ntee_code_found
    ntee_code = get_ntee_code_from_api(ein)
    if ntee_code:
        ntee_description = get_ntee_description_from_csv(ntee_code)
        return {"ntee_code": ntee_code, "ntee_description": ntee_description}
    else:
        logger.warning(f"Failed to get NTEE code for EIN {ein} from API. Attempting to infer with GPT-4.")
        inferred_ntee_code, confidence = infer_ntee_code_with_gpt4(organization_name, mission_statement)
        if inferred_ntee_code:
            ntee_description = get_ntee_description_from_csv(inferred_ntee_code)
            logger.info(f"Inferred NTEE code {inferred_ntee_code} for EIN {ein} with confidence {confidence}")
            openai_ntee_determinations += 1
            return {"ntee_code": inferred_ntee_code, "ntee_description": ntee_description, "inferred": True, "confidence": confidence}
        else:
            logger.warning(f"Failed to infer NTEE code for EIN {ein}")
            no_ntee_code_found += 1
            return {"ntee_code": "Unknown", "ntee_description": "Unknown", "inferred": False}

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
    state = input("Enter the state abbreviation to filter for (e.g., GA), or press Enter to process all states: ").upper()
    if state == "":
        state = None
        logger.info("User chose to process all states")
    else:
        logger.info(f"User selected state filter: {state}")
    
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

def print_openai_inference_summary():
    print("\n--- OpenAI Inference Summary ---")
    for i, attempt in enumerate(openai_inference_attempts, 1):
        print(f"\nAttempt {i}:")
        print(f"Organization: {attempt['organization']}")
        print(f"Mission: {attempt['mission']}")
        if attempt['response']:
            print(f"Inferred NTEE Code: {attempt['response']['ntee_code']}")
            print(f"Confidence: {attempt['response']['confidence']}")
        if attempt['error']:
            print(f"Error: {attempt['error']}")
        print("-" * 50)

def main():
    global successful_api_calls, unsuccessful_api_calls, openai_ntee_determinations, no_ntee_code_found
    logger.info(f"Starting Nonprofit Financial Health Predictor at {datetime.now()}")

    try:
        run_new990_check()

        state_filter, urls = get_user_input()
        logger.info(f"User selected state filter: {state_filter if state_filter else 'All states'}")
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
        logger.info(f"Processed {len(all_records)} {'all states' if state_filter is None else state_filter} nonprofit records from {total_files_processed} files in {processing_time:.2f} seconds")
    
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

        # Log summary of API calls and NTEE code determinations
        total_api_calls = successful_api_calls + unsuccessful_api_calls
        print(f"\nSummary of API calls and NTEE code determinations:")
        print(f"Total Nonprofit Explorer API calls: {total_api_calls}")
        print(f"Successful Nonprofit Explorer API calls: {successful_api_calls}")
        print(f"Unsuccessful Nonprofit Explorer API calls: {unsuccessful_api_calls}")
        print(f"NTEE codes determined by OpenAI: {openai_ntee_determinations}")
        print(f"Records with no NTEE code found: {no_ntee_code_found}")

        if total_api_calls > 0:
            success_rate = (successful_api_calls / total_api_calls) * 100
            print(f"Nonprofit Explorer API success rate: {success_rate:.2f}%")

        total_ntee_attempts = total_api_calls + openai_ntee_determinations
        if total_ntee_attempts > 0:
            ntee_success_rate = ((successful_api_calls + openai_ntee_determinations) / total_ntee_attempts) * 100
            print(f"Overall NTEE code determination success rate: {ntee_success_rate:.2f}%")

        # Print OpenAI inference summary
        print_openai_inference_summary()

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        logger.exception("Exception details:")

if __name__ == '__main__':
    main()