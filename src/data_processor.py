# data_processor.py

import time
from lxml import etree
from logger import logger
from config import (
    S3_BUCKET, S3_NOREV_FOLDER, S3_NOEXP_FOLDER, S3_NOASS_FOLDER, S3_NONASS_FOLDER, s3_client
)
from xml_parser import parse_return
from utils import is_state_nonprofit
from s3_utils import upload_file_to_s3

def process_xml_files(xml_files, state_filter):
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
            ns = {'irs': 'http://www.irs.gov/efile'}

            Returns = tree.xpath('//irs:Return', namespaces=ns)
            if not Returns:
                logger.warning(f'No Return elements found in {filename}')
                continue

            # Flags for missing fields
            file_missing_revenue = False
            file_missing_expenses = False
            file_missing_assets = False
            file_missing_net_assets = False

            for i, Return in enumerate(Returns):
                logger.info(f'Processing Return {i+1} in {filename}')
                try:
                    data = parse_return(Return, ns, filename)
                    if data and is_state_nonprofit(data, state_filter):  # Check for the specified state
                        records.append(data)
                        state_files.add(filename)
                        logger.info(f"{state_filter} nonprofit found in {filename}, Return {i+1}")

                        # Check for missing fields and upload to S3 if necessary
                        # ... (same logic as before)

                except Exception as e:
                    logger.error(f'Error processing Return {i+1} in {filename}: {e}')

            # Upload to the appropriate S3 folder based on missing fields
            if file_missing_revenue:
                no_revenue_files.add(filename)
                upload_file_to_s3(xml_content, f'{S3_NOREV_FOLDER}/{filename}')

            # ... handle other missing fields similarly

        except Exception as e:
            logger.error(f'Error processing {filename}: {e}')

        file_end_time = time.time()
        logger.info(f"Processed {filename} in {file_end_time - file_start_time:.2f} seconds")

    end_time = time.time()
    logger.info(f'Processed {len(records)} {state_filter} nonprofit records from {len(xml_files)} files in {end_time - start_time:.2f} seconds')
    # ... rest of the logging and statistics
    logger.info(f'Files without TotalRevenue: {len(no_revenue_files)}')
    logger.info(f'Files without TotalExpenses: {len(no_exp_files)}')
    logger.info(f'Files without TotalAssets: {len(no_ass_files)}')
    logger.info(f'Files without TotalNetAssets: {len(no_nass_files)}')

    if records:
        logger.info(f"Average fields per record: {sum(len(r) for r in records) / len(records):.2f}")
    else:
        logger.warning("No valid Georgia nonprofit records processed.")


    return records
