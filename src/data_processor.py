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
    no_total_assets_files = {}
    start_time = time.time()

    total_returns_processed = 0
    total_returns_with_bac = 0

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

            file_missing_revenue = False
            file_missing_expenses = False
            file_missing_assets = False
            file_missing_net_assets = False
            file_missing_total_assets = False
            returns_processed_in_file = 0
            returns_with_bac_in_file = 0

            for i, Return in enumerate(Returns):
                logger.info(f'Processing Return {i+1} in {filename}')
                total_returns_processed += 1
                returns_processed_in_file += 1
                try:
                    data = parse_return(Return, ns, filename)
                    if data and is_state_nonprofit(data, state_filter):
                        records.append(data)
                        state_files.add(filename)
                        logger.info(f"{state_filter} nonprofit found in {filename}, Return {i+1}")

                        if 'TotalRevenue' not in data or data['TotalRevenue'] is None:
                            file_missing_revenue = True
                        if 'TotalExpenses' not in data or data['TotalExpenses'] is None:
                            file_missing_expenses = True
                        if 'TotalAssets' not in data or data['TotalAssets'] is None:
                            file_missing_assets = True
                        if 'TotalNetAssets' not in data or data['TotalNetAssets'] is None:
                            file_missing_net_assets = True
                        
                        # Use the extracted data to check for BusinessActivityCode
                        if 'BusinessActivityCode' in data and data['BusinessActivityCode']:
                            total_returns_with_bac += 1
                            returns_with_bac_in_file += 1
                            logger.info(f"BusinessActivityCode found in {filename}, Return {i+1}: {data.get('BusinessActivityCode')}")
                        else:
                            logger.warning(f"BusinessActivityCode not found in {filename}, Return {i+1}")
                    
                except Exception as e:
                    logger.error(f'Error processing Return {i+1} in {filename}: {e}')

            logger.info(f"Processed {returns_processed_in_file} Returns in {filename}")
            logger.info(f"Returns with BusinessActivityCode: {returns_with_bac_in_file}/{returns_processed_in_file}")

            if file_missing_revenue:
                no_revenue_files.add(filename)
            if file_missing_expenses:
                no_exp_files.add(filename)
            if file_missing_assets:
                no_ass_files.add(filename)
            if file_missing_net_assets:
                no_nass_files.add(filename)
            if file_missing_assets:
                no_total_assets_files[filename] = xml_content

        except Exception as e:
            logger.error(f'Error processing {filename}: {e}')

        file_end_time = time.time()
        logger.info(f"Processed {filename} in {file_end_time - file_start_time:.2f} seconds")

    end_time = time.time()
    logger.info(f'Processed {len(records)} {state_filter} nonprofit records from {len(xml_files)} files in {end_time - start_time:.2f} seconds')
    logger.info(f'Total Returns processed: {total_returns_processed}')
    logger.info(f'Total Returns with BusinessActivityCode: {total_returns_with_bac}/{total_returns_processed}')
    logger.info(f'Files without TotalRevenue: {len(no_revenue_files)}')
    logger.info(f'Files without TotalExpenses: {len(no_exp_files)}')
    logger.info(f'Files without TotalAssets: {len(no_ass_files)}')
    logger.info(f'Files without TotalNetAssets: {len(no_nass_files)}')
    logger.info(f'Files without TotalAssets: {len(no_total_assets_files)}')

    if records:
        logger.info(f"Average fields per record: {sum(len(r) for r in records) / len(records):.2f}")
    else:
        logger.warning("No valid nonprofit records processed.")

    return records, no_total_assets_files
