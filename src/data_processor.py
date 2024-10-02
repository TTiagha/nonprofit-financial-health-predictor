# data_processor.py

import time
from lxml import etree
from logger import logger
from config import (
    S3_BUCKET, S3_NOREV_FOLDER, S3_NOEXP_FOLDER, S3_NOASS_FOLDER, S3_NONASS_FOLDER, s3_client, desired_fields
)
from xml_parser import parse_return
from utils import is_state_nonprofit
from s3_utils import upload_file_to_s3

def print_summary(start_time, xml_files, records, total_returns_processed, state_filter, field_extraction_stats, no_revenue_files, no_exp_files, no_ass_files, no_nass_files, no_total_assets_files):
    end_time = time.time()
    processing_time = end_time - start_time
    
    logger.info("\n" + "="*50)
    logger.info("PROCESSING SUMMARY")
    logger.info("="*50)
    logger.info(f"Total XML files processed: {len(xml_files)}")
    logger.info(f"Total Returns processed: {total_returns_processed}")
    logger.info(f"Total {state_filter} nonprofit records extracted: {len(records)}")
    logger.info(f"Total processing time: {processing_time:.2f} seconds")
    
    logger.info("\nField extraction statistics:")
    for field, count in field_extraction_stats.items():
        percentage = (count / total_returns_processed * 100) if total_returns_processed > 0 else 0
        logger.info(f"{field}: {count}/{total_returns_processed} ({percentage:.2f}%)")
    
    logger.info(f"\nFiles without TotalRevenue: {len(no_revenue_files)}")
    logger.info(f"Files without TotalExpenses: {len(no_exp_files)}")
    logger.info(f"Files without TotalAssets: {len(no_ass_files)}")
    logger.info(f"Files without TotalNetAssets: {len(no_nass_files)}")
    logger.info(f"Files without TotalAssets: {len(no_total_assets_files)}")
    
    if records:
        logger.info(f"\nAverage fields per record: {sum(len(r) for r in records) / len(records):.2f}")
    else:
        logger.warning("\nNo valid nonprofit records processed.")
    
    logger.info("="*50)

def process_xml_files(xml_files, state_filter, get_ntee_code_description):
    records = []
    state_files = set()
    no_revenue_files = set()
    no_exp_files = set()
    no_ass_files = set()
    no_nass_files = set()
    no_total_assets_files = {}
    start_time = time.time()

    total_returns_processed = 0
    field_extraction_stats = {field: 0 for field in desired_fields.keys()}

    for filename, xml_content in xml_files.items():
        try:
            tree = etree.fromstring(xml_content)
            ns = {'irs': 'http://www.irs.gov/efile'}

            Returns = tree.xpath('//irs:Return', namespaces=ns)
            if not Returns:
                continue

            file_missing_revenue = False
            file_missing_expenses = False
            file_missing_assets = False
            file_missing_net_assets = False

            for Return in Returns:
                total_returns_processed += 1
                try:
                    data = parse_return(Return, ns, filename)
                    if data and is_state_nonprofit(data, state_filter):
                        mission_description = f"Nonprofit Name: {data.get('OrganizationName', '')}\nMission: {data.get('MissionStatement', '')}"
                        ntee_code = get_ntee_code_description(mission_description)
                        data['NTEECode'] = ntee_code

                        records.append(data)
                        state_files.add(filename)

                        for field in desired_fields.keys():
                            if field in data and data[field] is not None:
                                field_extraction_stats[field] += 1

                        if 'TotalRevenue' not in data or data['TotalRevenue'] is None:
                            file_missing_revenue = True
                        if 'TotalExpenses' not in data or data['TotalExpenses'] is None:
                            file_missing_expenses = True
                        if 'TotalAssets' not in data or data['TotalAssets'] is None:
                            file_missing_assets = True
                        if 'TotalNetAssets' not in data or data['TotalNetAssets'] is None:
                            file_missing_net_assets = True
                    
                except Exception as e:
                    logger.error(f'Error processing Return in {filename}: {e}')

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

    print_summary(start_time, xml_files, records, total_returns_processed, state_filter, field_extraction_stats, 
                  no_revenue_files, no_exp_files, no_ass_files, no_nass_files, no_total_assets_files)

    return records, no_total_assets_files
