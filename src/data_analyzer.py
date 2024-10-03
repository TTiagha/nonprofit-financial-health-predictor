# data_analyzer.py
# This file has been updated to include analysis for NTEE Code and Description fields

from logger import logger
from collections import Counter
from config import desired_fields

def analyze_field_coverage(records):
    total_records = len(records)
    if total_records == 0:
        logger.warning("No records to analyze field coverage.")
        return

    field_coverage = {field: sum(1 for r in records if field in r) for field in desired_fields}
    
    # Add NTEE fields to the analysis
    field_coverage['NTEECode'] = sum(1 for r in records if 'NTEECode' in r and r['NTEECode'])
    field_coverage['NTEEDescription'] = sum(1 for r in records if 'NTEEDescription' in r and r['NTEEDescription'])

    logger.info("Field coverage analysis:")
    for field, count in field_coverage.items():
        percentage = (count / total_records) * 100
        logger.info(f"{field}: {count}/{total_records} ({percentage:.2f}%)")

def analyze_path_usage(records):
    fields_to_analyze = ['TotalExpenses', 'TotalAssets', 'TotalNetAssets', 'MissionStatement']
    path_usage = {field: {} for field in fields_to_analyze}

    for record in records:
        form_type = record['FormType']
        for field in fields_to_analyze:
            if field in record:
                path = record.get(f'{field}_path', 'Unknown')
                if form_type not in path_usage[field]:
                    path_usage[field][form_type] = {}
                path_usage[field][form_type][path] = path_usage[field][form_type].get(path, 0) + 1

    for field in fields_to_analyze:
        logger.info(f"Path usage analysis for {field}:")
        for form_type, paths in path_usage[field].items():
            logger.info(f"  {form_type}:")
            for path, count in paths.items():
                logger.info(f"    {path}: {count}")

def analyze_ntee_data(records):
    ntee_codes = [r['NTEECode'] for r in records if 'NTEECode' in r and r['NTEECode']]
    ntee_descriptions = [r['NTEEDescription'] for r in records if 'NTEEDescription' in r and r['NTEEDescription']]

    logger.info("NTEE Code analysis:")
    code_counter = Counter(ntee_codes)
    for code, count in code_counter.most_common(10):
        percentage = (count / len(ntee_codes)) * 100
        logger.info(f"  {code}: {count} ({percentage:.2f}%)")

    logger.info("NTEE Description analysis:")
    desc_counter = Counter(ntee_descriptions)
    for desc, count in desc_counter.most_common(10):
        percentage = (count / len(ntee_descriptions)) * 100
        logger.info(f"  {desc}: {count} ({percentage:.2f}%)")

    logger.info(f"Total unique NTEE Codes: {len(set(ntee_codes))}")
    logger.info(f"Total unique NTEE Descriptions: {len(set(ntee_descriptions))}")

def analyze_data(records):
    analyze_field_coverage(records)
    analyze_path_usage(records)
    analyze_ntee_data(records)
