# data_analyzer.py

from logger import logger
from collections import Counter
from config import desired_fields

def analyze_field_coverage(records):
    total_records = len(records)
    if total_records == 0:
        logger.warning("No records to analyze field coverage.")
        return

    field_coverage = {field: sum(1 for r in records if field in r) for field in desired_fields}

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
