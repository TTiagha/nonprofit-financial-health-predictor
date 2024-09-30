# xml_parser.py

from lxml import etree
from logger import logger
from config import desired_fields
from utils import extract_field, convert_value, detect_form_type
import re

def parse_return(Return, ns, filename):
    # Correct namespace mapping
    ns = {'irs': 'http://www.irs.gov/efile'}

    form_type = detect_form_type(Return, ns)
    logger.info(f"Detected form type for {filename}: {form_type}")
    data = {'FormType': form_type}

    # Add state extraction
    state_paths = desired_fields['State']['paths']['Common']
    state = extract_field(Return, state_paths, ns, 'State', data)
    if state:
        data['State'] = state

    for field_name, field_info in desired_fields.items():
        field_paths = field_info['paths'].get(form_type, field_info['paths'].get('Common', []))
        value = extract_field(Return, field_paths, ns, field_name, data)
        if value is not None:
            data[field_name] = convert_value(value, field_info['type'])
            if field_name == 'BusinessActivityCode':
                logger.info(f"Extracted BusinessActivityCode: {data[field_name]} from {filename}")
        else:
            logger.debug(f"Field {field_name} not found in {filename} for form type {form_type}")

        # Additional extraction attempt for BusinessActivityCode
        if field_name == 'BusinessActivityCode' and value is None:
            logger.debug(f"Attempting alternative extraction for BusinessActivityCode in {filename}")
            alternative_paths = [
                '//*[contains(local-name(), "PrincipalBusinessActivityCd")]/text()',
                '//*[contains(local-name(), "BusinessActivityCode")]/text()',
                '//*[contains(local-name(), "ActivityCd")]/text()',
            ]
            for path in alternative_paths:
                try:
                    value = Return.xpath(path, namespaces=ns)
                    if value:
                        data[field_name] = convert_value(value[0], field_info['type'])
                        logger.info(f"Extracted BusinessActivityCode using alternative path: {data[field_name]} from {filename}")
                        break
                except Exception as e:
                    logger.error(f"Error extracting BusinessActivityCode using alternative path in {filename}: {str(e)}")

    # Explicit handling for TaxYear
    tax_year = data.get('TaxYear')
    if tax_year is None:
        # Try to extract year from TaxPeriodEndDt
        tax_period_end_dt = extract_field(Return, ['irs:ReturnHeader/irs:TaxPeriodEndDt/text()'], ns, 'TaxPeriodEndDt', data)
        if tax_period_end_dt:
            match = re.search(r'\d{4}', tax_period_end_dt)
            if match:
                tax_year = int(match.group())
                logger.info(f"Extracted TaxYear {tax_year} from TaxPeriodEndDt in {filename}")
            else:
                logger.warning(f"Could not extract year from TaxPeriodEndDt: {tax_period_end_dt} in {filename}")
        else:
            logger.warning(f"TaxYear and TaxPeriodEndDt not found in {filename}")

    if tax_year is not None:
        data['TaxYear'] = tax_year
    else:
        # If we still can't determine the tax year, use a default or skip the record
        logger.error(f"Could not determine TaxYear for {filename}. Skipping record.")
        return None

    logger.info(f"Extracted {len(data)} fields from {filename}")
    logger.debug(f"Extracted fields from {filename}: {', '.join(data.keys())}")
    missing_fields = set(desired_fields.keys()) - set(data.keys())

    if missing_fields:
        logger.debug(f"Missing fields from {filename}: {', '.join(missing_fields)}")
    if 'TotalRevenue' not in data:
        logger.warning(f"Critical field 'TotalRevenue' missing in {filename}")
    if 'TotalExpenses' not in data:
        logger.warning(f"Critical field 'TotalExpenses' missing in {filename}")
    if 'EIN' in data and not str(data['EIN']).isdigit():
        logger.warning(f"Invalid EIN format in {filename}")
        return None  # Return None for invalid data

    data['_source_file'] = filename
    return data if len(data) > 1 else None
