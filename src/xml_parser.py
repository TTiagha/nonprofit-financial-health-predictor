# xml_parser.py

from lxml import etree
from logger import logger
from config import desired_fields
from utils import convert_value, detect_form_type
import re

# xml_parser.py

def extract_field(element, field_name, namespaces):
    field_info = desired_fields.get(field_name, {})
    paths_info = field_info.get('paths', {})
    if not paths_info:
        logger.warning(f"No paths defined for field '{field_name}'. Skipping extraction.")
        return None

    paths = paths_info.get('Common', [])
    # Include form-specific paths if available
    form_type = detect_form_type(element, namespaces)
    if form_type in paths_info:
        paths = paths_info[form_type] + paths  # Form-specific paths take precedence

    for path in paths:
        try:
            # Ensure paths are flexible with namespaces
            result = element.xpath(path, namespaces=namespaces)
            if result:
                # Handle cases where the result is an element or a string
                if isinstance(result[0], etree._Element):
                    value = result[0].text.strip()
                else:
                    value = str(result[0]).strip()
                logger.debug(f"Found {field_name} using path '{path}': {value}")
                return value
            else:
                logger.debug(f"No result for path '{path}' while extracting {field_name}.")
        except Exception as e:
            logger.error(f"Error extracting {field_name} using path '{path}': {str(e)}")

    logger.warning(f"{field_name} not found using provided paths.")
    return None


def parse_return(Return, namespaces, filename):
    data = {}
    form_type = detect_form_type(Return, namespaces)
    logger.info(f"Detected form type for {filename}: {form_type}")
    data['FormType'] = form_type

    # Extract fields defined in desired_fields
    for field_name in desired_fields.keys():
        value = extract_field(Return, field_name, namespaces)
        if value is not None:
            field_info = desired_fields[field_name]
            # Handle special case for 'EIN' to remove hyphens
            if field_name == 'EIN':
                value = value.replace('-', '')
            data[field_name] = convert_value(value, field_info['type'])
            logger.debug(f"Extracted {field_name}: {data[field_name]} from {filename}")
            if field_name == 'BusinessActivityCode':
                logger.info(f"Extracted BusinessActivityCode: {data[field_name]} from {filename}")
        else:
            logger.debug(f"Field {field_name} not found in {filename} for form type {form_type}")

    # Explicit handling for TaxYear
    if 'TaxYear' not in data or data['TaxYear'] is None:
        # Try to extract 'TaxPeriodEndDt' and parse year
        tax_period_end_dt = extract_field(Return, 'TaxPeriodEndDt', namespaces)
        if tax_period_end_dt:
            match = re.search(r'\d{4}', tax_period_end_dt)
            if match:
                tax_year = int(match.group())
                data['TaxYear'] = tax_year
                logger.info(f"Extracted TaxYear {tax_year} from TaxPeriodEndDt in {filename}")
            else:
                logger.warning(f"Could not extract year from TaxPeriodEndDt: {tax_period_end_dt} in {filename}")
        else:
            logger.warning(f"TaxYear and TaxPeriodEndDt not found in {filename}")

    if 'TaxYear' not in data or data['TaxYear'] is None:
        # If we still can't determine the tax year, skip the record
        logger.error(f"Could not determine TaxYear for {filename}. Skipping record.")
        return None

    # Check for missing critical fields
    if 'EIN' not in data or not str(data['EIN']).isdigit():
        logger.warning(f"Invalid or missing EIN in {filename}. Skipping record.")
        return None

    data['_source_file'] = filename
    return data if len(data) > 1 else None
