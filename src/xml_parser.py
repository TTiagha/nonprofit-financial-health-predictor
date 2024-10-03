# xml_parser.py

from lxml import etree
from logger import logger
from config import desired_fields
from utils import convert_value, detect_form_type

def extract_field(element, field_name, namespaces):
    field_info = desired_fields.get(field_name, {})
    paths_info = field_info.get('paths', {})
    if not paths_info:
        logger.warning(f"No paths defined for field '{field_name}'. Skipping extraction.")
        return None, None

    paths = paths_info.get('Common', [])
    # Include form-specific paths if available
    form_type = detect_form_type(element, namespaces)
    if form_type in paths_info:
        paths = paths_info[form_type] + paths  # Form-specific paths take precedence

    for path in paths:
        try:
            if not path.startswith('/'):
                path = './' + path
            logger.debug(f"Trying path for {field_name}: {path}")
            use_namespaces = not ('local-name()' in path or '/*' in path)
            if use_namespaces:
                result = element.xpath(path, namespaces=namespaces)
            else:
                result = element.xpath(path)
            
            if result:
                # Handle cases where the result is an element, a string, or a list
                if isinstance(result[0], etree._Element):
                    value = result[0].text
                elif isinstance(result[0], str):
                    value = result[0]
                else:
                    value = str(result[0])
                
                if value:
                    value = value.strip()
                    logger.debug(f"Field {field_name} found using path: {path}")
                    logger.debug(f"Extracted value for {field_name}: {value}")
                    return value, path
                else:
                    logger.debug(f"Empty result for path '{path}' while extracting {field_name}.")
            else:
                logger.debug(f"No result for path '{path}' while extracting {field_name}.")
        except Exception as e:
            logger.error(f"Error extracting {field_name} using path '{path}': {str(e)}")

    logger.warning(f"{field_name} not found using provided paths.")
    return None, None

def parse_return(Return, namespaces, filename):
    try:
        data = {}
        form_type = detect_form_type(Return, namespaces)
        logger.info(f"Detected form type for {filename}: {form_type}")
        data['FormType'] = form_type

        # Extract fields defined in desired_fields
        for field_name in desired_fields.keys():
            try:
                value, path = extract_field(Return, field_name, namespaces)
                if value is not None:
                    field_info = desired_fields[field_name]
                    # Handle special case for 'EIN' to remove hyphens
                    if field_name == 'EIN':
                        value = value.replace('-', '')
                    data[field_name] = convert_value(value, field_info['type'])
                    data[f'{field_name}_path'] = path  # Record the successful path
                    logger.debug(f"Extracted {field_name}: {data[field_name]} from {filename} using path: {path}")
                else:
                    logger.debug(f"Field {field_name} not found in {filename} for form type {form_type}")
            except Exception as e:
                logger.error(f"Error processing field {field_name} in {filename}: {str(e)}")

        # Check for missing critical fields
        if 'EIN' not in data or not str(data.get('EIN', '')).isdigit():
            logger.warning(f"Invalid or missing EIN in {filename}. Skipping record.")
            return None

        if 'TaxYear' not in data or data.get('TaxYear') is None:
            logger.warning(f"Missing TaxYear in {filename}. Skipping record.")
            return None

        # Handle NTEE Code and Description more gracefully
        if 'NTEECode' not in data or data['NTEECode'] is None:
            logger.warning(f"NTEECode not found in {filename}.")
            data['NTEECode'] = 'Unknown'
        
        if 'NTEEDescription' not in data or data['NTEEDescription'] is None:
            logger.warning(f"NTEEDescription not found in {filename}.")
            data['NTEEDescription'] = 'Unknown'

        # Ensure all string fields are properly handled to avoid NoneType errors
        for field, value in data.items():
            if desired_fields.get(field, {}).get('type') == 'string' and value is None:
                data[field] = ''  # Replace None with empty string for string fields

        data['_source_file'] = filename
        return data if len(data) > 1 else None

    except Exception as e:
        logger.error(f"Unexpected error processing Return in {filename}: {str(e)}")
        return None
