# utils.py

from logger import logger

def detect_form_type(Return, ns):
    """
    Detects the form type (e.g., 990, 990EZ, 990PF, 990T) from the XML Return data.
    
    Args:
        Return (lxml.etree.Element): The XML element representing the Return.
        ns (dict): The XML namespaces to use for XPath queries.
    
    Returns:
        str: The detected form type, or 'Unknown' if no form type is matched.
    """
    form_types = {
        '990': './irs:ReturnData/irs:IRS990',
        '990EZ': './irs:ReturnData/irs:IRS990EZ',
        '990PF': './irs:ReturnData/irs:IRS990PF',
        '990T': './irs:ReturnData/irs:IRS990T'
    }

    for form_type, xpath in form_types.items():
        try:
            if Return.xpath(xpath, namespaces=ns):
                logger.debug(f"Detected form type '{form_type}' using XPath: {xpath}")
                return form_type
        except Exception as e:
            logger.error(f"Error detecting form type with path '{xpath}': {str(e)}")

    logger.warning("Form type could not be detected, returning 'Unknown'.")
    return 'Unknown'

def extract_field(Return, field_paths, ns, field_name, data):
    """
    Extracts the value of a field from the XML Return data using multiple possible paths.
    
    Args:
        Return (lxml.etree.Element): The XML element representing the Return.
        field_paths (list): A list of XPath expressions to try for extraction.
        ns (dict): The XML namespaces to use for XPath queries.
        field_name (str): The name of the field being extracted.
        data (dict): A dictionary to store the extracted field and its corresponding XPath.
    
    Returns:
        str or None: The extracted value, or None if the field is not found.
    """
    for path in field_paths:
        try:
            # Ensure paths start with './' for relative path handling
            if not path.startswith('/'):
                path = './' + path
            
            logger.debug(f"Trying path for {field_name}: {path}")
            result = Return.xpath(path, namespaces=ns)

            if result:
                value = str(result[0]).strip()  # Ensure we return a string and strip whitespace
                logger.debug(f"Field '{field_name}' found using path: {path}")
                logger.debug(f"Extracted value for '{field_name}': {value}")
                
                # Record the successful path in the data
                data[f'{field_name}_path'] = path
                return value
        except Exception as e:
            logger.error(f"Error extracting '{field_name}' using path '{path}': {str(e)}")

    logger.debug(f"Field '{field_name}' not found using any of the provided paths.")
    return None

def convert_value(value, type_):
    """
    Converts a value to the specified data type.
    
    Args:
        value (str): The value to be converted.
        type_ (str): The target data type ('int', 'double', 'boolean', etc.).
    
    Returns:
        int, float, bool, or str: The converted value, or None if conversion fails.
    """
    try:
        if type_ == 'int':
            return int(value)
        elif type_ == 'double':
            return float(value)
        elif type_ == 'boolean':
            return value.lower() in ['true', '1', 'yes', 'x']
        else:
            return value  # Return the original string if no conversion is needed
    except ValueError as e:
        logger.debug(f"Conversion error: '{value}' cannot be converted to {type_}. Error: {str(e)}")
        return None

def is_state_nonprofit(data, state):
    """
    Checks if a nonprofit organization is from a specified state.
    
    Args:
        data (dict): A dictionary containing the nonprofit's data.
        state (str): The two-letter state abbreviation to check against.
    
    Returns:
        bool: True if the nonprofit is from the specified state, False otherwise.
    """
    logger.debug(f"Checking if nonprofit is from state: {state}")
    logger.debug(f"Nonprofit data: {data}")

    state_in_data = data.get('State')
    if state_in_data:
        logger.debug(f"State found in data: {state_in_data}")
        result = state_in_data.upper() == state.upper()
        logger.debug(f"Is nonprofit from {state}? {result}")
        return result
    else:
        logger.debug(f"State not found in data. Cannot determine if nonprofit is from {state}.")
        return False
