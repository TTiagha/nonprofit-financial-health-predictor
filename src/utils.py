# utils.py

from logger import logger

def detect_form_type(Return, ns):
    form_types = {
        '990': './irs:ReturnData/irs:IRS990',
        '990EZ': './irs:ReturnData/irs:IRS990EZ',
        '990PF': './irs:ReturnData/irs:IRS990PF',
        '990T': './irs:ReturnData/irs:IRS990T'
    }
    for form_type, xpath in form_types.items():
        try:
            if Return.xpath(xpath, namespaces=ns):
                return form_type
        except Exception as e:
            logger.error(f'Error detecting form type with path {xpath}: {e}')
    return 'Unknown'

def extract_field(Return, field_paths, ns, field_name, data):
    for path in field_paths:
        try:
            if not path.startswith('/'):
                path = './' + path
            logger.debug(f"Trying path for {field_name}: {path}")
            use_namespaces = not ('local-name()' in path or '/*' in path)
            if use_namespaces:
                result = Return.xpath(path, namespaces=ns)
            else:
                result = Return.xpath(path)
            if result:
                value = str(result[0]).strip()
                logger.debug(f"Field {field_name} found using path: {path}")
                logger.debug(f"Extracted value for {field_name}: {value}")
                data[f'{field_name}_path'] = path  # Record the successful path
                return value
        except Exception as e:
            logger.error(f'Error extracting {field_name} using path {path}: {e}')
    logger.debug(f'Field {field_name} not found using any defined path.')
    return None

def convert_value(value, type_):
    try:
        if type_ == 'int':
            return int(value)
        elif type_ == 'double':
            return float(value)
        elif type_ == 'boolean':
            return value.lower() in ['true', '1', 'yes', 'x']
        else:
            return value
    except ValueError:
        logger.debug(f"Conversion error: '{value}' cannot be converted to {type_}.")
        return None

def is_state_nonprofit(data, state):
    """
    Check if the nonprofit organization is from the specified state.

    Args:
    data (dict): A dictionary containing the nonprofit's data.
    state (str): The two-letter state code to check against.

    Returns:
    bool: True if the nonprofit is from the specified state, False otherwise.
    """
    return data.get('State') == state.upper()
