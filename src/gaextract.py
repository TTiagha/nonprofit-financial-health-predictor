import boto3
import botocore
import requests
import zipfile
import io
import os
import logging
from logging.handlers import RotatingFileHandler
from lxml import etree
import pyarrow as pa
import pyarrow.parquet as pq
from collections import Counter
import time

# Configure logging
log_filename = 'health.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        RotatingFileHandler(log_filename, maxBytes=10000000, backupCount=5),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

desired_fields = {
    
    'State': {
    'type': 'string',
    'paths': {
        'Common': [
            'irs:ReturnHeader/irs:Filer/irs:USAddress/irs:StateAbbreviationCd/text()',
            'irs:ReturnHeader/irs:Filer/irs:BusinessOfficeGrp/irs:USAddress/irs:StateAbbreviationCd/text()',
            '//*[contains(local-name(), "StateAbbreviationCd")]/text()'
        ]
    }
},


    'EIN': {
        'type': 'int',
        'paths': {
            'Common': [
                'irs:ReturnHeader/irs:Filer/irs:EIN/text()'
            ]
        }
    },
    'TaxYr': {
        'type': 'int',
        'paths': {
            'Common': [
                'irs:ReturnHeader/irs:TaxYr/text()',
                'irs:ReturnHeader/irs:TaxYear/text()',
                'substring(irs:ReturnHeader/irs:TaxPeriodEndDt/text(),1,4)'
            ]
        }
    },
    'OrganizationName': {
        'type': 'string',
        'paths': {
            'Common': [
                'irs:ReturnHeader/irs:Filer/irs:BusinessName/irs:BusinessNameLine1Txt/text()',
                'irs:ReturnHeader/irs:Filer/irs:Name/irs:BusinessNameLine1Txt/text()'
            ]
        }
    },
    'TotalRevenue': {
        'type': 'double',
        'paths': {
            '990': [
                'irs:ReturnData/irs:IRS990/irs:Revenue/irs:TotalRevenueAmt/text()',
                'irs:ReturnData/irs:IRS990/irs:TotalRevenueGrp/irs:TotalRevenueColumnAmt/text()'
            ],
            '990EZ': [
                'irs:ReturnData/irs:IRS990EZ/irs:TotalRevenueAmt/text()'
            ],
            '990PF': [
                'irs:ReturnData/irs:IRS990PF/irs:TotalRevenueAndExpensesAmt/text()',
                'irs:ReturnData/irs:IRS990PF/irs:AnalysisOfRevenueAndExpenses/irs:TotalRevAndExpnssAmt/text()'
            ],
            '990T': [
                'irs:ReturnData/irs:IRS990T/irs:TotalUBTIAmt/text()'
            ]
        }
    },
    'TotalExpenses': {
        'type': 'double',
        'paths': {
            '990': [
                '//*[local-name()="TotalFunctionalExpensesAmt"]/text()',
                '//*[local-name()="TotalExpensesAmt"]/text()',
                '//*[local-name()="TotalFunctionalExpenses"]/text()',
                '//*[local-name()="TotalExpenses"]/text()',
                '//*[contains(local-name(), "TotalFunctionalExpenses") or contains(local-name(), "TotalExpenses")]/text()',
            ],
            '990EZ': [
                '//*[local-name()="TotalExpensesAmt"]/text()',
                '//*[local-name()="TotalExpenses"]/text()',
                '//*[contains(local-name(), "TotalExpenses")]/text()',
            ],
            '990PF': [
                '//*[local-name()="TotalExpensesAndDisbursementsAmt"]/text()',
                '//*[local-name()="TotalExpensesRevAndExpnssAmt"]/text()',
                '//*[local-name()="TotalExpenses"]/text()',
                '//*[contains(local-name(), "TotalExpenses")]/text()',
            ],
            '990T': [
                '//*[local-name()="TotalDeductionsAmt"]/text()',
                '//*[local-name()="TotalDeductions"]/text()',
                '//*[local-name()="TotalExpenses"]/text()',
                '//*[contains(local-name(), "TotalExpenses") or contains(local-name(), "TotalDeductions")]/text()',
            ],
            'Common': [
                '//*[contains(local-name(), "TotalExpenses")]/text()',
                '//*[contains(local-name(), "TotalDeductions")]/text()',
            ]
        }
    },
    'TotalAssets': {
    'type': 'double',
    'paths': {
        '990': [
            '//*[local-name()="TotalAssetsEOYAmt"]/text()',
            '//*[local-name()="TotalAssetsEndOfYear"]/text()',
            '//*[local-name()="AssetsEOYAmt"]/text()',
            '//*[local-name()="AssetsEOY"]/text()',
            '//*[local-name()="TotalAssets"]/text()',
            '//*[contains(local-name(), "TotalAssets") and contains(local-name(), "EOY")]/text()',
            '//*[contains(local-name(), "TotalAssets")]/text()',
        ],
        '990EZ': [
            '//*[local-name()="TotalAssetsEOYAmt"]/text()',
            '//*[local-name()="AssetsEOYAmt"]/text()',
            '//*[local-name()="TotalAssets"]/text()',
            '//*[contains(local-name(), "TotalAssets") and contains(local-name(), "EOY")]/text()',
        ],
        '990PF': [
            '//*[local-name()="TotalAssetsEOYAmt"]/text()',
            '//*[local-name()="FMVAssetsEOYAmt"]/text()',
            '//*[local-name()="TotalAssets"]/text()',
            '//*[contains(local-name(), "TotalAssets") and contains(local-name(), "EOY")]/text()',
            '//*[contains(local-name(), "FMVAssets") and contains(local-name(), "EOY")]/text()',
        ],
        '990T': [
            '//*[local-name()="BookValueAssetsEOYAmt"]/text()',
            '//*[local-name()="TotalAssetsEOYAmt"]/text()',
            '//*[local-name()="TotalAssets"]/text()',
            '//*[contains(local-name(), "TotalAssets") and contains(local-name(), "EOY")]/text()',
        ],
        'Common': [
            '//*[contains(local-name(), "TotalAssets") and contains(local-name(), "EOY")]/text()',
            '//*[contains(local-name(), "AssetsEOY")]/text()',
            '//*[contains(local-name(), "TotalAssets")]/text()',
            '//*[contains(local-name(), "FMVAssets") and contains(local-name(), "EOY")]/text()',
            '//*[contains(local-name(), "BookValueAssets") and contains(local-name(), "EOY")]/text()',
        ]
        }
    },
    'MissionStatement': {
    'type': 'string',
    'paths': {
        '990': [
            '//*[local-name()="MissionDesc"]/text()',
            '//*[local-name()="MissionStatement"]/text()',
            '//*[local-name()="MissionStatementTxt"]/text()',
            '//*[local-name()="ActivityOrMissionDesc"]/text()',
            '//*[local-name()="PrimaryExemptPurposeTxt"]/text()',
            '//*[contains(local-name(), "Mission") and contains(local-name(), "Desc")]/text()',
        ],
        '990EZ': [
            '//*[local-name()="MissionDesc"]/text()',
            '//*[local-name()="MissionStatement"]/text()',
            '//*[local-name()="PrimaryExemptPurposeTxt"]/text()',
            '//*[contains(local-name(), "Mission") and contains(local-name(), "Desc")]/text()',
        ],
        '990PF': [
            '//*[local-name()="MissionDesc"]/text()',
            '//*[local-name()="MissionStatement"]/text()',
            '//*[local-name()="ActivityOrMissionDesc"]/text()',
            '//*[local-name()="Description1Txt"]/text()',
            '//*[local-name()="Description2Txt"]/text()',
            '//*[local-name()="SummaryOfDirectChrtblActyGrp"]/*[contains(local-name(), "Description")]/text()',
            '//*[contains(local-name(), "PrimaryExemptPurpose")]/text()',
        ],
        '990T': [
            '//*[local-name()="MissionDesc"]/text()',
            '//*[local-name()="MissionStatement"]/text()',
            '//*[contains(local-name(), "Mission") and contains(local-name(), "Desc")]/text()',
        ],
        'Common': [
            '//*[contains(local-name(), "Mission") and (contains(local-name(), "Desc") or contains(local-name(), "Statement") or contains(local-name(), "Txt"))]/text()',
            '//*[contains(local-name(), "ActivityOrMissionDesc")]/text()',
            '//*[contains(local-name(), "PrimaryExemptPurposeTxt")]/text()',
            '//*[contains(local-name(), "Description")]/text()',
        ]
    }
},

    'TotalNetAssets': {
    'type': 'double',
    'paths': {
        '990': [
            '//*[local-name()="TotalNetAssetsFundBalanceEOYAmt"]/text()',
            '//*[local-name()="TotNetAssetsFundBalanceEOYAmt"]/text()',
            '//*[local-name()="NetAssetsFundBalancesEOYAmt"]/text()',
            '//*[local-name()="NetAssetsOrFundBalancesEOYAmt"]/text()',
            '//*[local-name()="NetAssetsEOYAmt"]/text()',
            '//*[contains(local-name(), "TotalNetAssets") and contains(local-name(), "EOY")]/text()',
            '//*[contains(local-name(), "NetAssets") and contains(local-name(), "EOY")]/text()',
        ],
        '990EZ': [
            '//*[local-name()="TotalNetAssetsEOYAmt"]/text()',
            '//*[local-name()="NetAssetsOrFundBalancesEOYAmt"]/text()',
            '//*[contains(local-name(), "NetAssets") and contains(local-name(), "EOY")]/text()',
        ],
        '990PF': [
            '//*[local-name()="TotNetAstOrFundBalancesEOYAmt"]/text()',
            '//*[local-name()="TotalNetAssetsEOYAmt"]/text()',
            '//*[local-name()="NetAssetsOrFundBalancesEOYAmt"]/text()',
            '//*[contains(local-name(), "NetAssets") and contains(local-name(), "EOY")]/text()',
        ],
        '990T': [
            '//*[local-name()="TotalNetAssetsEOYAmt"]/text()',
            '//*[local-name()="NetAssetsOrFundBalancesEOYAmt"]/text()',
            '//*[contains(local-name(), "NetAssets") and contains(local-name(), "EOY")]/text()',
        ],
        'Common': [
            '//*[contains(local-name(), "TotalNetAssets") and contains(local-name(), "EOY")]/text()',
            '//*[contains(local-name(), "NetAssets") and contains(local-name(), "EOY")]/text()',
            '//*[contains(local-name(), "TotNetAstOrFundBalancesEOYAmt")]/text()',
            '//*[contains(local-name(), "NetAssetsOrFundBalancesEOYAmt")]/text()',
        ]
    }
},


    # Add other fields as needed
}

# AWS S3 bucket name
s3_bucket = 'nonprofit-financial-health-data'  # Replace with your S3 bucket name
s3_folder = 'irs990-data'  # Folder in S3 to save the Parquet files

# List of filenames to process
file_list = [
    '202300109339300100_public.xml',
    '202300109339300105_public.xml',
    '202300109339300250_public.xml',
    '202300109349100000_public.xml',
    '202300109349100005_public.xml',
    '202340269349301119_public.xml',
    '202340269349301124_public.xml',
    '202340269349301554_public.xml',
    '202340269349301559_public.xml',
    '202340269349301564_public.xml',
    '202340269349301569_public.xml',
    '202340269349301574_public.xml',
    '202340269349301654_public.xml',
    '202340269349301704_public.xml',
    '202340269349301709_public.xml',
    '202340269349301714_public.xml',
    '202340269349301719_public.xml',
    '202340269349301724_public.xml',
    '202340269349301729_public.xml',
    '202340269349301734_public.xml',
    '202340269349301804_public.xml',
    '202340269349301809_public.xml',
    '202340269349301814_public.xml',
    '202340269349301819_public.xml',
    '202340269349301824_public.xml',
    '202340269349301854_public.xml',
    '202340269349301859_public.xml',
    '202340269349301904_public.xml',
    '202340269349301909_public.xml',
    '202340269349301954_public.xml'
]

def download_and_extract_xml_files(url):
    logger.info(f'Downloading zip file from {url}')
    response = requests.get(url)
    response.raise_for_status()
    logger.info('Download complete.')

    logger.info('Extracting all XML files from zip archive.')
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    xml_files = {}
    for filename in zip_file.namelist():
        if filename.endswith('.xml'):
            with zip_file.open(filename) as file:
                xml_files[filename] = file.read()
                logger.info(f'Extracted {filename}')
    logger.info('Extraction complete.')
    return xml_files

def detect_form_type(Return, ns):
    form_types = {
        '990': 'irs:ReturnData/irs:IRS990',
        '990PF': 'irs:ReturnData/irs:IRS990PF',
        '990T': 'irs:ReturnData/irs:IRS990T'
    }
    for form_type, xpath in form_types.items():
        if Return.xpath(xpath, namespaces=ns):
            return form_type
    return 'Unknown'

def parse_return(Return, ns, filename):
    form_type = detect_form_type(Return, ns)
    logger.info(f"Detected form type for {filename}: {form_type}")
    data = {'FormType': form_type}
    
    # Add state extraction
    state_paths = [
        'irs:ReturnHeader/irs:Filer/irs:USAddress/irs:StateAbbreviationCd/text()',
        'irs:ReturnHeader/irs:Filer/irs:BusinessOfficeGrp/irs:USAddress/irs:StateAbbreviationCd/text()',
        '//*[contains(local-name(), "StateAbbreviationCd")]/text()'
    ]
    state = extract_field(Return, state_paths, ns, 'State')
    if state:
        data['State'] = state
    
    for field_name, field_info in desired_fields.items():
        field_paths = field_info['paths'].get(form_type, field_info['paths'].get('Common', []))
        value = extract_field(Return, field_paths, ns, field_name)
        if value is not None:
            data[field_name] = convert_value(value, field_info['type'])
        else:
            logger.debug(f"Field {field_name} not found in {filename} for form type {form_type}")
    
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

def extract_field(Return, field_paths, ns, field_name):
    for path in field_paths:
        try:
            result = Return.xpath(path, namespaces=ns)
            if result:
                value = str(result[0]).strip()
                logger.debug(f"Field {field_name} found using path: {path}")
                logger.debug(f"Extracted value for {field_name}: {value}")
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

def is_georgia_nonprofit(data):
    state = data.get('State')
    return state == 'GA'

def process_xml_files(xml_files):
    records = []
    ga_files = set()
    start_time = time.time()
    for filename, xml_content in xml_files.items():
        file_start_time = time.time()
        logger.info(f'Processing {filename}')
        try:
            tree = etree.fromstring(xml_content)
            ns = tree.nsmap.copy()
            irs_namespace = ns.get('ns0') or ns.get(None)
            if irs_namespace:
                ns = {'irs': irs_namespace}
            else:
                logger.warning(f'IRS namespace not found in {filename}')
                ns = {}

            Returns = tree.xpath('//irs:Return', namespaces=ns)
            if not Returns:
                logger.warning(f'No Return elements found in {filename}')
                continue

            for i, Return in enumerate(Returns):
                logger.info(f'Processing Return {i+1} in {filename}')
                try:
                    data = parse_return(Return, ns, filename)
                    if data and is_georgia_nonprofit(data):  # Only append Georgia nonprofits
                        records.append(data)
                        ga_files.add(filename)
                        logger.info(f"Georgia nonprofit found in {filename}, Return {i+1}")
                except Exception as e:
                    logger.error(f'Error processing Return {i+1} in {filename}: {e}')
        except Exception as e:
            logger.error(f'Error processing {filename}: {e}')
        
        file_end_time = time.time()
        logger.info(f"Processed {filename} in {file_end_time - file_start_time:.2f} seconds")
    
    end_time = time.time()
    logger.info(f'Processed {len(records)} Georgia nonprofit records from {len(xml_files)} files in {end_time - start_time:.2f} seconds')
    
    if records:
        logger.info(f"Average fields per record: {sum(len(r) for r in records) / len(records):.2f}")
    else:
        logger.warning("No valid Georgia nonprofit records processed.")
    logger.info(f"Files containing Georgia nonprofits: {', '.join(ga_files)}")
    return records

def save_to_s3_parquet(records, s3_bucket, s3_folder):
    if not records:
        logger.warning('No valid records to save.')
        return

    logger.info('Converting records to Parquet format.')
    # Convert records to Apache Arrow Table
    table = pa.Table.from_pylist(records)

    # Write Parquet file to local disk
    local_parquet_file = 'irs990_data.parquet'
    pq.write_table(table, local_parquet_file)

    # Upload Parquet file to S3
    s3_client = boto3.client('s3')
    s3_key = f'{s3_folder}/irs990_data.parquet'
    try:
        s3_client.upload_file(local_parquet_file, s3_bucket, s3_key)
        logger.info(f'Parquet file uploaded to s3://{s3_bucket}/{s3_key}')
    except botocore.exceptions.ClientError as e:
        logger.error(f'Error uploading file to S3: {e}')
    finally:
        # Clean up local Parquet file
        if os.path.exists(local_parquet_file):
            os.remove(local_parquet_file)

def analyze_field_coverage(records):
    total_records = len(records)
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
def main():
    url = 'https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_01A.zip'
    xml_files = download_and_extract_xml_files(url)
    records = process_xml_files(xml_files)
    logger.info(f"Total files processed: {len(xml_files)}")
    logger.info(f"Files containing Georgia nonprofits: {len(set(r.get('_source_file', '') for r in records))}")
    
    
    # Log overall statistics
    form_types = [r['FormType'] for r in records]
    logger.info(f"Form type distribution: {dict(Counter(form_types))}")
    
    # Log TotalNetAssets statistics
    total_net_assets = [r.get('TotalNetAssets') for r in records if 'TotalNetAssets' in r]
    logger.info(f"TotalNetAssets: found in {len(total_net_assets)}/{len(records)} records")
    valid_net_assets = [x for x in total_net_assets if x is not None and isinstance(x, (int, float))]
    if valid_net_assets:
        logger.info(f"TotalNetAssets: min={min(valid_net_assets)}, max={max(valid_net_assets)}, avg={sum(valid_net_assets)/len(valid_net_assets)}")
    else:
        logger.warning("No valid TotalNetAssets values found")

    # Log MissionStatement statistics
    mission_statements = [r.get('MissionStatement') for r in records if 'MissionStatement' in r]
    logger.info(f"MissionStatement: found in {len(mission_statements)}/{len(records)} records")
    if mission_statements:
        valid_statements = [ms for ms in mission_statements if ms]
        if valid_statements:
            avg_length = sum(len(ms) for ms in valid_statements) / len(valid_statements)
            logger.info(f"Average MissionStatement length: {avg_length:.2f} characters")
        else:
            logger.warning("No valid MissionStatement values found")

    # Log TotalAssets statistics
    total_assets = [r.get('TotalAssets') for r in records if 'TotalAssets' in r]
    logger.info(f"TotalAssets: found in {len(total_assets)}/{len(records)} records")
    valid_assets = [x for x in total_assets if x is not None and isinstance(x, (int, float))]
    if valid_assets:
        logger.info(f"TotalAssets: min={min(valid_assets)}, max={max(valid_assets)}, avg={sum(valid_assets)/len(valid_assets)}")
    else:
        logger.warning("No valid TotalAssets values found")
    
    # Log TotalRevenue statistics
    total_revenue = [r.get('TotalRevenue') for r in records if 'TotalRevenue' in r]
    logger.info(f"TotalRevenue: found in {len(total_revenue)}/{len(records)} records")
    valid_revenue = [x for x in total_revenue if x is not None and isinstance(x, (int, float))]
    if valid_revenue:
        logger.info(f"TotalRevenue: min={min(valid_revenue)}, max={max(valid_revenue)}, avg={sum(valid_revenue)/len(valid_revenue)}")
    else:
        logger.warning("No valid TotalRevenue values found")

    # Log TotalExpenses statistics
    total_expenses = [r.get('TotalExpenses') for r in records if 'TotalExpenses' in r]
    logger.info(f"TotalExpenses: found in {len(total_expenses)}/{len(records)} records")
    valid_expenses = [x for x in total_expenses if x is not None and isinstance(x, (int, float))]
    if valid_expenses:
        logger.info(f"TotalExpenses: min={min(valid_expenses)}, max={max(valid_expenses)}, avg={sum(valid_expenses)/len(valid_expenses)}")
    else:
        logger.warning("No valid TotalExpenses values found")
    
    analyze_field_coverage(records)
    analyze_path_usage(records)
    save_to_s3_parquet(records, s3_bucket, s3_folder)

if __name__ == '__main__':
    main()