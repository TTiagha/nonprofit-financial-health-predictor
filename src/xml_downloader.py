# xml_downloader.py

import requests
import zipfile
import io
from logger import logger

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
