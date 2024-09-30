# s3_utils.py

import boto3
from config import S3_BUCKET
from logger import logger
import botocore.exceptions
from io import BytesIO

def get_s3_client():
    return boto3.client('s3')

s3_client = get_s3_client()

def upload_file_to_s3(file_content, s3_key):
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=file_content)
        logger.info(f'Uploaded file to s3://{S3_BUCKET}/{s3_key}')
    except botocore.exceptions.ClientError as e:
        logger.error(f'Error uploading file to S3: {e}')

def download_file_from_s3(s3_key):
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        file_content = response['Body'].read()
        logger.info(f'Downloaded file from s3://{S3_BUCKET}/{s3_key}')
        return file_content
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.warning(f'File not found in S3: s3://{S3_BUCKET}/{s3_key}')
            return None
        else:
            logger.error(f'Error downloading file from S3: {e}')
            raise
