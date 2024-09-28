# s3_utils.py

from config import s3_client, S3_BUCKET
from logger import logger
import botocore.exceptions

def upload_file_to_s3(file_content, s3_key):
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=file_content)
        logger.info(f'Uploaded file to s3://{S3_BUCKET}/{s3_key}')
    except botocore.exceptions.ClientError as e:
        logger.error(f'Error uploading file to S3: {e}')
