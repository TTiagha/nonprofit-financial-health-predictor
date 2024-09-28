# logger.py

import logging
import watchtower
import boto3

class CloudWatchFilter(logging.Filter):
    def filter(self, record):
        allowed_messages = [
            "Processed",
            "Files without TotalRevenue:",
            "Files without TotalExpenses:",
            "Files without TotalAssets:",
            "Files without TotalNetAssets:",
            "Average fields per record:",
            "Files processed from this URL:",
            "Form type distribution:",
            "TotalNetAssets: found in",
            "TotalNetAssets: min=",
            "MissionStatement: found in",
            "Average MissionStatement length:",
            "TotalAssets: found in",
            "TotalAssets: min=",
            "TotalRevenue: found in",
            "TotalRevenue: min=",
            "TotalExpenses: found in",
            "TotalExpenses: min=",
            "Field coverage analysis:",
            "Converting records to Parquet format.",
            "Uploaded file to s3://"
        ]
        return any(record.message.startswith(msg) for msg in allowed_messages)

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.StreamHandler()
                    ])

# Create CloudWatch handler
cloudwatch_handler = watchtower.CloudWatchLogHandler(
    log_group="NonprofitFinancialHealthPredictor",
    stream_name="ApplicationLogs",
    boto3_session=boto3.Session()
)

# Add CloudWatch filter
cloudwatch_handler.addFilter(CloudWatchFilter())

# Get the root logger and add the CloudWatch handler
root_logger = logging.getLogger()
root_logger.addHandler(cloudwatch_handler)

logger = logging.getLogger(__name__)
