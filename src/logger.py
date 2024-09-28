# logger.py

import logging
from logging.handlers import RotatingFileHandler

# Configure logging
log_filename = 'health.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        RotatingFileHandler(log_filename, maxBytes=10000000, backupCount=5),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)
