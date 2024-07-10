import logging
import datetime
import os

log_directory = os.path.join(os.path.dirname(__file__), '../../logs')
log_filename = f'log_{datetime.date.today()}.log'
log_filepath = os.path.join(log_directory, log_filename)

# Create the log directory if it does not exist
os.makedirs(log_directory, exist_ok=True)

logging.basicConfig(
    filename=log_filepath, 
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)