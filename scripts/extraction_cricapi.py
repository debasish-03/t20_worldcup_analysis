import requests
import os
import yaml
import logging
import datetime

from dotenv import load_dotenv



log_directory = os.path.join(os.path.dirname(__file__), '../logs')
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

load_dotenv()


# Load Configuration
config_path = os.path.join(os.path.dirname(__file__), '../configs/config.yaml')
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

API_KEY = os.getenv('CRICAPI_KEY')
BASE_URL = config['cricapi']['base_url']



def series_search(key: str, offset: int = 0, api_key: str = API_KEY) -> dict:
    """
    Fetches series data from the API based on the provided search key and offset.

    Args:
        key (str): The search keyword for fetching series data.
        offset (int, optional): The offset for pagination. Defaults to 0.
        api_key (str, optional): The API key for authentication. Defaults to the global variable API_KEY.

    Returns:
        dict: The JSON response from the API containing the series data.

    Raises:
        Exception: If there is an error in fetching the data, it logs the error and raises an exception.
    """

    try:
        url = f'{BASE_URL}/series'
        params = {
            'apikey': api_key,
            'offset': offset,
            'search': key
        }

        response = requests.get(url=url, params=params)
        response.raise_for_status()
        logger.info(f'Series data for key={key} fetched successfully. ')
        return response.json()
    except Exception as e:
        logger.error(f'Error fetching data for key={key} \n Error: {e}')
        raise

def series_info(id: str, offset: int = 0, api_key: str = API_KEY):
    """
    Fetches detailed series information from the API based on the provided series ID and offset.

    Args:
        id (str): The series ID for fetching detailed series information.
        offset (int, optional): The offset for pagination. Defaults to 0.
        api_key (str, optional): The API key for authentication. Defaults to the global variable API_KEY.

    Returns:
        dict: The JSON response from the API containing the detailed series information.

    Raises:
        Exception: If there is an error in fetching the data, it logs the error and raises an exception.
    """
    try:
        url = f'{BASE_URL}/series_info'
        params = {
            'apikey': api_key,
            'offset': offset,
            'id': id
        }

        response = requests.get(url=url, params=params)
        response.raise_for_status()
        logger.info(f'Series data for id={id} fetched successfully. ')
        return response.json()
    except Exception as e:
        logger.error(f'Error fetching data for id={id} \n Error: {e}')
        raise

def match_info(id: str, offset: int = 0, api_key: str = API_KEY):
    """
    Fetches detailed match information from the API based on the provided match ID and offset.

    Args:
        id (str): The match ID for fetching detailed match information.
        offset (int, optional): The offset for pagination. Defaults to 0.
        api_key (str, optional): The API key for authentication. Defaults to the global variable API_KEY.

    Returns:
        dict: The JSON response from the API containing the detailed series information.

    Raises:
        Exception: If there is an error in fetching the data, it logs the error and raises an exception.
    """
    try:
        url = f'{BASE_URL}/match_info'
        params = {
            'apikey': api_key,
            'offset': offset,
            'id': id
        }

        response = requests.get(url=url, params=params)
        response.raise_for_status()
        logger.info(f'Match info for id={id} fetched successfully. ')
        return response.json()
    except Exception as e:
        logger.error(f'Error fetching Match info for id={id} \n Error: {e}')
        raise

def player_info(id: str, offset: int = 0, api_key: str = API_KEY):
    """
    Fetches detailed player information from the API based on the provided player ID and offset.

    Args:
        id (str): The player ID for fetching detailed match information.
        offset (int, optional): The offset for pagination. Defaults to 0.
        api_key (str, optional): The API key for authentication. Defaults to the global variable API_KEY.

    Returns:
        dict: The JSON response from the API containing the detailed series information.

    Raises:
        Exception: If there is an error in fetching the data, it logs the error and raises an exception.
    """
    try:
        url = f'{BASE_URL}/players_info'
        params = {
            'apikey': api_key,
            'offset': offset,
            'id': id
        }

        response = requests.get(url=url, params=params)
        response.raise_for_status()
        logger.info(f'Player info for player id={id} fetched successfully. ')
        return response.json()
    except Exception as e:
        logger.error(f'Error fetching Player info for player id={id} \n Error: {e}')
        raise




if __name__ == '__main__':
    pass

    # series_search_res = series_search(key='T20 Worl Cup')
    # logger.info(f'Series search: {series_search_res}')

    # series_info_res = series_info(id='d13235de-1bd4-4e5e-87e8-766c21f11661')
    # logger.info(f'Series Info: {series_info_res}')

    # match_res = match_info(id='410cc5ca-f577-48e8-ac1c-c240741ad3ea')
    # logger.info(f'Match Info: {match_res}')

    # player_res = player_info(id='6055cb49-9fbe-40b8-8d16-35dfb5fc1dcc')
    # logger.info(f'Player Info: {player_res}')