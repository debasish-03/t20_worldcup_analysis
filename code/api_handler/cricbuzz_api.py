from dotenv import load_dotenv
import os
import requests


import sys
sys.path.append('/opt/airflow/code/api_handler')
sys.path.append('/opt/airflow/code/utils')

from logger_utils import logging


load_dotenv()     


class CricBuzzHandler:
    def __init__(self):
        self.base_url = "https://cricbuzz-cricket.p.rapidapi.com"
        self.headers = {
            'x-rapidapi-host': 'cricbuzz-cricket.p.rapidapi.com',
            'x-rapidapi-key': os.getenv('CRICBUZZ_API_KEY')
        }
    
    def get_series_matches(self, series_id):
        url = f"{self.base_url}/series/v1/{series_id}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def get_match_info(self, match_id):
        url = f"{self.base_url}/mcenter/v1/{match_id}"
        response = requests.get(url, headers=self.headers)
        logging.info(f'matchId: {match_id}, response: {response.status_code}')
        response.raise_for_status()
        return response.json()
    
    def get_series_stat_filters(self, series_id):
        url = f"{self.base_url}/stats/v1/series/{series_id}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def get_series_stat(self, series_id, filter_type):
        url = f"{self.base_url}/stats/v1/series/{series_id}"
        querystring = {"statsType": f"{filter_type}"}
        response = requests.get(url, headers=self.headers, params=querystring)
        response.raise_for_status()
        return response.json()

