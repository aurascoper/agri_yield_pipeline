"""
Fetch weather data from NOAA API.
"""

import requests
import src.config as config

def get_weather(start_date: str, end_date: str):
    """
    Fetch weather data (daily observations) from the NOAA API for the configured station.
    Returns the JSON response.
    """
    headers = {'token': config.NOAA_TOKEN}
    params = {
        'datasetid': config.NOAA_DATASET_ID,
        'startdate': start_date,
        'enddate': end_date,
        'limit': config.NOAA_LIMIT,
        'stationid': config.NOAA_STATION_ID
    }
    url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data'
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()