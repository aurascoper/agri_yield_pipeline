"""
Configuration for the agricultural yield pipeline, including API keys and parameters.
"""

# NOAA API token
NOAA_TOKEN = 'YOUR_NOAA_API_TOKEN'

# NOAA dataset and station
NOAA_DATASET_ID = 'GHCND'
NOAA_STATION_ID = 'GHCND:USW00003952'  # Example station ID in Missouri
NOAA_LIMIT = 1000

# Bounding box for Missouri [west, south, east, north]
BBOX = [-95.77, 35.0, -89.1, 40.6]

# Date range for analysis
START_DATE = '2021-01-01'
END_DATE = '2021-12-31'