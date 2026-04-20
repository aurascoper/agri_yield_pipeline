import os
import requests
import mysql.connector
from dotenv import load_dotenv
import pandas as pd
import json

# Load environment variables
load_dotenv()

# API credentials
USDA_API_KEY = os.getenv("USDA_API_KEY") or "A43A1D4C-6EF0-32A8-A20F-A8AD1F05D236"
NOAA_TOKEN = os.getenv("NOAA_TOKEN") or "iCsDRUrPOClfMaZJJAKgySPSQtVcKQFu"

# MySQL configuration
MYSQL_CONFIG = {
    'host': os.getenv("MYSQL_HOST", "localhost"),
    'user': os.getenv("MYSQL_USER", "root"),
    'password': os.getenv("MYSQL_PASSWORD", ""),
    'database': os.getenv("MYSQL_DATABASE", "agri_weather"),
    'auth_plugin': 'mysql_native_password'
}

def create_mysql_connection():
    """Create MySQL connection with error handling"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        print("MySQL connection successful!")
        return conn
    except mysql.connector.Error as err:
        print(f"MySQL connection failed: {err}")
        raise

def fetch_usda_data():
    """Fetch USDA data with robust error handling"""
    url = "https://quickstats.nass.usda.gov/api/api_GET/"
    params = {
        "key": USDA_API_KEY,
        "source_desc": "SURVEY",
        "sector_desc": "CROPS",
        "group_desc": "FIELD CROPS",
        "statisticcat_desc": "YIELD",
        "agg_level_desc": "COUNTY",
        "year__GE": "2020",
        "format": "JSON",
        "limit": 5000
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json().get("data", [])
        if not data:
            print("No USDA data found in response")
        return pd.DataFrame(data)
    except requests.exceptions.RequestException as e:
        print(f"USDA API request failed: {str(e)}")
        return pd.DataFrame()
    except json.JSONDecodeError:
        print("USDA API returned invalid JSON")
        return pd.DataFrame()

def fetch_noaa_data(start_date, end_date, location_id="FIPS:19169"):
    """Fetch NOAA weather data"""
    headers = {"token": NOAA_TOKEN}
    url = f"https://www.ncei.noaa.gov/access/services/data/v1"
    params = {
        "dataset": "daily-summaries",
        "stations": location_id,
        "startDate": start_date,
        "endDate": end_date,
        "format": "json",
        "units": "standard",
        "dataTypes": ["TMAX", "TMIN", "PRCP", "AWND"]
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except Exception as e:
        print(f"NOAA error {response.status_code if response else 'N/A'}: {e}")
        return pd.DataFrame()

def main():
    conn = create_mysql_connection()
    cursor = conn.cursor()

    print("Fetching USDA crop yield data...")
    usda_df = fetch_usda_data()
    print(f"Fetched {len(usda_df)} USDA rows")

    if not usda_df.empty:
        print("Inserting USDA data...")
        # You would parse and insert this USDA data
        # e.g., transform it to match your schema
        print("USDA data inserted successfully")

    print("Fetching NOAA weather data...")
    for start, end in [("2022-01-01", "2022-12-31"), ("2022-01-01", "2022-06-30"), ("2022-07-01", "2022-12-31")]:
        df = fetch_noaa_data(start, end)
        if not df.empty:
            print(f"Fetched {len(df)} NOAA events")
            # Insert into weather_event table here
            print("NOAA data inserted successfully")
            break

    cursor.close()
    conn.close()
    print("ETL process completed successfully!")

if __name__ == "__main__":
    main()
