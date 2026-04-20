import os
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Location, Crop, CropYield, WeatherEvent, WeatherStation, Season
from datetime import datetime

load_dotenv()
engine = create_engine(os.getenv("PG_CONN_STR"))
Session = sessionmaker(bind=engine)
session = Session()

### --- NOAA Fetch Function ---
def fetch_noaa_weather(station_id, start_date, end_date):
    token = os.getenv("NOAA_TOKEN")
    headers = {"token": token}
    url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"

    params = {
        "datasetid": "GHCND",
        "datatypeid": ["TMAX", "TMIN", "PRCP", "AWND"],
        "stationid": station_id,
        "startdate": start_date,
        "enddate": end_date,
        "units": "metric",
        "limit": 1000
    }

    response = requests.get(url, headers=headers, params=params)
    data = response.json().get("results", [])
    return data

### --- USDA Yield Fetch Function ---
def fetch_usda_yield(fips_code, crop_name, year):
    url = "https://quickstats.nass.usda.gov/api/api_GET/"
    params = {
        "key": os.getenv("USDA_API_KEY"),
        "source_desc": "SURVEY",
        "sector_desc": "CROPS",
        "group_desc": "FIELD CROPS",
        "commodity_desc": crop_name.upper(),
        "agg_level_desc": "COUNTY",
        "year": year,
        "state_alpha": "MO",
        "county_code": fips_code,
        "format": "JSON"
    }

    response = requests.get(url, params=params)
    try:
        return response.json()["data"]
    except Exception:
        return []

def get_season_id(record_date, session):
    month = record_date.month
    year = record_date.year

    if month in [3, 4, 5]:
        season_name = "Spring"
    elif month in [6, 7, 8]:
        season_name = "Summer"
    elif month in [9, 10, 11]:
        season_name = "Fall"
    else:
        season_name = "Winter"
        if month == 12:
            year = year
        else:
            year = year - 1  # Jan/Feb are part of "last year's" Winter

    season = session.query(Season).filter_by(name=season_name).first()
    if season:
        return season.season_id
    else:
        raise ValueError(f"Season '{season_name}' not found in DB")


### --- Write Weather Event ---
def load_weather_events(weather_data, station_id):
    for record in weather_data:
        if record['datatype'] == "TMAX":
            max_temp = record["value"]
        elif record['datatype'] == "TMIN":
            min_temp = record["value"]
        elif record['datatype'] == "PRCP":
            precip = record["value"]
        elif record['datatype'] == "AWND":
            wind = record["value"]
        else:
            continue

        event = WeatherEvent(
            station_id=station_id,
            date=datetime.strptime(record["date"], "%Y-%m-%dT%H:%M:%S"),
            temperature_max=max_temp if 'max_temp' in locals() else None,
            temperature_min=min_temp if 'min_temp' in locals() else None,
            precipitation=precip if 'precip' in locals() else None,
            wind_speed=wind if 'wind' in locals() else None,
        )
        session.add(event)
    session.commit()

### --- Write Crop Yields ---
def load_crop_yield(usda_data, location_id, crop_id, season_id):
    for entry in usda_data:
        if "Value" not in entry or entry["Value"] == "(D)":
            continue
        yield_entry = CropYield(
            location_id=location_id,
            crop_id=crop_id,
            season_id=season_id,
            year=int(entry["year"]),
            production=float(entry["Value"].replace(",", "")),
            yield_per_acre=float(entry["Value"].replace(",", "")),  # Simplified
        )
        session.add(yield_entry)
    session.commit()

### --- ETL Runner ---
def run_etl():
    # Example: Columbia, MO
    station_id = "GHCND:USC00230120"
    fips = "019"  # Boone County
    crop = "Corn"
    year = 2023
    start = f"{year}-03-01"
    end = f"{year}-05-31"

    # Fetch and load weather
    weather_data = fetch_noaa_weather(station_id, start, end)
    load_weather_events(weather_data, station_id)

    # Fetch and load yield
    usda_data = fetch_usda_yield(fips, crop, year)
    load_crop_yield(usda_data, location_id=1, crop_id=1, season_id=1)

if __name__ == "__main__":
    run_etl()

