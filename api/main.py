import mysql.connector
from mysql.connector import errorcode
from pydantic import BaseModel, AfterValidator
from typing import List, Dict, Annotated, Optional
import requests
from dotenv import load_dotenv
import os
import time
import logging
from collections import Counter
from weather_station_map import MISSOURI_STATION_LOCATION_MAP

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(dotenv_path="/Users/aurascoper/agri_yield_pipeline/api/.env")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
NOAA_API_KEY = os.getenv("NOAA_API_KEY")
USDA_API_KEY = os.getenv("USDA_API_KEY")

# ------------------------------
# Validators and Models
# ------------------------------
def is_short(value: str) -> str:
    if len(value) > 448:
        raise ValueError(f"{value} is too long!")
    return value

ShortDesc = Annotated[str, AfterValidator(is_short)]

class WeatherEvent(BaseModel):
    weather_id: int
    station_id: int
    date: str
    temperature_max: int | None = None
    temperature_min: int | None = None
    precipitation: str | None = None
    wind_speed: int | None = None

class CropYieldRecord(BaseModel):
    crop_id: int
    season_id: int
    location_id: int
    yield_amount: float

# ------------------------------
# Database Initialization
# ------------------------------
def initialize_database(sql_path: str):
    logger.info("Initializing database...")
    with open(sql_path, "r") as f:
        sql_script = f.read()

    statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
    conn = None

    try:
        conn = mysql.connector.connect(user=DB_USER, password=DB_PASS, host=DB_HOST)
        cursor = conn.cursor()
        for stmt in statements:
            try:
                cursor.execute(stmt)
                if cursor.with_rows:
                    cursor.fetchall()
            except mysql.connector.Error as e:
                logger.warning(f"⚠️ Failed SQL: {stmt[:60]}...\n{e}")
                continue
        conn.commit()
        cursor.close()
        logger.info("✅ Database initialized from SQL script.")
    except mysql.connector.Error as e:
        logger.error(f"❌ Database initialization failed: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

# ------------------------------
# Helper Functions
# ------------------------------
def get_next_yield_id() -> int:
    """Get the next available yield_id from the database"""
    try:
        conn = mysql.connector.connect(user=DB_USER, password=DB_PASS, host=DB_HOST, database=DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT COALESCE(MAX(yield_id), 0) + 1 FROM crop_yield")
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else 1
    except mysql.connector.Error as e:
        logger.error(f"❌ Failed to get next yield_id: {e}")
        return 1
    finally:
        if conn.is_connected():
            conn.close()

def get_county_location_mapping() -> Dict[str, int]:
    """Get mapping of county names to location_ids from database"""
    try:
        conn = mysql.connector.connect(user=DB_USER, password=DB_PASS, host=DB_HOST, database=DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT county_name, location_id FROM location WHERE state = 'Missouri'")
        results = cursor.fetchall()
        cursor.close()
        return {county.upper(): loc_id for county, loc_id in results}
    except mysql.connector.Error as e:
        logger.error(f"❌ Failed to get county mapping: {e}")
        return {}
    finally:
        if conn.is_connected():
            conn.close()

def get_crop_mapping() -> Dict[str, int]:
    """Get mapping of crop names to crop_ids from database"""
    try:
        conn = mysql.connector.connect(user=DB_USER, password=DB_PASS, host=DB_HOST, database=DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT name, crop_id FROM crop")
        results = cursor.fetchall()
        cursor.close()
        return {crop.upper(): crop_id for crop, crop_id in results}
    except mysql.connector.Error as e:
        logger.error(f"❌ Failed to get crop mapping: {e}")
        return {}
    finally:
        if conn.is_connected():
            conn.close()

# ------------------------------
# Optimized NOAA Weather ETL
# ------------------------------
def extract_noaa_events(start_date: str, end_date: str, station_codes: List[str], token: str) -> List[Dict]:
    """Extract weather events from NOAA API with pagination and retry logic"""
    base_url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
    headers = {"token": token}
    results = []
    max_retries = 3
    retry_delay = 5  # seconds
    
    logger.info(f"🌐 Extracting NOAA data for {len(station_codes)} stations from {start_date} to {end_date}")
    
    for i, sid in enumerate(station_codes):
        logger.info(f"📡 [{i+1}/{len(station_codes)}] Processing station {sid}")
        
        offset = 1
        limit = 1000
        total_records = None
        records_retrieved = 0
        
        while total_records is None or records_retrieved < total_records:
            params = {
                "datasetid": "GHCND",
                "datatypeid": ["TMAX", "TMIN", "PRCP", "AWND", "WSF2", "WSF5"],
                "startdate": start_date,
                "enddate": end_date,
                "stationid": f"GHCND:{sid}",
                "limit": limit,
                "offset": offset,
                "units": "standard",
                "format": "json"
            }
            
            for attempt in range(max_retries):
                try:
                    resp = requests.get(base_url, headers=headers, params=params, timeout=30)
                    logger.debug(f"   Request: offset={offset}, status={resp.status_code}")
                    
                    if resp.status_code == 200:
                        data = resp.json()
                        
                        # First request: capture metadata
                        if total_records is None:
                            total_records = data.get("metadata", {}).get("resultset", {}).get("count", 0)
                            logger.info(f"   📊 Total records available: {total_records}")
                        
                        station_results = data.get("results", [])
                        results.extend(station_results)
                        records_retrieved += len(station_results)
                        
                        if station_results:
                            logger.debug(f"   ✅ Retrieved {len(station_results)} records")
                        
                        # Break retry loop on success
                        break
                    
                    elif resp.status_code == 429:
                        sleep_time = retry_delay * (attempt + 1)
                        logger.warning(f"   ⏸️ Rate limited, waiting {sleep_time} seconds...")
                        time.sleep(sleep_time)
                        continue
                    
                    else:
                        logger.warning(f"   ⚠️ Failed with status {resp.status_code}: {resp.text[:100]}")
                        break
                        
                except requests.exceptions.RequestException as e:
                    logger.error(f"   ❌ Request failed (attempt {attempt+1}): {e}")
                    time.sleep(retry_delay)
            
            # Prepare for next page
            offset += limit
            if records_retrieved >= total_records:
                break
                
        logger.info(f"   🎯 Retrieved {records_retrieved} records for station {sid}")
        time.sleep(0.1)  # 100ms between stations
    
    logger.info(f"🎯 Total NOAA records retrieved: {len(results)}")
    return results

# ------------------------------
# Enhanced USDA Crop Yield ETL
# ------------------------------
def extract_crop_yields_from_usda(year: str, state_fips: str = "29") -> List[Dict]:
    """Extract real crop yield data from USDA NASS API"""
    if not USDA_API_KEY:
        logger.error("❌ USDA_API_KEY not found in environment variables")
        return []
        
    base_url = "http://quickstats.nass.usda.gov/api/api_GET/"
    
    # Major crops to extract data for
    crops_to_extract = [
        "CORN",
        "SOYBEANS", 
        "WHEAT",
        "COTTON"
    ]
    
    all_yield_data = []
    county_mapping = get_county_location_mapping()
    crop_mapping = get_crop_mapping()
    
    logger.info(f"🌾 Extracting USDA crop yield data for Missouri ({state_fips}) in {year}")
    logger.info(f"📍 Found {len(county_mapping)} counties in database")
    logger.info(f"🌱 Found {len(crop_mapping)} crops in database")
    
    for crop in crops_to_extract:
        logger.info(f"\n🔍 Extracting {crop} data...")
        
        params = {
            "key": USDA_API_KEY,
            "source_desc": "SURVEY",
            "sector_desc": "CROPS",
            "group_desc": "FIELD CROPS",
            "commodity_desc": crop,
            "statisticcat_desc": "YIELD",
            "unit_desc": "BU / ACRE" if crop in ["CORN", "SOYBEANS", "WHEAT"] else "LB / ACRE",
            "state_fips_code": state_fips,  # Missouri
            "year": year,
            "agg_level_desc": "COUNTY",
            "format": "JSON"
        }
        
        try:
            response = requests.get(base_url, params=params, timeout=60)
            logger.info(f"   📡 USDA API Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                if "data" in data and data["data"]:
                    records = data["data"]
                    logger.info(f"   ✅ Retrieved {len(records)} {crop} records")
                    
                    for record in records:
                        try:
                            county_name = record.get("county_name", "").replace(" COUNTY", "").strip().upper()
                            
                            # Normalize special county names
                            county_name = (
                                county_name.replace("DE KALB", "DEKALB")
                                .replace("ST CLAIR", "ST. CLAIR")
                                .replace("ST CHARLES", "ST. CHARLES")
                                .replace("STE GENEVIEVE", "STE. GENEVIEVE")
                                .replace("ST FRANCOIS", "ST. FRANCOIS")
                                .replace("ST LOUIS", "ST. LOUIS")
                                .replace("OTHER COUNTIES", "")
                            )
                            
                            if not county_name:
                                continue
                                
                            yield_value = record.get("Value", "")
                            
                            # Skip records with non-numeric yields
                            if not yield_value or yield_value in ["(D)", "(Z)", "(X)", ""]:
                                continue
                                
                            # Clean yield value
                            try:
                                yield_amount = float(str(yield_value).replace(",", ""))
                            except (ValueError, TypeError):
                                logger.warning(f"   ⚠️ Skipping invalid yield value: {yield_value}")
                                continue
                            
                            # Map county name to location_id
                            if county_name in county_mapping:
                                location_id = county_mapping[county_name]
                                
                                # Map crop name to crop_id
                                crop_key = crop
                                if crop_key == "COTTON" and "UPLAND" in record.get("commodity_desc", ""):
                                    crop_key = "COTTON"
                                
                                if crop_key in crop_mapping:
                                    crop_id = crop_mapping[crop_key]
                                    
                                    yield_record = {
                                        "crop_id": crop_id,
                                        "season_id": int(year),  # Using year as season_id
                                        "location_id": location_id,
                                        "yield_amount": yield_amount,
                                        "county_name": county_name,
                                        "crop_name": crop
                                    }
                                    
                                    all_yield_data.append(yield_record)
                                else:
                                    logger.warning(f"   ⚠️ Crop {crop} not found in database mapping")
                            else:
                                logger.warning(f"   ⚠️ County {county_name} not found in database mapping")
                                
                        except Exception as e:
                            logger.error(f"   ⚠️ Error processing record: {e}")
                            continue
                            
                else:
                    logger.warning(f"   ⚠️ No data returned for {crop}")
                    
            else:
                logger.error(f"   ❌ USDA API request failed: {response.status_code} - {response.text[:200]}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"   ❌ Request failed for {crop}: {e}")
            
        # Rate limiting for USDA API
        time.sleep(1.5)  # 1.5 seconds between crop requests
    
    logger.info(f"\n🎯 Total USDA yield records extracted: {len(all_yield_data)}")
    
    # Print summary by crop
    crop_counts = Counter([record["crop_name"] for record in all_yield_data])
    for crop, count in crop_counts.items():
        logger.info(f"   {crop}: {count} county records")
    
    return all_yield_data

def load_weather_events(events: List[WeatherEvent]) -> None:
    """Load weather events into database with batch processing"""
    if not events:
        logger.warning("⚠️ No weather events to load")
        return
        
    query = """
    INSERT INTO weather_event
      (weather_id, station_id, date, temperature_max, temperature_min, precipitation, wind_speed)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      temperature_max = VALUES(temperature_max),
      temperature_min = VALUES(temperature_min),
      precipitation = VALUES(precipitation),
      wind_speed = VALUES(wind_speed);
    """
    
    data = [
        (
            ev.weather_id, ev.station_id, ev.date,
            ev.temperature_max, ev.temperature_min,
            ev.precipitation, ev.wind_speed
        )
        for ev in events
    ]
    
    try:
        conn = mysql.connector.connect(
            user=DB_USER, 
            password=DB_PASS, 
            host=DB_HOST, 
            database=DB_NAME,
            pool_size=10
        )
        cursor = conn.cursor()
        
        # Batch processing
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            cursor.executemany(query, batch)
            conn.commit()
            logger.info(f"💾 Inserted/updated batch {i//batch_size + 1} ({len(batch)} records)")
            
        cursor.close()
        logger.info(f"✅ Inserted/updated {len(data)} weather events")
    except mysql.connector.Error as e:
        logger.error(f"❌ Database insertion failed: {e}")
    finally:
        if conn.is_connected():
            conn.close()

def load_crop_yields(yield_records: List[Dict]) -> None:
    """Load crop yield records into database with batch processing"""
    if not yield_records:
        logger.warning("⚠️ No crop yield records to load")
        return
        
    # Get starting yield_id
    next_id = get_next_yield_id()
    
    query = """
    INSERT INTO crop_yield (yield_id, crop_id, season_id, location_id, yield_amount)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      yield_amount = VALUES(yield_amount);
    """
    
    data = [
        (next_id + i, rec["crop_id"], rec["season_id"], rec["location_id"], rec["yield_amount"])
        for i, rec in enumerate(yield_records)
    ]
    
    try:
        conn = mysql.connector.connect(
            user=DB_USER, 
            password=DB_PASS, 
            host=DB_HOST, 
            database=DB_NAME,
            pool_size=10
        )
        cursor = conn.cursor()
        
        # Batch processing
        batch_size = 500
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            cursor.executemany(query, batch)
            conn.commit()
            logger.info(f"💾 Inserted/updated batch {i//batch_size + 1} ({len(batch)} records)")
            
        cursor.close()
        logger.info(f"✅ Inserted/updated {len(data)} crop yield records")
    except mysql.connector.Error as e:
        logger.error(f"❌ Crop yield DB insertion failed: {e}")
    finally:
        if conn.is_connected():
            conn.close()

def transform_noaa_data(raw_weather_data: List[Dict]) -> List[WeatherEvent]:
    """Transform raw NOAA data into WeatherEvent objects"""
    logger.info(f"\n🔄 Transforming {len(raw_weather_data)} NOAA raw weather records...")
    
    # Group by station and date
    grouped_data = {}
    station_mapping = {code: sid for code, sid in MISSOURI_STATION_LOCATION_MAP.items()}
    skipped_stations = set()
    
    for rec in raw_weather_data:
        try:
            station_code = rec["station"].split(":")[-1]
            date = rec["date"][:10]
            
            # Map NOAA station code to database station_id
            if station_code in station_mapping:
                db_station_id = station_mapping[station_code]
                key = (db_station_id, date)
                
                if key not in grouped_data:
                    grouped_data[key] = WeatherEvent(
                        weather_id=hash(f"{db_station_id}-{date}") & 0x7FFFFFFF,
                        station_id=db_station_id,
                        date=date
                    )
                
                event = grouped_data[key]
                datatype = rec["datatype"]
                value = rec["value"]
                
                # Map NOAA data types to our fields
                if datatype == "TMAX":
                    event.temperature_max = int(value)
                elif datatype == "TMIN":
                    event.temperature_min = int(value)
                elif datatype == "PRCP":
                    event.precipitation = f"{value}in"
                elif datatype in ["AWND", "WSF2", "WSF5"]:  # Wind speed values
                    # Take the highest wind speed value
                    if event.wind_speed is None or value > event.wind_speed:
                        event.wind_speed = int(value)
                    
            else:
                if station_code not in skipped_stations:
                    logger.warning(f"⚠️ Station code {station_code} not found in mapping - skipping")
                    skipped_stations.add(station_code)
                
        except Exception as e:
            logger.error(f"⚠️ Failed to process NOAA record: {e}")
            continue
    
    weather_events = list(grouped_data.values())
    logger.info(f"📊 Transformed into {len(weather_events)} weather events")
    
    # Calculate summary statistics
    stats = {
        "temp_max": sum(1 for e in weather_events if e.temperature_max is not None),
        "temp_min": sum(1 for e in weather_events if e.temperature_min is not None),
        "precip": sum(1 for e in weather_events if e.precipitation is not None),
        "wind": sum(1 for e in weather_events if e.wind_speed is not None)
    }
    
    logger.info(f"   📈 Records with max temp: {stats['temp_max']}")
    logger.info(f"   📉 Records with min temp: {stats['temp_min']}")
    logger.info(f"   🌧️ Records with precipitation: {stats['precip']}")
    logger.info(f"   💨 Records with wind speed: {stats['wind']}")
    
    return weather_events

# ------------------------------
# Main ETL Pipeline
# ------------------------------
def run():
    logger.info("🚀 Starting ETL pipeline...")

    # Initialize database from schema
    logger.info("🛠️ Initializing database...")
    initialize_database("agri_weather.sql")

    # Get all Missouri station codes for comprehensive weather data
    missouri_station_codes = list(MISSOURI_STATION_LOCATION_MAP.keys())
    logger.info(f"🌡️ Found {len(missouri_station_codes)} Missouri weather stations to query")

    # PHASE 1: NOAA Weather Extraction
    logger.info("\n" + "="*60)
    logger.info("📡 PHASE 1: EXTRACTING NOAA WEATHER DATA")
    logger.info("="*60)
    
    all_raw_weather = extract_noaa_events(
        start_date="2000-01-01",
        end_date="2023-12-31",
        station_codes=missouri_station_codes,
        token=NOAA_API_KEY,
    )

    weather_events = transform_noaa_data(all_raw_weather)
    load_weather_events(weather_events)

    # PHASE 2: USDA Crop Yields Extraction
    logger.info("\n" + "="*60)
    logger.info("🌾 PHASE 2: EXTRACTING USDA CROP YIELD DATA")
    logger.info("="*60)

    raw_yields = extract_crop_yields_all_years(start_year=2000, end_year=2023, state="MISSOURI")
    transformed_yields = transform_crop_yields(raw_yields)
    load_crop_yields(transformed_yields)

    # Final Summary
    logger.info("\n" + "="*60)
    logger.info("📊 ETL PIPELINE SUMMARY")
    logger.info("="*60)
    logger.info(f"✅ Weather Events Processed: {len(weather_events)}")
    logger.info(f"✅ Crop Yield Records Processed: {len(transformed_yields)}")
    logger.info("🎉 ETL Pipeline completed successfully!")
