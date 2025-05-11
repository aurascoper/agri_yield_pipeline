#!/usr/bin/env python3
"""
Real-time ingestor for NOAA weather and USDA yield data.
Fetches data asynchronously using aiohttp, with retries and exponential backoff,
and writes records to InfluxDB v2.
"""

import os
import asyncio
import logging
from typing import Any, Dict, List, Optional

import aiohttp
from aiohttp import ClientResponseError, ClientError
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment
NOAA_API_URL: str = os.getenv("NOAA_API_URL", "https://www.ncdc.noaa.gov/cdo-web/api/v2/data")
USDA_API_URL: str = os.getenv("USDA_API_URL", "https://quickstats.nass.usda.gov/api/api_GET/")
NOAA_TOKEN: str = os.getenv("NOAA_API_TOKEN", "")
USDA_API_KEY: str = os.getenv("USDA_API_KEY", "")
INFLUXDB_URL: str = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN: str = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG: str = os.getenv("INFLUXDB_ORG", "")
INFLUXDB_BUCKET: str = os.getenv("INFLUXDB_BUCKET", "")

if not NOAA_TOKEN:
    logger.error("NOAA_API_TOKEN is not set")
    exit(1)
if not USDA_API_KEY:
    logger.error("USDA_API_KEY is not set")
    exit(1)
if not INFLUXDB_TOKEN:
    logger.error("INFLUXDB_TOKEN is not set")
    exit(1)
if not INFLUXDB_ORG:
    logger.error("INFLUXDB_ORG is not set")
    exit(1)
if not INFLUXDB_BUCKET:
    logger.error("INFLUXDB_BUCKET is not set")
    exit(1)

# Defaults and limits
NOAA_DATASET_ID: str = os.getenv("NOAA_DATASET_ID", "GHCND")
NOAA_LIMIT: int = int(os.getenv("NOAA_LIMIT", "1000"))
DEFAULT_COMMODITY: str = os.getenv("COMMODITY_DESC", "CORN")
DEFAULT_STATE: str = os.getenv("STATE_NAME", "MISSOURI")

async def fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    retries: int = 3,
    backoff_factor: float = 1.0,
) -> Dict[str, Any]:
    """
    Perform GET request with retries and exponential backoff.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, headers=headers, params=params, timeout=10) as resp:
                resp.raise_for_status()
                return await resp.json()
        except (ClientResponseError, ClientError, asyncio.TimeoutError) as e:
            last_exc = e
            wait = backoff_factor * 2 ** (attempt - 1)
            logger.warning(
                "Request failed [%s] attempt %d/%d: %s. Retrying in %.1f seconds...",
                url, attempt, retries, e, wait,
            )
            await asyncio.sleep(wait)
    logger.error("Failed to fetch %s after %d attempts", url, retries)
    raise last_exc  # type: ignore

async def fetch_noaa_weather(
    session: aiohttp.ClientSession,
    start_date: str,
    end_date: str,
    station_id: str,
) -> List[Dict[str, Any]]:
    """
    Fetch NOAA weather records for a station and date range.
    """
    params = {
        "datasetid": NOAA_DATASET_ID,
        "startdate": start_date,
        "enddate": end_date,
        "stationid": station_id,
        "limit": NOAA_LIMIT,
    }
    headers = {"token": NOAA_TOKEN}
    data = await fetch_with_retry(session, NOAA_API_URL, headers=headers, params=params)
    return data.get("results", [])

async def fetch_usda_yield(
    session: aiohttp.ClientSession,
    year: int,
    commodity_desc: str = DEFAULT_COMMODITY,
    agg_level_desc: str = "STATE",
    state_name: str = DEFAULT_STATE,
) -> List[Dict[str, Any]]:
    """
    Fetch USDA Quick Stats yield data for a given year.
    """
    params = {
        "key": USDA_API_KEY,
        "year": year,
        "commodity_desc": commodity_desc,
        "agg_level_desc": agg_level_desc,
        "state_name": state_name,
    }
    data = await fetch_with_retry(session, USDA_API_URL, params=params)
    return data.get("data", [])

async def write_weather_to_influx(
    client: InfluxDBClient,
    records: List[Dict[str, Any]],
) -> None:
    """
    Write NOAA weather records to InfluxDB.
    """
    write_api = client.write_api(write_options=SYNCHRONOUS)
    for rec in records:
        station = rec.get("station")
        datatype = rec.get("datatype")
        date = rec.get("date")
        value = rec.get("value")
        if station is None or datatype is None or date is None or value is None:
            logger.warning("Skipping invalid NOAA record: %s", rec)
            continue
        try:
            point = (
                Point("noaa_weather")
                .tag("station", station)
                .tag("datatype", datatype)
                .field("value", float(value))
                .time(date, WritePrecision.NS)
            )
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        except Exception as e:
            logger.exception("Failed to write NOAA record to InfluxDB: %s, error: %s", rec, e)

async def write_yield_to_influx(
    client: InfluxDBClient,
    records: List[Dict[str, Any]],
) -> None:
    """
    Write USDA yield records to InfluxDB.
    """
    write_api = client.write_api(write_options=SYNCHRONOUS)
    for rec in records:
        raw_value = rec.get("Value") or rec.get("value")
        commodity = rec.get("commodity_desc") or rec.get("commodity")
        state = rec.get("state_name") or rec.get("state")
        year = rec.get("year") or rec.get("Year")
        if raw_value is None or commodity is None or state is None or year is None:
            logger.warning("Skipping invalid USDA record: %s", rec)
            continue
        try:
            fv = float(raw_value)
            time_str = f"{int(year)}-01-01T00:00:00Z"
            point = (
                Point("usda_yield")
                .tag("commodity", commodity)
                .tag("state", state)
                .field("yield", fv)
                .time(time_str, WritePrecision.NS)
            )
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        except Exception as e:
            logger.exception("Failed to write USDA record to InfluxDB: %s, error: %s", rec, e)

async def ingest(
    start_date: str,
    end_date: str,
    station_id: str,
    year: int,
) -> None:
    """
    Orchestrate fetch and write operations for weather and yield data.
    """
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    try:
        async with aiohttp.ClientSession() as session:
            weather_records = await fetch_noaa_weather(session, start_date, end_date, station_id)
            logger.info("Fetched %d NOAA records", len(weather_records))
            await write_weather_to_influx(client, weather_records)
            yield_records = await fetch_usda_yield(session, year)
            logger.info("Fetched %d USDA yield records", len(yield_records))
            await write_yield_to_influx(client, yield_records)
    finally:
        client.close()

def parse_args() -> Any:
    import argparse
    parser = argparse.ArgumentParser(
        description="Live data ingestor for NOAA weather and USDA yield"
    )
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--station-id", required=True, help="NOAA station ID")
    parser.add_argument("--year", type=int, required=True, help="Year for USDA yield data")
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    asyncio.run(ingest(args.start_date, args.end_date, args.station_id, args.year))

if __name__ == "__main__":
    main()