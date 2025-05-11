"""
Real-time ingestion module that asynchronously fetches NOAA weather data and USDA yield data,
saves results in InfluxDB, using aiohttp, asyncio, and environment variables.
"""

import os
import asyncio
import logging
from typing import Any, Dict, List

import aiohttp
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
NOAA_TOKEN: str = os.getenv("NOAA_TOKEN", "")
USDA_API_KEY: str = os.getenv("USDA_API_KEY", "")
INFLUXDB_URL: str = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN: str = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG: str = os.getenv("INFLUXDB_ORG", "")
INFLUXDB_BUCKET: str = os.getenv("INFLUXDB_BUCKET", "")


async def fetch_noaa_weather(
    session: aiohttp.ClientSession, start_date: str, end_date: str, station_id: str
) -> List[Dict[str, Any]]:
    """
    Fetch NOAA weather data asynchronously.
    Returns a list of result dicts.
    """
    url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    params = {
        "datasetid": "GHCND",
        "startdate": start_date,
        "enddate": end_date,
        "stationid": station_id,
        "limit": 1000,
    }
    headers = {"token": NOAA_TOKEN}
    async with session.get(url, headers=headers, params=params) as resp:
        resp.raise_for_status()
        data = await resp.json()
        results = data.get("results", [])
        logger.info("Fetched %d NOAA weather records", len(results))
        return results


async def fetch_usda_yield(
    session: aiohttp.ClientSession, year: int, commodity_desc: str = "CORN"
) -> List[Dict[str, Any]]:
    """
    Fetch USDA yield data asynchronously using QuickStats API.
    Returns a list of result dicts.
    """
    url = "https://quickstats.nass.usda.gov/api/api_GET/"
    params = {
        "key": USDA_API_KEY,
        "year": year,
        "commodity_desc": commodity_desc,
        "agg_level_desc": "STATE",
        "state_name": "MISSOURI",
    }
    async with session.get(url, params=params) as resp:
        resp.raise_for_status()
        data = await resp.json()
        results = data.get("data", [])
        logger.info("Fetched %d USDA yield records", len(results))
        return results


def write_point_to_influx(
    client: InfluxDBClient, bucket: str, org: str, point: Point
) -> None:
    """
    Write a single point to InfluxDB synchronously.
    """
    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=bucket, org=org, record=point)


async def ingest_data(
    start_date: str, end_date: str, station_id: str, year: int
) -> None:
    """
    Ingest NOAA weather and USDA yield data into InfluxDB.
    """
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    async with aiohttp.ClientSession() as session:
        # Ingest NOAA weather
        weather_records = await fetch_noaa_weather(session, start_date, end_date, station_id)
        for record in weather_records:
            dtype = record.get("datatype")
            value = record.get("value")
            point = (
                Point("noaa_weather")
                .tag("station", station_id)
                .tag("datatype", dtype or "")
                .field("value", float(value) if value is not None else 0.0)
                .time(record.get("date"))
            )
            await asyncio.get_running_loop().run_in_executor(
                None,
                write_point_to_influx,
                client,
                INFLUXDB_BUCKET,
                INFLUXDB_ORG,
                point,
            )
        # Ingest USDA yield
        yield_records = await fetch_usda_yield(session, year)
        for rec in yield_records:
            value = rec.get("Value")
            tags = {"commodity": rec.get("commodity_desc", ""), "state": rec.get("state_name", "")}
            point = (
                Point("usda_yield")
                .tag("commodity", tags["commodity"])
                .tag("state", tags["state"])
                .field("yield", float(value) if value is not None else 0.0)
                .time(f"{year}-01-01T00:00:00Z")
            )
            await asyncio.get_running_loop().run_in_executor(
                None,
                write_point_to_influx,
                client,
                INFLUXDB_BUCKET,
                INFLUXDB_ORG,
                point,
            )
    logger.info("Data ingestion completed.")


def parse_args() -> Any:
    import argparse

    parser = argparse.ArgumentParser(description="Live data ingestion for NOAA and USDA yield.")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--station-id", required=True, help="NOAA station ID")
    parser.add_argument("--year", type=int, required=True, help="Year for USDA yield data")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    await ingest_data(args.start_date, args.end_date, args.station_id, args.year)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception("Error during ingestion: %s", e)