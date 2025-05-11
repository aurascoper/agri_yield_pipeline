#!/usr/bin/env python3
"""
InfluxDB v2 writer for weather and yield data.
Provides methods to write data points, query, and manage retention policies.
"""

import os
import logging
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.exceptions import InfluxDBError

# Load environment variables from .env
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InfluxDBWriter:
    """
    A writer class for InfluxDB v2.4 to store weather and yield data.
    """
    def __init__(
        self,
        url: Optional[str] = None,
        token: Optional[str] = None,
        org: Optional[str] = None,
        bucket: Optional[str] = None,
    ):
        # Load from environment if not provided
        self.url = url or os.getenv("INFLUXDB_URL")
        self.token = token or os.getenv("INFLUXDB_TOKEN")
        self.org = org or os.getenv("INFLUXDB_ORG")
        self.bucket = bucket or os.getenv("INFLUXDB_BUCKET")
        if not all([self.url, self.token, self.org, self.bucket]):
            logger.error(
                "InfluxDB configuration missing. Ensure INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET are set."
            )
            raise ValueError("Incomplete InfluxDB configuration")
        try:
            self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.query_api = self.client.query_api()
            self.buckets_api = self.client.buckets_api()
        except InfluxDBError as e:
            logger.exception("Failed to initialize InfluxDB client: %s", e)
            raise

    def write_point(
        self,
        measurement: str,
        tags: Dict[str, str],
        fields: Dict[str, Any],
        time: Optional[str] = None,
    ) -> None:
        """
        Write a single point to InfluxDB.
        :param measurement: Measurement name.
        :param tags: Dictionary of tag keys and values.
        :param fields: Dictionary of field keys and values.
        :param time: Optional timestamp in RFC3339 format.
        """
        try:
            point = Point(measurement)
            for k, v in tags.items():
                point.tag(k, v)
            for k, v in fields.items():
                point.field(k, v)
            if time:
                point.time(time)
            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            logger.debug(
                "Wrote point: %s %s %s %s", measurement, tags, fields, time
            )
        except Exception as e:
            logger.exception(
                "Error writing point to InfluxDB: %s %s %s %s", measurement, tags, fields, e
            )

    def write_weather(
        self,
        station: str,
        datatype: str,
        value: float,
        time: str,
    ) -> None:
        """
        Write a weather record as an InfluxDB point.
        """
        tags = {"station": station, "datatype": datatype}
        fields = {"value": value}
        self.write_point("weather", tags, fields, time)

    def write_yield(
        self,
        commodity: str,
        state: str,
        yield_val: float,
        time: str,
    ) -> None:
        """
        Write a yield record as an InfluxDB point.
        """
        tags = {"commodity": commodity, "state": state}
        fields = {"yield": yield_val}
        self.write_point("yield", tags, fields, time)

    def query(self, flux_query: str) -> List[Dict[str, Any]]:
        """
        Execute a Flux query and return results as a list of dictionaries.
        """
        results: List[Dict[str, Any]] = []
        try:
            tables = self.query_api.query(flux_query)
            for table in tables:
                for record in table.records:
                    results.append(record.values)
        except Exception:
            logger.exception("Error querying InfluxDB with query: %s", flux_query)
        return results

    def setup_retention(self, retention_hours: int) -> None:
        """
        Create or update bucket retention policy.
        :param retention_hours: Retention period in hours.
        """
        retention_seconds = retention_hours * 3600
        try:
            bucket = self.buckets_api.find_bucket_by_name(self.bucket)
            if bucket is None:
                # Create new bucket with retention
                self.buckets_api.create_bucket(
                    bucket_name=self.bucket,
                    org=self.org,
                    retention_rules=[
                        {"type": "expire", "everySeconds": retention_seconds}
                    ],
                )
                logger.info(
                    "Created bucket '%s' with %d-hour retention.",
                    self.bucket,
                    retention_hours,
                )
            else:
                # Update existing bucket's retention
                bucket.retention_rules = [
                    {"type": "expire", "everySeconds": retention_seconds}
                ]
                self.buckets_api.update_bucket(bucket=bucket)
                logger.info(
                    "Updated bucket '%s' retention to %d hours.",
                    self.bucket,
                    retention_hours,
                )
        except Exception:
            logger.exception(
                "Error setting up retention policy for bucket '%s'", self.bucket
            )

    def close(self) -> None:
        """
        Close the InfluxDB client connection.
        """
        try:
            self.client.close()
        except Exception:
            logger.exception("Error closing InfluxDB client")