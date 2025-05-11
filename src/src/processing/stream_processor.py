"""
Kafka consumer to process incoming weather and yield streams.
Computes rolling averages, detects drought/flood events,
enriches yield data with temperature and precipitation metrics,
and serializes enriched records using Avro.
"""

import os
import sys
import json
import logging
from typing import Any, Dict, Deque, Tuple, Optional
from collections import deque
from datetime import datetime, timedelta

from confluent_kafka import Consumer, Producer, KafkaException
import avro.schema
import avro.io
import io
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_args() -> Any:
    import argparse
    parser = argparse.ArgumentParser(description="Stream processor for weather and yield data.")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--group-id", default=os.getenv("KAFKA_GROUP_ID", "agri_processor"))
    parser.add_argument("--weather-topic", default=os.getenv("KAFKA_WEATHER_TOPIC", "noaa_weather"))
    parser.add_argument("--yield-topic", default=os.getenv("KAFKA_YIELD_TOPIC", "usda_yield"))
    parser.add_argument("--output-topic", default=os.getenv("KAFKA_OUTPUT_TOPIC", "enriched_yield"))
    parser.add_argument("--station-id", default=os.getenv("TARGET_STATION_ID", ""),
                        help="Station ID to use for enrichment")
    parser.add_argument("--window-days", type=int,
                        default=int(os.getenv("ROLLING_WINDOW_DAYS", "7")),
                        help="Window size in days for rolling averages")
    parser.add_argument("--drought-threshold", type=float,
                        default=float(os.getenv("DROUGHT_THRESHOLD", "1.0")),
                        help="Precipitation threshold below which drought is signaled")
    parser.add_argument("--flood-threshold", type=float,
                        default=float(os.getenv("FLOOD_THRESHOLD", "20.0")),
                        help="Precipitation threshold above which flood is signaled")
    return parser.parse_args()

def get_station_data(weather_data: Dict[str, Dict[str, Deque[Tuple[datetime, float]]]],
                     station_id: str) -> Dict[str, Deque[Tuple[datetime, float]]]:
    """
    Retrieve deque mapping for a given station, or first available.
    """
    if station_id and station_id in weather_data:
        return weather_data[station_id]
    if weather_data:
        # fallback to any station
        return next(iter(weather_data.values()))
    return {}

def process_weather(msg: Dict[str, Any],
                    weather_data: Dict[str, Dict[str, Deque[Tuple[datetime, float]]]],
                    args: Any) -> None:
    """
    Update rolling window data and detect drought/flood based on precipitation.
    """
    station = msg.get("station")
    date_str = msg.get("date") or msg.get("time")
    datatype = msg.get("datatype")
    value = msg.get("value")
    if not station or not date_str or datatype is None or value is None:
        logger.warning("Invalid weather message skipped: %s", msg)
        return
    try:
        timestamp = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        val = float(value)
    except Exception:
        logger.exception("Error parsing weather message: %s", msg)
        return
    station_map = weather_data.setdefault(station, {})
    dq = station_map.setdefault(datatype, deque())
    dq.append((timestamp, val))
    # purge old entries
    cutoff = timestamp - timedelta(days=args.window_days)
    while dq and dq[0][0] < cutoff:
        dq.popleft()
    # compute rolling average
    avg = sum(v for _, v in dq) / len(dq) if dq else 0.0
    station_map[f"{datatype}_avg"] = avg
    # drought/flood detection for precipitation
    if datatype.upper() == "PRCP":
        if avg < args.drought_threshold:
            logger.warning("Drought detected at %s: avg_precip=%.2f", station, avg)
        elif avg > args.flood_threshold:
            logger.warning("Flood detected at %s: avg_precip=%.2f", station, avg)

def process_yield(msg: Dict[str, Any],
                  weather_data: Dict[str, Dict[str, Deque[Tuple[datetime, float]]]],
                  args: Any,
                  producer: Producer,
                  avro_schema: avro.schema.Schema) -> None:
    """
    Enrich yield record with recent weather metrics and produce Avro-serialized output.
    """
    state = msg.get("state_name") or msg.get("state")
    commodity = msg.get("commodity_desc") or msg.get("commodity")
    year = msg.get("year") or msg.get("Year")
    yield_val = msg.get("Value") or msg.get("yield") or msg.get("value")
    if not state or not commodity or year is None or yield_val is None:
        logger.warning("Invalid yield message skipped: %s", msg)
        return
    try:
        year_int = int(year)
        yield_float = float(yield_val)
    except Exception:
        logger.exception("Error parsing yield message: %s", msg)
        return
    station_map = get_station_data(weather_data, args.station_id)
    prcp_avg = station_map.get("PRCP_avg")
    tmax = station_map.get("TMAX_avg")
    tmin = station_map.get("TMIN_avg")
    temp_avg: Optional[float] = None
    if tmax is not None and tmin is not None:
        temp_avg = (tmax + tmin) / 2
    record = {
        "state_name": state,
        "commodity_desc": commodity,
        "year": year_int,
        "yield": yield_float,
        "avg_precipitation": prcp_avg,
        "avg_temperature": temp_avg,
        "event_time": datetime.utcnow().isoformat() + "Z"
    }
    # serialize with Avro
    writer = avro.io.DatumWriter(avro_schema)
    bytes_io = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_io)
    try:
        writer.write(record, encoder)
        payload = bytes_io.getvalue()
        producer.produce(args.output_topic, value=payload)
        producer.poll(0)
        logger.info("Enriched yield record produced for %s %d", state, year_int)
    except Exception:
        logger.exception("Failed to serialize/produce enriched record: %s", record)

def main() -> None:
    args = parse_args()
    # Kafka consumer configuration
    consumer_conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": "earliest"
    }
    consumer = Consumer(consumer_conf)
    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    # Prepare Avro schema
    schema_str = r"""
    {"type": "record", "name": "EnrichedYield", "fields": [
      {"name": "state_name", "type": "string"},
      {"name": "commodity_desc", "type": "string"},
      {"name": "year", "type": "int"},
      {"name": "yield", "type": "float"},
      {"name": "avg_precipitation", "type": ["null", "float"], "default": null},
      {"name": "avg_temperature", "type": ["null", "float"], "default": null},
      {"name": "event_time", "type": "string"}
    ]}
    """
    avro_schema = avro.schema.parse(schema_str)
    # State for rolling windows
    weather_data: Dict[str, Dict[str, Deque[Tuple[datetime, float]]]] = {}
    # Subscribe to topics
    consumer.subscribe([args.weather_topic, args.yield_topic])
    logger.info("Subscribed to topics: %s, %s", args.weather_topic, args.yield_topic)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue
            try:
                payload = msg.value()
                msg_dict = json.loads(payload.decode('utf-8') if isinstance(payload, bytes) else payload)
            except Exception:
                logger.exception("Failed to decode message: %s", msg)
                continue
            topic = msg.topic()
            if topic == args.weather_topic:
                process_weather(msg_dict, weather_data, args)
            elif topic == args.yield_topic:
                process_yield(msg_dict, weather_data, args, producer, avro_schema)
    except KeyboardInterrupt:
        logger.info("Interrupted, shutting down...")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    main()