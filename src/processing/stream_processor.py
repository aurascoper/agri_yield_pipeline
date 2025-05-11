#!/usr/bin/env python3
"""
Kafka consumer that listens to 'weather' and 'yield' topics.
Computes rolling 5-minute soil moisture averages and detects drought
(precip < threshold for drought_days days). Enriches yield records with
weather metrics and outputs Avro-serialized records for downstream use.
"""

import os
import json
import logging
from typing import Any, Dict, Deque, Tuple, Optional
from collections import deque
from datetime import datetime, timedelta

from confluent_kafka import Consumer, Producer
import avro.schema
import avro.io
import io
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stream_processor")

def parse_args() -> Any:
    import argparse
    parser = argparse.ArgumentParser(description="Kafka stream processor for weather and yield.")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                        help="Kafka bootstrap servers")
    parser.add_argument("--group-id", default=os.getenv("KAFKA_GROUP_ID", "processor-group"),
                        help="Kafka consumer group ID")
    parser.add_argument("--weather-topic", default=os.getenv("KAFKA_WEATHER_TOPIC", "weather"),
                        help="Weather topic name")
    parser.add_argument("--yield-topic", default=os.getenv("KAFKA_YIELD_TOPIC", "yield"),
                        help="Yield topic name")
    parser.add_argument("--output-topic", default=os.getenv("KAFKA_OUTPUT_TOPIC", "enriched-yield"),
                        help="Output topic for enriched records")
    parser.add_argument("--soil-window-minutes", type=int,
                        default=int(os.getenv("SOIL_WINDOW_MINUTES", "5")),
                        help="Window for soil moisture rolling average (minutes)")
    parser.add_argument("--precip-threshold", type=float,
                        default=float(os.getenv("PRECIP_THRESHOLD", "2.0")),
                        help="Precipitation threshold (mm) for drought detection")
    parser.add_argument("--drought-days", type=int,
                        default=int(os.getenv("DROUGHT_DAYS", "3")),
                        help="Consecutive days below threshold to signal drought")
    return parser.parse_args()

def process_weather(msg: Dict[str, Any], weather_data: Dict[str, Any], args: Any) -> None:
    station = msg.get("station")
    date_str = msg.get("time") or msg.get("date")
    datatype = msg.get("datatype")
    value = msg.get("value")
    if not station or not date_str or datatype is None or value is None:
        logger.warning("Skipping invalid weather msg: %s", msg)
        return
    try:
        ts = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        val = float(value)
    except Exception:
        logger.exception("Error parsing weather msg: %s", msg)
        return
    station_map = weather_data.setdefault(station, {})
    dt = datatype.upper()

    # Soil moisture rolling average (window in minutes)
    if dt in ("SM", "SOIL_MOISTURE"):
        dq: Deque[Tuple[datetime, float]] = station_map.setdefault("SM", deque())
        dq.append((ts, val))
        cutoff = ts - timedelta(minutes=args.soil_window_minutes)
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        avg_sm = sum(v for _, v in dq) / len(dq) if dq else None
        station_map["SM_avg"] = avg_sm

    # Precipitation drought detection (daily)
    if dt == "PRCP":
        dq2: Deque[Tuple[datetime.date, float]] = station_map.setdefault("PRCP_days", deque())
        day = ts.date()
        dq2.append((day, val))
        cutoff_day = day - timedelta(days=args.drought_days - 1)
        while dq2 and dq2[0][0] < cutoff_day:
            dq2.popleft()
        days = {d for d, _ in dq2}
        if len(days) >= args.drought_days and all(v < args.precip_threshold for _, v in dq2):
            station_map["drought"] = True
            logger.warning("Drought detected at %s", station)
        else:
            station_map["drought"] = False

def process_yield(msg: Dict[str, Any], weather_data: Dict[str, Any],
                  args: Any, producer: Producer, avro_schema: avro.schema.Schema) -> None:
    state = msg.get("state_name") or msg.get("state")
    commodity = msg.get("commodity_desc") or msg.get("commodity")
    year = msg.get("year") or msg.get("Year")
    raw_yield = msg.get("Value") or msg.get("yield") or msg.get("value")
    if not state or not commodity or year is None or raw_yield is None:
        logger.warning("Skipping invalid yield msg: %s", msg)
        return
    try:
        year_i = int(year)
        yield_f = float(raw_yield)
    except Exception:
        logger.exception("Error parsing yield msg: %s", msg)
        return
    # pick any station available
    station_map = next(iter(weather_data.values())) if weather_data else {}
    avg_sm = station_map.get("SM_avg")
    drought_flag = station_map.get("drought", False)

    record = {
        "state_name": state,
        "commodity_desc": commodity,
        "year": year_i,
        "yield": yield_f,
        "avg_soil_moisture": avg_sm,
        "drought": drought_flag,
        "event_time": datetime.utcnow().isoformat() + "Z"
    }
    writer = avro.io.DatumWriter(avro_schema)
    buf = io.BytesIO()
    encoder = avro.io.BinaryEncoder(buf)
    try:
        writer.write(record, encoder)
        producer.produce(args.output_topic, value=buf.getvalue())
        producer.poll(0)
        logger.info("Produced enriched yield for %s %d", state, year_i)
    except Exception:
        logger.exception("Failed to produce enriched yield: %s", record)

def main() -> None:
    args = parse_args()
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": "latest"
    })
    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    # Avro schema for enriched yield
    schema_str = """
    {"type":"record","name":"EnrichedYield","fields":[
      {"name":"state_name","type":"string"},
      {"name":"commodity_desc","type":"string"},
      {"name":"year","type":"int"},
      {"name":"yield","type":"double"},
      {"name":"avg_soil_moisture","type":["null","double"],"default":null},
      {"name":"drought","type":"boolean"},
      {"name":"event_time","type":"string"}
    ]}
    """
    avro_schema = avro.schema.parse(schema_str)
    weather_data: Dict[str, Any] = {}
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
                raw = msg.value()
                data = json.loads(raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw)
            except Exception:
                logger.exception("Failed to parse message: %s", msg)
                continue
            topic = msg.topic()
            if topic == args.weather_topic:
                process_weather(data, weather_data, args)
            elif topic == args.yield_topic:
                process_yield(data, weather_data, args, producer, avro_schema)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    main()