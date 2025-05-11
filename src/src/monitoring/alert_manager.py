#!/usr/bin/env python3
"""
StreamMonitor: subscribes to Kafka stream processing metrics,
sends SMS alerts via Twilio on anomalies, logs events in PostgreSQL,
and tracks user acknowledgments of alerts.
"""

import os
import json
import logging
from datetime import datetime

import psycopg2
from confluent_kafka import Consumer, KafkaError
from twilio.rest import Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamMonitor:
    """Monitor Kafka metrics, send alerts, log events, and track acknowledgments."""

    def __init__(self):
        # Kafka configuration
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.group_id = os.getenv("KAFKA_CONSUMER_GROUP", "monitor_group")
        self.metrics_topic = os.getenv("KAFKA_METRICS_TOPIC", "stream_metrics")

        # Anomaly thresholds per metric
        self.thresholds = {
            "lag": float(os.getenv("LAG_THRESHOLD", "1000")),
            "error_rate": float(os.getenv("ERROR_RATE_THRESHOLD", "0.05")),
        }

        # Twilio client setup
        account_sid = os.getenv("TWILIO_ACCOUNT_SID")
        auth_token = os.getenv("TWILIO_AUTH_TOKEN")
        self.twilio_from = os.getenv("TWILIO_FROM_NUMBER")
        self.twilio_to = os.getenv("TWILIO_TO_NUMBER")
        self.twilio_client = Client(account_sid, auth_token)

        # PostgreSQL connection
        dsn = os.getenv("POSTGRES_DSN")
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True
        self._init_db()

        # Track active alerts to avoid duplicates
        self.active_alerts = set()

        # Initialize Kafka consumer
        self.consumer = Consumer({
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "latest"
        })
        self.consumer.subscribe([self.metrics_topic])
        logger.info("Subscribed to metrics topic: %s", self.metrics_topic)

    def _init_db(self):
        """Create monitor_events table if it does not exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS monitor_events (
            id SERIAL PRIMARY KEY,
            stream TEXT,
            metric TEXT,
            value DOUBLE PRECISION,
            threshold DOUBLE PRECISION,
            event_time TIMESTAMP,
            alert_time TIMESTAMP,
            acknowledged BOOLEAN DEFAULT FALSE,
            ack_time TIMESTAMP,
            details TEXT
        );
        """
        with self.conn.cursor() as cur:
            cur.execute(create_table_sql)

    def process_message(self, msg):
        """Process a single Kafka message and detect anomalies."""
        try:
            payload = msg.value()
            data = json.loads(payload.decode("utf-8") if isinstance(payload, (bytes, bytearray)) else payload)
        except Exception as e:
            logger.error("Invalid message format: %s", e)
            return

        stream = data.get("stream")
        metric = data.get("metric")
        value = data.get("value")
        timestamp = data.get("timestamp")
        try:
            event_time = datetime.fromisoformat(timestamp) if timestamp else datetime.utcnow()
        except Exception:
            event_time = datetime.utcnow()

        # Only monitor configured metrics
        if metric not in self.thresholds:
            return

        threshold = self.thresholds[metric]
        alert_key = f"{stream}:{metric}"
        # Check for anomaly
        if value is not None and value > threshold:
            if alert_key not in self.active_alerts:
                details = f"{metric}={value} exceeded {threshold}"
                self._send_alert(stream, metric, value, threshold, event_time, details)
                self.active_alerts.add(alert_key)
        else:
            # Condition resolved
            if alert_key in self.active_alerts:
                self.ack(stream, metric)

    def _send_alert(self, stream, metric, value, threshold, event_time, details):
        """Log the alert event and send an SMS via Twilio."""
        alert_time = datetime.utcnow()
        insert_sql = ("""
            INSERT INTO monitor_events
            (stream, metric, value, threshold, event_time, alert_time, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        )
        with self.conn.cursor() as cur:
            cur.execute(insert_sql, (stream, metric, value, threshold, event_time, alert_time, details))

        # Send SMS alert
        message = (f"ALERT: stream={stream}, metric={metric} value={value} > {threshold} "
                   f"at {event_time.isoformat()}")
        try:
            self.twilio_client.messages.create(
                body=message, from_=self.twilio_from, to=self.twilio_to
            )
            logger.info("SMS sent for %s", alert_key)
        except Exception as e:
            logger.error("Failed to send SMS: %s", e)

    def ack(self, stream, metric):
        """Acknowledge and clear an active alert."""
        ack_time = datetime.utcnow()
        update_sql = ("""
            UPDATE monitor_events
            SET acknowledged = TRUE, ack_time = %s
            WHERE stream = %s AND metric = %s AND acknowledged = FALSE;
        """
        )
        with self.conn.cursor() as cur:
            cur.execute(update_sql, (ack_time, stream, metric))
        alert_key = f"{stream}:{metric}"
        self.active_alerts.discard(alert_key)
        logger.info("Alert acked for %s", alert_key)

    def run(self):
        """Start consuming Kafka messages and monitoring metrics."""
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Consumer error: %s", msg.error())
                    continue
                self.process_message(msg)
        except KeyboardInterrupt:
            logger.info("Shutting down monitor…")
        finally:
            self.consumer.close()
            self.conn.close()

if __name__ == "__main__":
    monitor = StreamMonitor()
    monitor.run()