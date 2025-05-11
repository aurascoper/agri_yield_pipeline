"""
Stream health monitoring with SMS alerts (Twilio) and PostgreSQL logging.
Avoids duplicate alerts and tracks acknowledgments.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Set, Optional

import psycopg2
from twilio.rest import Client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AlertManager:
    """
    Monitors named data streams by tracking heartbeats.
    Sends SMS alerts via Twilio when downtime exceeds thresholds.
    Logs incidents in PostgreSQL and avoids duplicate alerts until acknowledged.
    """

    def __init__(self, stream_thresholds: Dict[str, timedelta]) -> None:
        # Maximum allowed downtime per stream
        self.stream_thresholds = stream_thresholds
        # Last heartbeat times per stream
        self.last_heartbeats: Dict[str, datetime] = {}
        # Active streams in alert state to prevent duplicates
        self.active_alerts: Set[str] = set()

        # Twilio setup
        account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        auth_token = os.getenv("TWILIO_AUTH_TOKEN", "")
        self.twilio_from = os.getenv("TWILIO_FROM_NUMBER", "")
        self.twilio_to = os.getenv("TWILIO_TO_NUMBER", "")
        self.twilio_client = Client(account_sid, auth_token)

        # PostgreSQL setup
        dsn = os.getenv("POSTGRES_DSN", "")
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True
        self._init_db()

    def _init_db(self) -> None:
        """Create the incidents table if it does not exist."""
        ddl = """
        CREATE TABLE IF NOT EXISTS incidents (
            id SERIAL PRIMARY KEY,
            stream_name TEXT NOT NULL,
            incident_type TEXT NOT NULL,
            first_detected TIMESTAMP NOT NULL,
            last_notified TIMESTAMP NOT NULL,
            acknowledged BOOLEAN NOT NULL DEFAULT FALSE,
            ack_timestamp TIMESTAMP,
            details TEXT
        );
        """
        with self.conn.cursor() as cur:
            cur.execute(ddl)

    def record_heartbeat(self, stream_name: str) -> None:
        """Record receipt of a new event for the stream. Resolves active alerts."""
        now = datetime.utcnow()
        self.last_heartbeats[stream_name] = now
        # Auto-resolve if in alert
        if stream_name in self.active_alerts:
            logger.info("Stream '%s' recovered at %s", stream_name, now)
            self.ack(stream_name)

    def check_streams(self) -> None:
        """Check all streams for downtime and trigger alerts as needed."""
        now = datetime.utcnow()
        for stream, threshold in self.stream_thresholds.items():  # type: ignore
            last = self.last_heartbeats.get(stream)
            if last is None or (now - last) > threshold:
                if stream not in self.active_alerts:
                    details = f"No data for '{stream}' since {last}"
                    self._alert(stream, "down", details)

    def _alert(self, stream_name: str, incident_type: str, details: str) -> None:
        """Log a new incident and send an SMS alert via Twilio."""
        now = datetime.utcnow()
        # Insert into DB
        insert_sql = (
            "INSERT INTO incidents"
            " (stream_name, incident_type, first_detected, last_notified, details)"
            " VALUES (%s, %s, %s, %s, %s) RETURNING id;"
        )
        incident_id: Optional[int] = None
        try:
            with self.conn.cursor() as cur:
                cur.execute(insert_sql, (stream_name, incident_type, now, now, details))
                incident_id = cur.fetchone()[0]
        except Exception:
            logger.exception("Failed to log incident for %s", stream_name)

        # Send SMS
        msg = f"ALERT: {stream_name} {incident_type} at {now.isoformat()}. {details}"
        try:
            self.twilio_client.messages.create(
                body=msg, from_=self.twilio_from, to=self.twilio_to
            )
            logger.info("SMS sent for stream '%s'", stream_name)
        except Exception:
            logger.exception("Failed to send SMS for %s", stream_name)

        self.active_alerts.add(stream_name)

    def ack(self, stream_name: str) -> None:
        """Acknowledge and clear active alerts for a stream."""
        now = datetime.utcnow()
        update_sql = (
            "UPDATE incidents SET acknowledged = TRUE, ack_timestamp = %s"
            " WHERE stream_name = %s AND acknowledged = FALSE;"
        )
        try:
            with self.conn.cursor() as cur:
                cur.execute(update_sql, (now, stream_name))
        except Exception:
            logger.exception("Failed to acknowledge incidents for %s", stream_name)
        self.active_alerts.discard(stream_name)
        logger.info("Alerts acknowledged for stream '%s'", stream_name)