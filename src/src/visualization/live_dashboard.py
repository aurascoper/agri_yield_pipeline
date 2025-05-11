"""
Real-time Dash visualization dashboard for agricultural data.
"""

import os
import json
import logging
from typing import Any, Dict, List

import pandas as pd
import plotly.express as px

from dash import Dash, html, dcc, Input, Output
import dash_leaflet as dl

import redis

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redis configuration
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_WEATHER_KEY: str = os.getenv("REDIS_WEATHER_KEY", "weather_data")
REDIS_YIELD_KEY: str = os.getenv("REDIS_YIELD_KEY", "yield_data")
REDIS_ALERTS_KEY: str = os.getenv("REDIS_ALERTS_KEY", "alerts")

# Initialize Redis client
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

def fetch_cached_data(key: str) -> List[Dict[str, Any]]:
    """Fetch and decode JSON data stored under Redis key."""
    try:
        data = redis_client.get(key)
        if not data:
            return []
        return json.loads(data)
    except Exception as e:
        logger.exception("Error fetching key %s from Redis: %s", key, e)
        return []

# Initialize Dash app
app = Dash(__name__)

app.layout = html.Div([
    html.H1("Real-Time Agricultural Dashboard"),

    # Leaflet map centered on Missouri
    dl.Map(
        children=[dl.TileLayer()],
        center=[38.5, -92.5],
        zoom=6,
        style={"width": "100%", "height": "500px"}
    ),

    # Charts
    html.Div([
        dcc.Graph(id="precipitation-chart"),
        dcc.Graph(id="yield-chart"),
    ], style={"display": "flex", "justifyContent": "space-around", "marginTop": "20px"}),

    # Alert panel
    html.Div(id="alert-panel", style={"marginTop": "20px"}),

    # Interval for updates
    dcc.Interval(
        id="interval-component",
        interval=30 * 1000,
        n_intervals=0
    )
], style={"margin": "20px"})


@app.callback(
    [Output("precipitation-chart", "figure"),
     Output("yield-chart", "figure"),
     Output("alert-panel", "children")],
    [Input("interval-component", "n_intervals")]
)
def update_dashboard(n_intervals: int):
    # Fetch cached data
    weather_data = fetch_cached_data(REDIS_WEATHER_KEY)
    yield_data = fetch_cached_data(REDIS_YIELD_KEY)
    alerts = fetch_cached_data(REDIS_ALERTS_KEY)

    # Precipitation chart
    if weather_data:
        df_w = pd.DataFrame(weather_data)
        df_w["date"] = pd.to_datetime(df_w.get("date", df_w.get("time")))
        fig_precip = px.line(df_w, x="date", y="value", title="Precipitation Over Time")
    else:
        fig_precip = px.line(title="Precipitation Over Time")

    # Yield chart
    if yield_data:
        df_y = pd.DataFrame(yield_data)
        if "year" in df_y:
            df_y["year"] = df_y["year"].astype(int)
            df_y.sort_values("year", inplace=True)
            fig_yield = px.bar(df_y, x="year", y="yield", title="Yield Over Years")
        else:
            fig_yield = px.bar(title="Yield Over Years")
    else:
        fig_yield = px.bar(title="Yield Over Years")

    # Alerts panel
    if alerts:
        alert_items = [html.Li(f"{a.get('event_time')}: {a.get('station', a.get('state'))} - {a.get('type', 'ALERT')}") for a in alerts]
        alert_panel = html.Div([
            html.H3("Alerts"),
            html.Ul(alert_items)
        ], style={"color": "red"})
    else:
        alert_panel = html.Div("No alerts", style={"color": "green"})

    return fig_precip, fig_yield, alert_panel


if __name__ == "__main__":
    app.run_server(debug=True)