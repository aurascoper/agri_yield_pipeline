#!/usr/bin/env python3
"""
Live-updating Dash dashboard with:
- Leaflet map of Missouri showing NDVI, weather, and yield overlays
- Time-series graphs for precipitation and yield
- Real-time alert panel
Updates every 30 seconds, caching queries in Redis.
"""

import os
import pandas as pd
import dash
from dash import html, dcc, Input, Output
import dash_leaflet as dl
import plotly.express as px
from influxdb_client import InfluxDBClient
import redis
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# InfluxDB configuration
INFLUX_URL = os.getenv("INFLUXDB_URL")
INFLUX_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUX_ORG = os.getenv("INFLUXDB_ORG")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET")

# Redis configuration and keys
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_WEATHER_KEY = os.getenv("REDIS_WEATHER_KEY", "weather_data")
REDIS_YIELD_KEY = os.getenv("REDIS_YIELD_KEY", "yield_data")
REDIS_NDVI_KEY = os.getenv("REDIS_NDVI_KEY", "ndvi_data")
REDIS_ALERTS_KEY = os.getenv("REDIS_ALERTS_KEY", "alerts")

# Dash update interval in milliseconds
UPDATE_INTERVAL = int(os.getenv("UPDATE_INTERVAL_SECONDS", 30)) * 1000

# Map center for Missouri
MO_CENTER = [38.5, -92.3]
MO_ZOOM = 6

# Initialize clients
influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = influx_client.query_api()
redis_client = redis.Redis.from_url(REDIS_URL)

app = dash.Dash(__name__)
server = app.server

def fetch_data(query: str, redis_key: str, expire: int = None):
    """Fetch data from Redis cache or InfluxDB."""
    try:
        cached = redis_client.get(redis_key)
        if cached:
            return pd.read_json(cached, orient="split")
        tables = query_api.query(query)
        records = [rec.values for table in tables for rec in table.records]
        df = pd.DataFrame(records)
        if expire:
            redis_client.set(redis_key, df.to_json(orient="split"), ex=expire)
        return df
    except Exception as e:
        print(f"Error fetching {redis_key}: {e}")
        return pd.DataFrame()

app.layout = html.Div([
    html.H2("Real-Time Agricultural Dashboard"),
    dcc.Interval(id="interval", interval=UPDATE_INTERVAL, n_intervals=0),
    dl.Map(
        children=[dl.TileLayer()],
        id="map",
        center=MO_CENTER,
        zoom=MO_ZOOM,
        style={"width": "100%", "height": "500px"},
    ),
    html.Div(style={"display": "flex"}, children=[
        dcc.Graph(id="precip-graph", style={"flex": 1}),
        dcc.Graph(id="yield-graph", style={"flex": 1}),
    ]),
    html.Div(id="alerts", style={"margin": "20px", "padding": "10px", "border": "1px solid red"}),
])

@app.callback(
    Output("map", "children"),
    Input("interval", "n_intervals"),
)
def update_map(n):
    layers = [dl.TileLayer()]
    # NDVI heatmap
    ndvi_query = (
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: -30m) |> filter(fn: (r) => r._measurement == "ndvi")'
    )
    ndvi_df = fetch_data(ndvi_query, REDIS_NDVI_KEY, expire=UPDATE_INTERVAL//1000)
    if {"lat", "lon", "_value"}.issubset(ndvi_df.columns):
        heat = ndvi_df[["lat", "lon", "_value"]].rename(columns={"_value": "intensity"})
        data = heat.values.tolist()
        layers.append(dl.Heatmap(data=data, radius=20))
    # Weather markers
    weather_query = (
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: -30m) |> filter(fn: (r) => r._measurement == "weather")'
    )
    weather_df = fetch_data(weather_query, REDIS_WEATHER_KEY, expire=UPDATE_INTERVAL//1000)
    if {"lat", "lon", "datatype", "_value"}.issubset(weather_df.columns):
        markers = []
        for _, row in weather_df.iterrows():
            markers.append(
                dl.Marker(
                    position=(row["lat"], row["lon"]),
                    children=dl.Tooltip(f'{row["datatype"]}: {row["_value"]}')
                )
            )
        layers.append(dl.LayerGroup(markers))
    # Yield markers
    yield_query = (
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: -30m) |> filter(fn: (r) => r._measurement == "yield")'
    )
    yield_df = fetch_data(yield_query, REDIS_YIELD_KEY, expire=UPDATE_INTERVAL//1000)
    if {"lat", "lon", "yield"}.issubset(yield_df.columns):
        markers = []
        for _, row in yield_df.iterrows():
            txt = f"{row.get('state','')}: {row.get('yield','')}"
            markers.append(dl.Marker(position=(row["lat"], row["lon"]), children=dl.Tooltip(txt)))
        layers.append(dl.LayerGroup(markers))
    return layers

@app.callback(
    Output("precip-graph", "figure"),
    Output("yield-graph", "figure"),
    Input("interval", "n_intervals"),
)
def update_graphs(n):
    # Precipitation time-series
    precip_query = (
        f'from(bucket:"{INFLUX_BUCKET}")'
        '|> range(start: -24h) '
        '|> filter(fn: (r) => r._measurement == "weather" and r.datatype == "PRCP") '
        '|> aggregateWindow(every: 1h, fn: mean)'
    )
    prcp_df = fetch_data(precip_query, f"{REDIS_WEATHER_KEY}_prcp", expire=UPDATE_INTERVAL//1000)
    if not prcp_df.empty and "_time" in prcp_df.columns:
        fig_prcp = px.line(prcp_df, x="_time", y="_value", title="Precipitation (last 24h)")
    else:
        fig_prcp = px.line(title="Precipitation (no data)")
    # Yield time-series
    yield_ts_query = (
        f'from(bucket:"{INFLUX_BUCKET}")'
        '|> range(start: -24h) '
        '|> filter(fn: (r) => r._measurement == "yield") '
        '|> aggregateWindow(every: 1h, fn: mean)'
    )
    yld_df = fetch_data(yield_ts_query, f"{REDIS_YIELD_KEY}_ts", expire=UPDATE_INTERVAL//1000)
    if not yld_df.empty and "_time" in yld_df.columns:
        fig_yld = px.line(yld_df, x="_time", y="_value", title="Yield (last 24h)")
    else:
        fig_yld = px.line(title="Yield (no data)")
    return fig_prcp, fig_yld

@app.callback(
    Output("alerts", "children"),
    Input("interval", "n_intervals"),
)
def update_alerts(n):
    items = []
    try:
        raw = redis_client.lrange(REDIS_ALERTS_KEY, -20, -1)
        items = [html.Div(msg.decode('utf-8')) for msg in raw]
    except Exception as e:
        items = [html.Div(f"Error fetching alerts: {e}")]
    return [html.H3("Alerts")] + items

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=True)