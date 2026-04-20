#!/usr/bin/env python3
"""
Parquet-backed Dash dashboard for the baseline pipeline.

Unlike `dash_app.py` (which requires PostgreSQL + InfluxDB), this app reads
directly from `data/real/*.parquet`, so it works out-of-the-box after running
`scripts/fetch_real_data.py`, `scripts/fetch_all_counties.py`, and
`scripts/fetch_usda_all_crops.py`.

Run:
    .venv/bin/python dash_baseline.py
    # → http://127.0.0.1:8050
"""

from __future__ import annotations

from pathlib import Path

import dash
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Input, Output, dcc, html

REPO = Path(__file__).resolve().parent
DATA = REPO / "data" / "real"
ENGINE = "fastparquet"

CROP_COLORS = {
    "CORN": "#e6a515", "SOYBEANS": "#4a7c2a", "WHEAT": "#c58c3f",
    "SORGHUM": "#a63d40", "COTTON": "#d9d9d9", "RICE": "#7fb97f",
    "OATS": "#b89f6b", "HAY": "#6d8c5a",
}


def load_data():
    yields = pd.read_parquet(DATA / "usda_allcrops_county_missouri_2001_2023.parquet", engine=ENGINE)
    centroids = pd.read_parquet(DATA / "mo_county_centroids.parquet", engine=ENGINE)

    ndvi_files = sorted(DATA.glob("ndvi_modis_MOD13Q1_*_2001_2023.parquet"))
    frames = []
    for f in ndvi_files:
        df = pd.read_parquet(f, engine=ENGINE)
        df["date"] = pd.to_datetime(df["date"])
        df["year"] = df["date"].dt.year
        if "county" not in df.columns:
            df["county"] = f.stem.replace("ndvi_modis_MOD13Q1_", "").rsplit("_", 2)[0].upper()
        frames.append(df)
    ndvi = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return yields, centroids, ndvi


YIELDS, CENTROIDS, NDVI = load_data()
COUNTIES = sorted(YIELDS["county"].unique())
CROPS = sorted(YIELDS["commodity"].unique())
YEARS = sorted(YIELDS["year"].unique())

app = dash.Dash(__name__, title="MO Agri Yield — Baseline")
app.layout = html.Div(
    style={"fontFamily": "system-ui, sans-serif", "margin": "18px"},
    children=[
        html.H2("Missouri agricultural yield — baseline dashboard"),
        html.P(
            f"{len(YIELDS):,} yield rows · {len(COUNTIES)} counties · "
            f"{len(CROPS)} crops · {len(NDVI):,} NDVI observations · "
            f"{YEARS[0]}–{YEARS[-1]}",
            style={"color": "#555"},
        ),

        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "20px"}, children=[
            html.Div([
                html.H4("Statewide corn yield by county"),
                html.Label("Year range"),
                dcc.RangeSlider(
                    id="year-range",
                    min=YEARS[0], max=YEARS[-1], value=[2015, YEARS[-1]],
                    marks={y: str(y) for y in YEARS[::4]}, step=1,
                ),
                dcc.Graph(id="map"),
            ]),
            html.Div([
                html.H4("Crop yield trend"),
                html.Label("Commodity"),
                dcc.Dropdown(id="crop-pick", options=[{"label": c.title(), "value": c} for c in CROPS],
                             value="CORN", clearable=False),
                dcc.Graph(id="trend"),
            ]),
        ]),

        html.Hr(),
        html.H4("County NDVI + yield drill-down"),
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "20px"}, children=[
            html.Div([
                html.Label("County"),
                dcc.Dropdown(id="county-pick", options=[{"label": c.title(), "value": c} for c in COUNTIES],
                             value="BOONE", clearable=False),
                dcc.Graph(id="ndvi-series"),
            ]),
            html.Div([
                html.Label("Crop (for yield overlay)"),
                dcc.Dropdown(id="county-crop-pick", options=[{"label": c.title(), "value": c} for c in CROPS],
                             value="CORN", clearable=False),
                dcc.Graph(id="county-yield"),
            ]),
        ]),

        html.Hr(),
        html.P(
            "Data: USDA NASS Quick Stats · MODIS MOD13Q1 NDVI (ORNL DAAC) · "
            "NOAA GHCND (KC MCI station). Cached locally in data/real/.",
            style={"color": "#777", "fontSize": "12px"},
        ),
    ],
)


@app.callback(Output("map", "figure"), Input("year-range", "value"))
def update_map(years):
    lo, hi = years
    corn = YIELDS[(YIELDS["commodity"] == "CORN") & (YIELDS["year"].between(lo, hi))]
    avg = corn.groupby(["county", "county_ansi"])["yield_value"].mean().reset_index()
    avg = avg.rename(columns={"yield_value": "yield_bu_acre"})
    merged = CENTROIDS.merge(avg, on=["county", "county_ansi"], how="left")

    fig = px.scatter_geo(
        merged.dropna(subset=["yield_bu_acre"]),
        lat="lat", lon="lon", color="yield_bu_acre", size="yield_bu_acre",
        hover_name="county", color_continuous_scale="YlGn",
        labels={"yield_bu_acre": "bu/acre"},
    )
    fig.update_geos(fitbounds="locations", visible=True, showsubunits=True, subunitcolor="#aaa")
    fig.update_layout(title=f"Corn yield — avg {lo}-{hi}", margin=dict(l=0, r=0, t=40, b=0))
    return fig


@app.callback(Output("trend", "figure"), Input("crop-pick", "value"))
def update_trend(crop):
    sub = YIELDS[YIELDS["commodity"] == crop]
    trend = sub.groupby("year")["yield_value"].agg(["mean", "std"]).reset_index()
    unit = sub["unit"].iloc[0] if not sub.empty else ""
    color = CROP_COLORS.get(crop, "#444")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend["year"], y=trend["mean"] + trend["std"],
        line=dict(width=0), showlegend=False, hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=trend["year"], y=trend["mean"] - trend["std"],
        fill="tonexty", line=dict(width=0),
        fillcolor=f"rgba{tuple(list(int(color[i:i+2], 16) for i in (1, 3, 5)) + [0.2])}",
        name="±1σ across counties", hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=trend["year"], y=trend["mean"],
        mode="lines+markers", line=dict(color=color, width=2),
        name=f"mean {crop.lower()}",
    ))
    for yr in (2012, 2018, 2022):
        if yr in trend["year"].values:
            fig.add_vline(x=yr, line=dict(color="firebrick", width=1, dash="dot"), opacity=0.35)
    fig.update_layout(
        title=f"{crop.title()} — MO statewide mean ({unit.lower()}), 2001-2023",
        xaxis_title="Year", yaxis_title=unit.lower(), margin=dict(l=40, r=20, t=50, b=40),
    )
    return fig


@app.callback(Output("ndvi-series", "figure"), Input("county-pick", "value"))
def update_ndvi(county):
    sub = NDVI[NDVI["county"] == county].sort_values("date")
    if sub.empty:
        return go.Figure().update_layout(title=f"No NDVI for {county}")
    fig = px.scatter(sub, x="date", y="NDVI", color=sub["date"].dt.month,
                     color_continuous_scale="RdYlGn",
                     labels={"color": "month"})
    fig.update_traces(marker=dict(size=4))
    fig.update_layout(title=f"{county.title()} — MODIS NDVI 16-day composite",
                      margin=dict(l=40, r=20, t=50, b=40))
    return fig


@app.callback(
    Output("county-yield", "figure"),
    Input("county-pick", "value"), Input("county-crop-pick", "value"),
)
def update_county_yield(county, crop):
    sub = YIELDS[(YIELDS["county"] == county) & (YIELDS["commodity"] == crop)]
    if sub.empty:
        return go.Figure().update_layout(title=f"No {crop.lower()} yield for {county}")
    unit = sub["unit"].iloc[0]
    color = CROP_COLORS.get(crop, "#444")
    fig = go.Figure(go.Scatter(
        x=sub["year"], y=sub["yield_value"], mode="lines+markers",
        line=dict(color=color, width=2),
    ))
    fig.update_layout(title=f"{county.title()} — {crop.lower()} yield ({unit.lower()})",
                      xaxis_title="Year", yaxis_title=unit.lower(),
                      margin=dict(l=40, r=20, t=50, b=40))
    return fig


if __name__ == "__main__":
    app.run_server(debug=False, host="127.0.0.1", port=8050)
