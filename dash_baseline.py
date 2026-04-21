#!/usr/bin/env python3
"""
Parquet-backed Dash dashboard with two tabs:

  "Statewide MO"  → USDA/MODIS/Daymet baseline (county yields + NDVI series)
  "Fields"        → per-field NDVI + SAR time series from Sentinel-2 / -1
                    exports cached in data/fields/<name>/

Reads only local parquets/GeoTIFFs at runtime — Google Earth Engine is
called once by scripts/export_fields_{ndvi,sar}.py, never here.

Run:
    .venv/bin/python dash_baseline.py    # → http://127.0.0.1:8050
"""

from __future__ import annotations

import base64
import io
from pathlib import Path

import dash
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import yaml
from dash import Input, Output, dcc, html

REPO = Path(__file__).resolve().parent
DATA = REPO / "data" / "real"
FIELDS_DIR = REPO / "data" / "fields"
FIELDS_CFG = REPO / "fields.yml"
ENGINE = "fastparquet"

CROP_COLORS = {
    "CORN": "#e6a515", "SOYBEANS": "#4a7c2a", "WHEAT": "#c58c3f",
    "SORGHUM": "#a63d40", "COTTON": "#d9d9d9", "RICE": "#7fb97f",
    "OATS": "#b89f6b", "HAY": "#6d8c5a",
}

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_statewide():
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


def load_fields_config() -> list[dict]:
    if not FIELDS_CFG.exists():
        return []
    cfg = yaml.safe_load(FIELDS_CFG.read_text()) or {}
    return cfg.get("fields", [])


def field_series(name: str, kind: str) -> pd.DataFrame:
    """kind: 'ndvi' or 'sar'."""
    p = FIELDS_DIR / name / f"{kind}_series.parquet"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_parquet(p)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")


def latest_tif(name: str, kind: str) -> Path | None:
    d = FIELDS_DIR / name
    if not d.exists():
        return None
    tifs = sorted(d.glob(f"{kind}_*.tif"))
    return tifs[-1] if tifs else None


def png_as_b64(path: Path) -> str | None:
    if not path.exists():
        return None
    import base64
    return "data:image/png;base64," + base64.b64encode(path.read_bytes()).decode()


def tif_as_png_b64(tif_path: Path, cmap: str, vmin: float | None = None, vmax: float | None = None) -> str:
    import rioxarray  # lazy import to keep cold start fast
    da = rioxarray.open_rasterio(tif_path, masked=True).squeeze()
    fig, ax = plt.subplots(figsize=(4, 4))
    im = ax.imshow(da.values, cmap=cmap, vmin=vmin, vmax=vmax)
    ax.axis("off")
    fig.colorbar(im, ax=ax, shrink=0.7)
    ax.set_title(tif_path.stem.replace("_", " "), fontsize=9)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=120)
    plt.close(fig)
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()


def field_baseline_z(series: pd.DataFrame, value_col: str, window_days: int = 7) -> tuple[float | None, str]:
    """Per-field z-score: latest value vs same-DOY ±window across prior years."""
    if series.empty:
        return None, "no data"
    latest = series.iloc[-1]
    doy = latest["date"].dayofyear
    prior = series.iloc[:-1]
    years_available = prior["date"].dt.year.nunique()
    if years_available < 3:
        return None, f"no baseline yet ({years_available} prior yr)"
    mask = ((prior["date"].dt.dayofyear - doy).abs() <= window_days)
    baseline = prior.loc[mask, value_col]
    if len(baseline) < 3 or baseline.std() == 0:
        return None, "insufficient baseline"
    z = (latest[value_col] - baseline.mean()) / baseline.std()
    return float(z), f"baseline n={len(baseline)} · ±{window_days}d · {years_available} yrs"


def acreage_estimate(name: str, field_cfg: dict) -> tuple[float, float | None]:
    """Returns (aoi_hectares, vegetated_hectares_or_None)."""
    buf = field_cfg.get("buffer_m", 500)
    aoi_ha = (2 * buf) ** 2 / 10000.0
    tif = latest_tif(name, "ndvi")
    if tif is None:
        return aoi_ha, None
    try:
        import rioxarray
        da = rioxarray.open_rasterio(tif, masked=True).squeeze()
        veg = (da > 0.3).sum().values
        total_valid = (~da.isnull()).sum().values
        if total_valid == 0:
            return aoi_ha, None
        veg_ha = aoi_ha * float(veg) / float(total_valid)
        return aoi_ha, veg_ha
    except Exception:
        return aoi_ha, None


# ---------------------------------------------------------------------------
# Load once at startup
# ---------------------------------------------------------------------------

YIELDS, CENTROIDS, NDVI = load_statewide()
COUNTIES = sorted(YIELDS["county"].unique())
CROPS = sorted(YIELDS["commodity"].unique())
YEARS = sorted(YIELDS["year"].unique())
FIELDS = load_fields_config()
FIELDS_BY_NAME = {f["name"]: f for f in FIELDS}
FIELD_NAMES = [f["name"] for f in FIELDS]

# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

app = dash.Dash(__name__, title="MO Agri — Baseline + Fields")
server = app.server  # gunicorn entrypoint for Render / Heroku

STATEWIDE_TAB = html.Div(style={"padding": "18px"}, children=[
    html.P(
        f"{len(YIELDS):,} yield rows · {len(COUNTIES)} counties · "
        f"{len(CROPS)} crops · {len(NDVI):,} MODIS NDVI observations · "
        f"{YEARS[0]}–{YEARS[-1]}",
        style={"color": "#555"},
    ),
    html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "20px"}, children=[
        html.Div([
            html.H4("Statewide corn yield by county"),
            html.Label("Year range"),
            dcc.RangeSlider(
                id="year-range", min=YEARS[0], max=YEARS[-1], value=[2015, YEARS[-1]],
                marks={str(y): str(y) for y in YEARS[::4]}, step=1,
            ),
            dcc.Graph(id="map"),
        ]),
        html.Div([
            html.H4("Crop yield trend"),
            html.Label("Commodity"),
            dcc.Dropdown(id="crop-pick",
                         options=[{"label": c.title(), "value": c} for c in CROPS],
                         value="CORN", clearable=False),
            dcc.Graph(id="trend"),
        ]),
    ]),
    html.Hr(),
    html.H4("County NDVI + yield drill-down"),
    html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "20px"}, children=[
        html.Div([
            html.Label("County"),
            dcc.Dropdown(id="county-pick",
                         options=[{"label": c.title(), "value": c} for c in COUNTIES],
                         value="BOONE", clearable=False),
            dcc.Graph(id="ndvi-series"),
        ]),
        html.Div([
            html.Label("Crop (for yield overlay)"),
            dcc.Dropdown(id="county-crop-pick",
                         options=[{"label": c.title(), "value": c} for c in CROPS],
                         value="CORN", clearable=False),
            dcc.Graph(id="county-yield"),
        ]),
    ]),

    html.Hr(),
    html.H3("Per-county Daymet × yield / NDVI correlations"),
    html.P("Pearson r between 23 years of growing-season weather and corn yield, "
           "one dot per county. Shows geographic heterogeneity the statewide "
           "regression hides: July heat hurts yield in 84/85 counties, May-Aug "
           "rain helps in 81/85, canopy heat-stress tracks in 84/85."),
    html.Img(
        src=png_as_b64(REPO / "figures" / "real" / "daymet_correlation_maps.png") or "",
        style={"width": "100%", "maxWidth": "1400px", "display": "block"},
    ),
])

FIELDS_TAB = html.Div(style={"padding": "18px"}, children=[
    html.Div(id="fields-empty-notice", children=(
        html.Div(style={"padding": "40px", "background": "#fff8e1", "border": "1px solid #f0c040"}, children=[
            html.H4("No fields configured yet"),
            html.P([
                "Copy ", html.Code("fields.yml.example"), " to ", html.Code("fields.yml"),
                ", add your field lat/lon/buffer, then run:"
            ]),
            html.Pre(
                ".venv/bin/python scripts/export_fields_ndvi.py\n"
                ".venv/bin/python scripts/export_fields_sar.py",
                style={"background": "#fafafa", "padding": "8px"}
            ),
            html.P("Restart this dashboard to pick up the new fields."),
        ]) if not FIELDS else html.Div()
    )),
    html.Div(children=[] if not FIELDS else [
        html.Div(style={"display": "flex", "alignItems": "center", "gap": "20px"}, children=[
            html.Label("Field", style={"fontWeight": "bold"}),
            dcc.Dropdown(id="field-pick",
                         options=[{"label": n, "value": n} for n in FIELD_NAMES],
                         value=FIELD_NAMES[0] if FIELD_NAMES else None,
                         clearable=False, style={"width": "360px"}),
            html.Div(id="field-meta", style={"color": "#555"}),
        ]),
        html.Hr(),
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr 1fr", "gap": "20px"}, children=[
            html.Div([html.H5("Latest NDVI tile"), html.Img(id="ndvi-tif", style={"width": "100%"})]),
            html.Div([html.H5("Latest SAR VV tile"), html.Img(id="sar-tif",  style={"width": "100%"})]),
            html.Div(id="field-metrics",
                     style={"background": "#f6f6f6", "padding": "14px", "borderRadius": "6px"}),
        ]),
        html.Hr(),
        html.H5("NDVI time series (mean · p10-p90 band)"),
        dcc.Graph(id="ndvi-field-series"),
        html.H5("SAR VV backscatter (dB) — lower = possibly drier"),
        dcc.Graph(id="sar-field-series"),
    ]),
])

app.layout = html.Div(style={"fontFamily": "system-ui, sans-serif", "margin": "10px"}, children=[
    html.H2("Missouri agricultural monitoring — local dashboard"),
    dcc.Tabs(id="tabs", value="tab-statewide", children=[
        dcc.Tab(label="Statewide MO", value="tab-statewide", children=[STATEWIDE_TAB]),
        dcc.Tab(label=f"Fields ({len(FIELDS)})", value="tab-fields", children=[FIELDS_TAB]),
    ]),
    html.Hr(),
    html.P(
        "Statewide: USDA NASS · MODIS MOD13Q1 · Daymet 1-km · "
        "Fields: Sentinel-2 SR + Sentinel-1 GRD exported via GEE to data/fields/.",
        style={"color": "#777", "fontSize": "12px"},
    ),
])


# ---------------------------------------------------------------------------
# Statewide callbacks (unchanged)
# ---------------------------------------------------------------------------

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
    fig.add_trace(go.Scatter(x=trend["year"], y=trend["mean"] + trend["std"],
                             line=dict(width=0), showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(
        x=trend["year"], y=trend["mean"] - trend["std"], fill="tonexty", line=dict(width=0),
        fillcolor=f"rgba{tuple(list(int(color[i:i+2], 16) for i in (1, 3, 5)) + [0.2])}",
        name="±1σ across counties", hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(x=trend["year"], y=trend["mean"], mode="lines+markers",
                             line=dict(color=color, width=2), name=f"mean {crop.lower()}"))
    for yr in (2012, 2018, 2022):
        if yr in trend["year"].values:
            fig.add_vline(x=yr, line=dict(color="firebrick", width=1, dash="dot"), opacity=0.35)
    fig.update_layout(title=f"{crop.title()} — MO statewide mean ({unit.lower()}), 2001-2023",
                      xaxis_title="Year", yaxis_title=unit.lower(),
                      margin=dict(l=40, r=20, t=50, b=40))
    return fig


@app.callback(Output("ndvi-series", "figure"), Input("county-pick", "value"))
def update_county_ndvi(county):
    sub = NDVI[NDVI["county"] == county].sort_values("date")
    if sub.empty:
        return go.Figure().update_layout(title=f"No NDVI for {county}")
    fig = px.scatter(sub, x="date", y="NDVI", color=sub["date"].dt.month,
                     color_continuous_scale="RdYlGn", labels={"color": "month"})
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
    fig = go.Figure(go.Scatter(x=sub["year"], y=sub["yield_value"], mode="lines+markers",
                               line=dict(color=color, width=2)))
    fig.update_layout(title=f"{county.title()} — {crop.lower()} yield ({unit.lower()})",
                      xaxis_title="Year", yaxis_title=unit.lower(),
                      margin=dict(l=40, r=20, t=50, b=40))
    return fig


# ---------------------------------------------------------------------------
# Fields callbacks
# ---------------------------------------------------------------------------

if FIELDS:
    @app.callback(Output("field-meta", "children"), Input("field-pick", "value"))
    def update_field_meta(name):
        f = FIELDS_BY_NAME.get(name, {})
        return f"{f.get('lat', '?'):.4f}, {f.get('lon', '?'):.4f}  ·  buffer {f.get('buffer_m', '?')} m"

    @app.callback(
        Output("ndvi-field-series", "figure"),
        Output("sar-field-series", "figure"),
        Output("ndvi-tif", "src"),
        Output("sar-tif", "src"),
        Output("field-metrics", "children"),
        Input("field-pick", "value"),
    )
    def update_field_view(name):
        ndvi_s = field_series(name, "ndvi")
        sar_s = field_series(name, "sar")

        # NDVI series
        if ndvi_s.empty:
            fig_n = go.Figure().update_layout(title="No NDVI exports yet — run export_fields_ndvi.py")
        else:
            fig_n = go.Figure()
            fig_n.add_trace(go.Scatter(x=ndvi_s["date"], y=ndvi_s["ndvi_p90"],
                                       line=dict(width=0), showlegend=False, hoverinfo="skip"))
            fig_n.add_trace(go.Scatter(x=ndvi_s["date"], y=ndvi_s["ndvi_p10"],
                                       fill="tonexty", line=dict(width=0),
                                       fillcolor="rgba(74,124,42,0.15)",
                                       name="p10–p90", hoverinfo="skip"))
            fig_n.add_trace(go.Scatter(x=ndvi_s["date"], y=ndvi_s["ndvi_mean"],
                                       mode="lines+markers", line=dict(color="#4a7c2a", width=2),
                                       name="NDVI mean"))
            fig_n.update_layout(title=f"{name} — Sentinel-2 NDVI",
                                yaxis_range=[0, 1], margin=dict(l=40, r=20, t=50, b=40))

        # SAR series
        if sar_s.empty:
            fig_s = go.Figure().update_layout(title="No SAR exports yet — run export_fields_sar.py")
        else:
            fig_s = go.Figure()
            fig_s.add_trace(go.Scatter(x=sar_s["date"], y=sar_s["vv_p90"],
                                       line=dict(width=0), showlegend=False, hoverinfo="skip"))
            fig_s.add_trace(go.Scatter(x=sar_s["date"], y=sar_s["vv_p10"],
                                       fill="tonexty", line=dict(width=0),
                                       fillcolor="rgba(166,61,64,0.15)",
                                       name="p10–p90", hoverinfo="skip"))
            fig_s.add_trace(go.Scatter(x=sar_s["date"], y=sar_s["vv_mean_db"],
                                       mode="lines+markers", line=dict(color="#a63d40", width=2),
                                       name="VV dB mean"))
            fig_s.update_layout(title=f"{name} — Sentinel-1 VV backscatter",
                                yaxis_title="VV (dB)", margin=dict(l=40, r=20, t=50, b=40))

        # Latest TIF previews
        ndvi_tif = latest_tif(name, "ndvi")
        sar_tif = latest_tif(name, "sar")
        ndvi_src = tif_as_png_b64(ndvi_tif, cmap="RdYlGn", vmin=0, vmax=1) if ndvi_tif else ""
        sar_src  = tif_as_png_b64(sar_tif,  cmap="viridis") if sar_tif else ""

        # Metrics card
        fcfg = FIELDS_BY_NAME.get(name, {})
        aoi_ha, veg_ha = acreage_estimate(name, fcfg)
        z_n, z_n_src = field_baseline_z(ndvi_s, "ndvi_mean")
        z_s, z_s_src = field_baseline_z(sar_s, "vv_mean_db")

        def fmt_z(z, src):
            if z is None:
                return html.Div(f"— ({src})", style={"color": "#999"})
            color = "firebrick" if z < -1.5 else "seagreen" if z > 1.5 else "#333"
            return html.Div([html.B(f"{z:+.2f}σ"), html.Span(f"  ({src})",
                                                              style={"color": "#888", "fontSize": "11px"})],
                            style={"color": color})

        last_ndvi = ndvi_s["ndvi_mean"].iloc[-1] if not ndvi_s.empty else None
        last_vv = sar_s["vv_mean_db"].iloc[-1] if not sar_s.empty else None
        last_ndvi_date = ndvi_s["date"].iloc[-1].strftime("%Y-%m-%d") if not ndvi_s.empty else "—"
        last_sar_date = sar_s["date"].iloc[-1].strftime("%Y-%m-%d") if not sar_s.empty else "—"

        metrics = [
            html.H5("Field metrics"),
            html.Table(style={"width": "100%", "fontSize": "13px"}, children=[
                html.Tr([html.Td("AOI acreage"),      html.Td(f"{aoi_ha:.1f} ha")]),
                html.Tr([html.Td("Vegetated (NDVI>0.3)"),
                         html.Td(f"{veg_ha:.1f} ha" if veg_ha is not None else "—")]),
                html.Tr([html.Td("Latest NDVI"),
                         html.Td(f"{last_ndvi:.3f} ({last_ndvi_date})" if last_ndvi is not None else "—")]),
                html.Tr([html.Td("NDVI z-score"), html.Td(fmt_z(z_n, z_n_src))]),
                html.Tr([html.Td("Latest VV dB"),
                         html.Td(f"{last_vv:.2f} ({last_sar_date})" if last_vv is not None else "—")]),
                html.Tr([html.Td("VV z-score"), html.Td(fmt_z(z_s, z_s_src))]),
            ]),
            html.Div(
                "⚠ Below 2σ baseline — consider field inspection"
                if (z_n is not None and z_n < -2) or (z_s is not None and z_s < -2)
                else "",
                style={"marginTop": "10px", "color": "firebrick", "fontWeight": "bold"},
            ),
        ]
        return fig_n, fig_s, ndvi_src, sar_src, metrics


if __name__ == "__main__":
    app.run(debug=False, host="127.0.0.1", port=8050)
