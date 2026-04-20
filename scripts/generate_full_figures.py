#!/usr/bin/env python3
"""
Generate the full Missouri baseline figure pack from cached parquets.

Outputs (to figures/real/):
  choropleth_corn_yield.png          Avg corn yield per county (2015-2023)
  choropleth_corn_yield.html         Interactive plotly version
  ndvi_yield_scatter_full.png        NDVI peak vs corn yield, colored by county
  multi_crop_coverage.png            Crop × county presence/intensity heatmap
  feature_importance_full.png        GBR top features on statewide dataset
  residuals_by_county.png            Mean residual per county (GBR)
  yield_trend_by_crop.png            Annual avg yield trend 2001-2023 per crop
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "data" / "real"
FIGS = REPO / "figures" / "real"
FIGS.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(REPO / "src"))
from yield_model import build_features, train_model  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gen_figs")

PARQUET_ENGINE = "fastparquet"


def load_all():
    ndvi_files = sorted(DATA.glob("ndvi_modis_MOD13Q1_*_2001_2023.parquet"))
    weather = pd.read_parquet(DATA / "noaa_USW00013993_2001_2023.parquet", engine=PARQUET_ENGINE)
    weather["date"] = pd.to_datetime(weather["date"])
    weather["year"] = weather["date"].dt.year

    yields = pd.read_parquet(DATA / "usda_allcrops_county_missouri_2001_2023.parquet", engine=PARQUET_ENGINE)
    counties = pd.read_parquet(DATA / "mo_county_centroids.parquet", engine=PARQUET_ENGINE)

    frames = []
    for f in ndvi_files:
        df = pd.read_parquet(f, engine=PARQUET_ENGINE)
        df["date"] = pd.to_datetime(df["date"])
        df["year"] = df["date"].dt.year
        if "county" not in df.columns:
            df["county"] = f.stem.replace("ndvi_modis_MOD13Q1_", "").rsplit("_", 2)[0].upper()
        frames.append(df)
    ndvi = pd.concat(frames, ignore_index=True)
    return ndvi, weather, yields, counties


# ---------------------------------------------------------------------------
# 1. Choropleth: avg corn yield per county (2015-2023)
# ---------------------------------------------------------------------------

def fig_choropleth_corn(yields: pd.DataFrame, counties: pd.DataFrame):
    corn = yields[(yields["commodity"] == "CORN") & (yields["year"] >= 2015)]
    avg = corn.groupby(["county", "county_ansi"])["yield_value"].mean().reset_index()
    avg["fips"] = "29" + avg["county_ansi"].astype(str).str.zfill(3)

    fig = px.choropleth(
        avg,
        geojson="https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json",
        locations="fips",
        color="yield_value",
        color_continuous_scale="YlGn",
        scope="usa",
        labels={"yield_value": "bu/acre"},
        title="Missouri corn yield (bu/acre, avg 2015-2023)",
        hover_data={"county": True, "fips": False, "yield_value": ":.1f"},
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    out_png = FIGS / "choropleth_corn_yield.png"
    out_html = FIGS / "choropleth_corn_yield.html"
    fig.write_html(out_html)
    try:
        fig.write_image(out_png, width=900, height=700, scale=2)
        log.info("Saved %s + html", out_png)
    except Exception as e:
        log.warning("PNG export failed (%s) — HTML saved", e)


# ---------------------------------------------------------------------------
# 2. NDVI peak vs corn yield scatter
# ---------------------------------------------------------------------------

def fig_ndvi_vs_yield(ndvi: pd.DataFrame, yields: pd.DataFrame):
    ndvi_growing = ndvi[(ndvi["date"].dt.month >= 6) & (ndvi["date"].dt.month <= 8)]
    peak = ndvi_growing.groupby(["county", "year"])["NDVI"].max().reset_index()
    peak = peak.rename(columns={"NDVI": "ndvi_peak"})
    corn = yields[yields["commodity"] == "CORN"][["year", "county", "yield_value"]]
    merged = peak.merge(corn, on=["year", "county"], how="inner")

    fig, ax = plt.subplots(figsize=(10, 6))
    counties = sorted(merged["county"].unique())
    cmap = plt.cm.tab20
    for i, c in enumerate(counties[:20]):  # legend sanity
        sub = merged[merged["county"] == c]
        ax.scatter(sub["ndvi_peak"], sub["yield_value"], alpha=0.6, s=22,
                   label=c if i < 10 else None, color=cmap(i % 20))
    # others in grey
    others = merged[~merged["county"].isin(counties[:20])]
    ax.scatter(others["ndvi_peak"], others["yield_value"], alpha=0.3, s=15, color="grey", label="other counties")
    # trend line
    slope, intercept = np.polyfit(merged["ndvi_peak"], merged["yield_value"], 1)
    xs = np.linspace(merged["ndvi_peak"].min(), merged["ndvi_peak"].max(), 50)
    ax.plot(xs, slope * xs + intercept, "k--", linewidth=1.5,
            label=f"linear fit: y={slope:.0f}·NDVI+{intercept:.0f}")
    r = merged[["ndvi_peak", "yield_value"]].corr().iloc[0, 1]
    ax.set_xlabel("Peak summer NDVI (Jun-Aug max)")
    ax.set_ylabel("Corn yield (bu/acre)")
    ax.set_title(f"Peak NDVI vs Corn Yield — {len(counties)} MO counties, 2001-2023 (Pearson r = {r:.2f}, n={len(merged)})")
    ax.legend(loc="upper left", fontsize=7, ncol=2)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    out = FIGS / "ndvi_yield_scatter_full.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    log.info("Saved %s (r=%.3f)", out, r)


# ---------------------------------------------------------------------------
# 3. Multi-crop coverage heatmap
# ---------------------------------------------------------------------------

def fig_crop_coverage(yields: pd.DataFrame):
    recent = yields[yields["year"] >= 2018]
    pivot = recent.pivot_table(index="county", columns="commodity",
                               values="yield_value", aggfunc="mean")
    # rank normalize per column for a comparable heatmap
    rank = pivot.rank(pct=True)
    rank = rank.reindex(rank.mean(axis=1).sort_values(ascending=False).index)

    fig, ax = plt.subplots(figsize=(9, 16))
    im = ax.imshow(rank.values, aspect="auto", cmap="YlGn", vmin=0, vmax=1)
    ax.set_xticks(range(len(rank.columns)))
    ax.set_xticklabels(rank.columns, rotation=45, ha="right")
    ax.set_yticks(range(len(rank.index)))
    ax.set_yticklabels(rank.index, fontsize=6)
    ax.set_title("MO crop yield rank per county (2018-2023 avg) — darker = higher yield")
    fig.colorbar(im, ax=ax, shrink=0.4, label="yield rank (0-1)")
    fig.tight_layout()
    out = FIGS / "multi_crop_coverage.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    log.info("Saved %s", out)


# ---------------------------------------------------------------------------
# 4. Annual yield trend per crop
# ---------------------------------------------------------------------------

def fig_yield_trend(yields: pd.DataFrame):
    trend = yields.groupby(["commodity", "year"])["yield_value"].mean().reset_index()
    crops = ["CORN", "SOYBEANS", "WHEAT", "SORGHUM", "COTTON", "HAY"]
    fig, axes = plt.subplots(2, 3, figsize=(14, 7), sharex=True)
    for ax, crop in zip(axes.flat, crops):
        sub = trend[trend["commodity"] == crop]
        if sub.empty:
            ax.set_visible(False); continue
        ax.plot(sub["year"], sub["yield_value"], marker="o", linewidth=1.5, color="#4a7c2a")
        # drought year markers
        for yr in (2012, 2018, 2022):
            if yr in sub["year"].values:
                ax.axvline(yr, color="firebrick", alpha=0.25, linewidth=1)
        unit = yields[yields["commodity"] == crop]["unit"].iloc[0]
        ax.set_title(f"{crop.title()} — mean yield ({unit.lower()})")
        ax.grid(alpha=0.3)
        ax.tick_params(axis="x", rotation=45)
    fig.suptitle("Missouri statewide crop yield trends, 2001-2023 (red = known drought year)", y=1.02)
    fig.tight_layout()
    out = FIGS / "yield_trend_by_crop.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved %s", out)


# ---------------------------------------------------------------------------
# 5. Feature importance + residuals-by-county (requires training)
# ---------------------------------------------------------------------------

def fig_model_diagnostics(ndvi: pd.DataFrame, weather: pd.DataFrame, yields: pd.DataFrame):
    corn = yields[yields["commodity"] == "CORN"][["year", "county", "county_ansi", "yield_value"]]
    corn = corn.rename(columns={"yield_value": "yield_bu_acre"})

    rows = []
    for county in sorted(ndvi["county"].unique()):
        n_c = ndvi[ndvi["county"] == county][["year", "date", "NDVI"]].copy()
        feat = build_features(n_c, weather).reset_index()
        feat["county"] = county
        y_c = corn[corn["county"] == county][["year", "yield_bu_acre"]]
        merged = feat.merge(y_c, on="year", how="inner")
        if not merged.empty:
            rows.append(merged)
    if not rows:
        log.warning("No merged rows for diagnostics")
        return

    combined = pd.concat(rows, ignore_index=True)
    combined["county_year"] = combined["county"] + "_" + combined["year"].astype(str)
    combined = combined.set_index("county_year")

    feat_cols = [c for c in combined.columns if c not in ("county", "year", "yield_bu_acre", "county_ansi")]
    X = combined[feat_cols]
    dummies = pd.get_dummies(combined["county"], prefix="county", dtype=float)
    X = pd.concat([X, dummies], axis=1)
    y = combined["yield_bu_acre"]
    mask = y.notna()
    X, y = X.loc[mask], y.loc[mask]

    result = train_model(X, y, model_type="gbr", cv=5)

    # feature importance (non-county features only)
    fi = result["feature_importance"]
    fi_base = fi[~fi.index.str.startswith("county_")].head(15)
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh(fi_base.index[::-1], fi_base.values[::-1], color="teal")
    ax.set_xlabel("GBR feature importance")
    ax.set_title(f"Top features (GBR) — statewide dataset, CV R²={result['cv_r2']:.3f}")
    fig.tight_layout()
    out = FIGS / "feature_importance_full.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    log.info("Saved %s", out)

    # residuals by county
    residuals = (y - result["y_pred_train"]).to_frame("residual")
    residuals["county"] = [ix.rsplit("_", 1)[0] for ix in residuals.index]
    per_county = residuals.groupby("county")["residual"].agg(["mean", "std", "count"])
    per_county = per_county.sort_values("mean")

    fig, ax = plt.subplots(figsize=(9, 14))
    colors = ["firebrick" if m < 0 else "seagreen" for m in per_county["mean"]]
    ax.barh(per_county.index, per_county["mean"], color=colors)
    ax.axvline(0, color="k", linewidth=0.7)
    ax.set_xlabel("Mean residual (bu/acre)  [actual - predicted]")
    ax.set_title("Per-county bias — GBR on statewide corn yields")
    ax.tick_params(axis="y", labelsize=6)
    fig.tight_layout()
    out = FIGS / "residuals_by_county.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    log.info("Saved %s", out)

    # stash summary for README
    summary = {
        "n_samples": int(len(X)),
        "n_counties": int(combined["county"].nunique()),
        "n_features": int(X.shape[1]),
        "gbr_cv_r2": float(result["cv_r2"]),
        "gbr_cv_rmse": float(result["cv_rmse"]),
        "gbr_train_r2": float(result["train_r2"]),
        "top_features": fi_base.head(5).to_dict(),
    }
    (FIGS / "summary.json").write_text(json.dumps(summary, indent=2))
    log.info("Summary: %s", summary)


def main():
    ndvi, weather, yields, counties = load_all()
    log.info("NDVI %d | Weather %d | Yields %d | Counties %d",
             len(ndvi), len(weather), len(yields), len(counties))
    fig_choropleth_corn(yields, counties)
    fig_ndvi_vs_yield(ndvi, yields)
    fig_crop_coverage(yields)
    fig_yield_trend(yields)
    fig_model_diagnostics(ndvi, weather, yields)


if __name__ == "__main__":
    main()
