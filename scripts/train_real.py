#!/usr/bin/env python3
"""
Train yield prediction on real county-level data cached in data/real/.

Handles both single-AOI and multi-county NDVI parquets. Uses build_features
per (county, year) when county data is available, else per-year aggregation.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "data" / "real"
FIGS = REPO / "figures" / "real"
FIGS.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(REPO / "src"))
from yield_model import build_features, train_model

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("train_real")

PARQUET_ENGINE = "fastparquet"


def load_parquet(path):
    return pd.read_parquet(path, engine=PARQUET_ENGINE)


def load_data():
    ndvi_files = sorted(DATA.glob("ndvi_*.parquet"))
    weather_files = sorted(DATA.glob("noaa_*.parquet"))
    county_yield = sorted(DATA.glob("usda_corn_county_*.parquet"))
    state_yield = sorted(DATA.glob("usda_corn_state_*.parquet"))

    if not ndvi_files: raise FileNotFoundError(f"No NDVI in {DATA}")
    if not weather_files: raise FileNotFoundError(f"No weather in {DATA}")

    ndvi_frames = []
    for f in ndvi_files:
        df = load_parquet(f)
        df["date"] = pd.to_datetime(df["date"])
        df["year"] = df["date"].dt.year
        if "county" not in df.columns:
            # Single-AOI cache, infer county from filename stem
            df["county"] = f.stem.replace("ndvi_modis_MOD13Q1_", "").rsplit("_", 2)[0].upper()
        ndvi_frames.append(df)
    ndvi = pd.concat(ndvi_frames, ignore_index=True)

    weather = load_parquet(weather_files[0])
    weather["date"] = pd.to_datetime(weather["date"])
    weather["year"] = weather["date"].dt.year

    yields = load_parquet(county_yield[0]) if county_yield else load_parquet(state_yield[0])

    return ndvi, weather, yields


def build_multi_county_features(ndvi: pd.DataFrame, weather: pd.DataFrame,
                                yields: pd.DataFrame) -> tuple:
    """
    Build a (county, year) indexed feature matrix. Each county shares the same
    NOAA weather (single station), but has its own NDVI series.
    """
    has_county_yield = "county" in yields.columns
    has_county_ndvi = "county" in ndvi.columns

    if not has_county_yield or not has_county_ndvi:
        # fallback: single-series
        feat = build_features(ndvi, weather)
        target = yields.set_index("year")["yield_bu_acre"] if "year" in yields.columns else None
        return feat, target

    rows = []
    targets = []
    counties = sorted(ndvi["county"].unique())
    for county in counties:
        n_c = ndvi[ndvi["county"] == county][["year", "date", "NDVI"]].copy()
        feat_c = build_features(n_c, weather)
        feat_c["county"] = county
        feat_c = feat_c.reset_index()
        y_c = yields[yields["county"] == county][["year", "yield_bu_acre"]].copy()
        merged = feat_c.merge(y_c, on="year", how="inner")
        if merged.empty:
            log.warning("No yield match for county %s", county)
            continue
        rows.append(merged)

    if not rows:
        raise RuntimeError("No (county,year) matches between NDVI and yields")

    combined = pd.concat(rows, ignore_index=True)
    combined["county_year"] = combined["county"] + "_" + combined["year"].astype(str)
    combined = combined.set_index("county_year")

    feature_cols = [c for c in combined.columns
                    if c not in ("county", "year", "yield_bu_acre")]
    X = combined[feature_cols]
    y = combined["yield_bu_acre"]

    # add county as one-hot for the model to learn spatial bias
    county_dummies = pd.get_dummies(combined["county"], prefix="county", dtype=float)
    X = pd.concat([X, county_dummies], axis=1)

    log.info("Built features: %d samples × %d features across %d counties",
             len(X), X.shape[1], combined["county"].nunique())
    return X, y


def plot_model_results(result: dict, title_suffix: str, path: Path):
    y_true = result["y_true_train"]
    y_pred = result["y_pred_train"]
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    ax = axes[0]
    ax.scatter(y_true, y_pred, edgecolor="k", alpha=0.6, s=25)
    lo, hi = min(y_true.min(), y_pred.min()) - 5, max(y_true.max(), y_pred.max()) + 5
    ax.plot([lo, hi], [lo, hi], "r--")
    ax.set_xlabel("Actual yield (bu/acre)")
    ax.set_ylabel("Predicted yield (bu/acre)")
    ax.set_title(f"{title_suffix} | CV R²={result['cv_r2']:.3f}, RMSE={result['cv_rmse']:.1f}")

    ax = axes[1]
    resid = y_true - y_pred
    ax.hist(resid, bins=25, color="steelblue", edgecolor="k")
    ax.axvline(0, color="k", linewidth=0.5)
    ax.set_xlabel("Residual (bu/acre)")
    ax.set_title(f"Residual distribution (n={len(resid)})")

    ax = axes[2]
    if result["feature_importance"] is not None:
        fi = result["feature_importance"].head(12)
        ax.barh(fi.index[::-1], fi.values[::-1], color="teal")
        ax.set_xlabel("Importance")
        ax.set_title("Top 12 features (GBR)")
    else:
        ax.text(0.5, 0.5, "N/A (Ridge)", ha="center", va="center", transform=ax.transAxes)

    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    log.info("Saved %s", path)


def main():
    ndvi, weather, yields = load_data()
    log.info("NDVI %d rows | Weather %d rows | Yields %d rows",
             len(ndvi), len(weather), len(yields))

    X, y = build_multi_county_features(ndvi, weather, yields)

    # Drop rows with NaN target
    mask = y.notna()
    X, y = X.loc[mask], y.loc[mask]
    log.info("Training samples after cleanup: %d", len(X))

    for model_type in ("ridge", "gbr"):
        log.info("Training %s...", model_type.upper())
        result = train_model(X, y, model_type=model_type, cv=5)
        log.info("  %s CV R²=%.3f  RMSE=%.1f bu/acre  TrainR²=%.3f",
                 model_type.upper(), result["cv_r2"], result["cv_rmse"], result["train_r2"])
        plot_model_results(result, f"Real county-level | {model_type.upper()}",
                           FIGS / f"model_{model_type}_county.png")

    log.info("Figures saved to %s", FIGS)


if __name__ == "__main__":
    main()
