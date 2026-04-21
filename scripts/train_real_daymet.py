#!/usr/bin/env python3
"""
Retrain the statewide baseline with per-county Daymet weather.

Uses `data/real/daymet_<county>_*.parquet` when available, falling back to
the single KC MCI station for any county without Daymet data. Prints a
side-by-side comparison with the KC-only baseline and writes:

    figures/real/model_gbr_county_daymet.png
    figures/real/feature_importance_daymet.png
    figures/real/residuals_by_county_daymet.png
    figures/real/summary_daymet.json
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

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "data" / "real"
FIGS = REPO / "figures" / "real"
FIGS.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(REPO / "src"))
from yield_model import build_features, train_model  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("train_daymet")
ENGINE = "fastparquet"


def load_weather_for_county(county: str, fallback: pd.DataFrame) -> pd.DataFrame:
    key = county.lower().replace(" ", "_")
    p = DATA / f"daymet_{key}_2001_2023.parquet"
    if p.exists():
        w = pd.read_parquet(p, engine=ENGINE)
        w["date"] = pd.to_datetime(w["date"])
        w["year"] = w["date"].dt.year
        return w
    return fallback


def build_dataset(per_county_weather: bool):
    ndvi_files = sorted(DATA.glob("ndvi_modis_MOD13Q1_*_2001_2023.parquet"))
    frames = []
    for f in ndvi_files:
        df = pd.read_parquet(f, engine=ENGINE)
        df["date"] = pd.to_datetime(df["date"])
        df["year"] = df["date"].dt.year
        if "county" not in df.columns:
            df["county"] = f.stem.replace("ndvi_modis_MOD13Q1_", "").rsplit("_", 2)[0].upper()
        frames.append(df)
    ndvi = pd.concat(frames, ignore_index=True)

    fallback = pd.read_parquet(DATA / "noaa_USW00013993_2001_2023.parquet", engine=ENGINE)
    fallback["date"] = pd.to_datetime(fallback["date"])
    fallback["year"] = fallback["date"].dt.year

    yields = pd.read_parquet(DATA / "usda_allcrops_county_missouri_2001_2023.parquet", engine=ENGINE)
    corn = yields[yields["commodity"] == "CORN"][["year", "county", "county_ansi", "yield_value"]]
    corn = corn.rename(columns={"yield_value": "yield_bu_acre"})

    rows = []
    daymet_hits = daymet_misses = 0
    for county in sorted(ndvi["county"].unique()):
        wx = load_weather_for_county(county, fallback) if per_county_weather else fallback
        if per_county_weather:
            key = county.lower().replace(" ", "_")
            if (DATA / f"daymet_{key}_2001_2023.parquet").exists():
                daymet_hits += 1
            else:
                daymet_misses += 1

        n_c = ndvi[ndvi["county"] == county][["year", "date", "NDVI"]].copy()
        feat = build_features(n_c, wx).reset_index()
        feat["county"] = county
        y_c = corn[corn["county"] == county][["year", "yield_bu_acre"]]
        merged = feat.merge(y_c, on="year", how="inner")
        if not merged.empty:
            rows.append(merged)

    if per_county_weather:
        log.info("Daymet coverage: %d counties with per-county weather, %d fell back to KC",
                 daymet_hits, daymet_misses)
    return pd.concat(rows, ignore_index=True)


def prep(df: pd.DataFrame):
    df = df.dropna(subset=["yield_bu_acre"]).copy()
    df["county_year"] = df["county"] + "_" + df["year"].astype(str)
    df = df.set_index("county_year")
    feat_cols = [c for c in df.columns if c not in ("county", "year", "yield_bu_acre", "county_ansi")]
    X = df[feat_cols]
    X = pd.concat([X, pd.get_dummies(df["county"], prefix="county", dtype=float)], axis=1)
    y = df["yield_bu_acre"]
    return X, y, df


def render_diagnostics(result, y, df, tag: str):
    # feature importance
    fi = result["feature_importance"]
    fi_base = fi[~fi.index.str.startswith("county_")].head(15)
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh(fi_base.index[::-1], fi_base.values[::-1], color="teal")
    ax.set_xlabel("GBR feature importance")
    ax.set_title(f"Top features (GBR + Daymet) — CV R²={result['cv_r2']:.3f}")
    fig.tight_layout()
    out = FIGS / f"feature_importance_{tag}.png"
    fig.savefig(out, dpi=150); plt.close(fig)
    log.info("Saved %s", out)

    # actual vs predicted, colored by county
    fig, ax = plt.subplots(figsize=(8, 8))
    counties = df["county"].unique()
    cmap = plt.cm.tab20
    for i, c in enumerate(sorted(counties)):
        mask = df["county"] == c
        ax.scatter(y[mask], result["y_pred_train"][mask], alpha=0.55, s=22,
                   color=cmap(i % 20), edgecolors="none")
    lo, hi = y.min() - 5, y.max() + 5
    ax.plot([lo, hi], [lo, hi], "k--", linewidth=1, alpha=0.5)
    ax.set_xlabel("Actual yield (bu/acre)")
    ax.set_ylabel("Predicted yield (bu/acre)")
    ax.set_title(f"GBR (Daymet per county) — CV R²={result['cv_r2']:.3f} RMSE={result['cv_rmse']:.1f}")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    out = FIGS / f"model_gbr_county_{tag}.png"
    fig.savefig(out, dpi=150); plt.close(fig)
    log.info("Saved %s", out)

    # residuals
    residuals = (y - result["y_pred_train"]).to_frame("residual")
    residuals["county"] = df["county"].values
    per_county = residuals.groupby("county")["residual"].agg(["mean", "std", "count"])
    per_county = per_county.sort_values("mean")
    fig, ax = plt.subplots(figsize=(9, 14))
    colors = ["firebrick" if m < 0 else "seagreen" for m in per_county["mean"]]
    ax.barh(per_county.index, per_county["mean"], color=colors)
    ax.axvline(0, color="k", linewidth=0.7)
    ax.set_xlabel("Mean residual (bu/acre)  [actual − predicted]")
    ax.set_title(f"GBR residuals per county (Daymet) — {len(per_county)} counties")
    fig.tight_layout()
    out = FIGS / f"residuals_by_county_{tag}.png"
    fig.savefig(out, dpi=150); plt.close(fig)
    log.info("Saved %s", out)
    return per_county


def main():
    log.info("=== A. KC MCI single station (baseline) ===")
    df_kc = build_dataset(per_county_weather=False)
    X_kc, y_kc, _ = prep(df_kc)
    r_kc = train_model(X_kc, y_kc, model_type="gbr", cv=5)

    log.info("=== B. Daymet per-county weather ===")
    df_dm = build_dataset(per_county_weather=True)
    X_dm, y_dm, df_dm_idx = prep(df_dm)
    r_dm = train_model(X_dm, y_dm, model_type="gbr", cv=5)

    log.info("=== C. Daymet per-county + year fixed effects ===")
    yr_dummies = pd.get_dummies(df_dm_idx["year"], prefix="yr", dtype=float)
    X_dm_yr = pd.concat([X_dm, yr_dummies], axis=1)
    r_dm_yr = train_model(X_dm_yr, y_dm, model_type="gbr", cv=5)

    log.info("")
    log.info("━" * 60)
    log.info("COMPARISON")
    log.info("  A. KC-only            : %d × %d | CV R²=%.3f RMSE=%.2f Train R²=%.3f",
             len(y_kc), X_kc.shape[1], r_kc["cv_r2"], r_kc["cv_rmse"], r_kc["train_r2"])
    log.info("  B. Daymet only        : %d × %d | CV R²=%.3f RMSE=%.2f Train R²=%.3f",
             len(y_dm), X_dm.shape[1], r_dm["cv_r2"], r_dm["cv_rmse"], r_dm["train_r2"])
    log.info("  C. Daymet + year FE   : %d × %d | CV R²=%.3f RMSE=%.2f Train R²=%.3f",
             len(y_dm), X_dm_yr.shape[1], r_dm_yr["cv_r2"], r_dm_yr["cv_rmse"], r_dm_yr["train_r2"])
    log.info("  ΔR² (C − A) = %+.3f   ΔRMSE = %+.2f bu/acre",
             r_dm_yr["cv_r2"] - r_kc["cv_r2"], r_dm_yr["cv_rmse"] - r_kc["cv_rmse"])
    log.info("━" * 60)

    per_county = render_diagnostics(r_dm_yr, y_dm, df_dm_idx, tag="daymet")
    r_dm_only = r_dm  # preserve for summary
    r_dm = r_dm_yr   # downstream summary uses the best model

    summary = {
        "kc_baseline": {
            "n_samples": int(len(y_kc)),
            "n_features": int(X_kc.shape[1]),
            "cv_r2": float(r_kc["cv_r2"]),
            "cv_rmse": float(r_kc["cv_rmse"]),
            "train_r2": float(r_kc["train_r2"]),
        },
        "daymet_plus_year_fe": {
            "n_samples": int(len(y_dm)),
            "n_features": int(X_dm_yr.shape[1]),
            "cv_r2": float(r_dm_yr["cv_r2"]),
            "cv_rmse": float(r_dm_yr["cv_rmse"]),
            "train_r2": float(r_dm_yr["train_r2"]),
            "top_features": {k: float(v) for k, v in
                             r_dm_yr["feature_importance"][~r_dm_yr["feature_importance"].index.str.match(r"(county|yr)_")].head(8).to_dict().items()},
        },
        "daymet_only": {
            "cv_r2": float(r_dm_only["cv_r2"]),
            "cv_rmse": float(r_dm_only["cv_rmse"]),
        },
        "delta_best_vs_kc": {
            "cv_r2": float(r_dm_yr["cv_r2"] - r_kc["cv_r2"]),
            "cv_rmse": float(r_dm_yr["cv_rmse"] - r_kc["cv_rmse"]),
        },
        "residuals_within_2_buacre": int((per_county["mean"].abs() < 2).sum()),
        "residuals_within_5_buacre": int((per_county["mean"].abs() < 5).sum()),
        "residuals_above_10_buacre": int((per_county["mean"].abs() >= 10).sum()),
    }
    (FIGS / "summary_daymet.json").write_text(json.dumps(summary, indent=2))
    log.info("Saved %s", FIGS / "summary_daymet.json")


if __name__ == "__main__":
    main()
