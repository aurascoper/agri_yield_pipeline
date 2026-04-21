#!/usr/bin/env python3
"""
Per-county Daymet × yield / NDVI correlation choropleths.

Visualizes where (geographically) growing-season weather drives corn yield
or peak NDVI most strongly. Two panels per metric:

  1. r(TMAX_july, corn_yield)       — where July heat hurts yield most
  2. r(PRCP_may_aug, corn_yield)    — where growing-season rain matters most
  3. r(TMAX_july, NDVI_july)        — where heat stress shows up in canopy

A single state-wide regression hides this heterogeneity; per-county
correlations let you see the Ozarks vs. Bootheel vs. Missouri River
bottom as distinct weather-response regions.

Out: figures/real/daymet_correlation_maps.png
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
REAL = REPO / "data" / "real"
FIG = REPO / "figures" / "real"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("daymet_corr")


def county_to_file(name: str) -> str:
    return name.lower().replace(" ", "_")


def growing_season_aggregates(daymet: pd.DataFrame) -> pd.DataFrame:
    """Collapse daily Daymet to annual growing-season features."""
    daymet = daymet.copy()
    daymet["month"] = pd.to_datetime(daymet["date"]).dt.month
    tmax_july = (
        daymet.loc[daymet["month"] == 7].groupby("year")["TMAX"].mean()
    )
    prcp = (
        daymet.loc[daymet["month"].between(5, 8)].groupby("year")["PRCP"].sum()
    )
    gs = daymet.loc[daymet["month"].between(5, 8)].copy()
    gs["gdd"] = gs[["TMAX", "TMIN"]].clip(lower=10).mean(axis=1).sub(10).clip(lower=0)
    gdd = gs.groupby("year")["gdd"].sum()
    return pd.concat(
        [tmax_july.rename("tmax_july"), prcp.rename("prcp_may_aug"), gdd.rename("gdd_may_aug")],
        axis=1,
    ).reset_index()


def ndvi_july_mean(ndvi_df: pd.DataFrame) -> pd.DataFrame:
    ndvi_df = ndvi_df.copy()
    ndvi_df["month"] = pd.to_datetime(ndvi_df["date"]).dt.month
    return (
        ndvi_df[ndvi_df["month"] == 7]
        .groupby("year", as_index=False)["NDVI"].mean()
        .rename(columns={"NDVI": "ndvi_july"})
    )


def per_county_correlations() -> pd.DataFrame:
    centroids = pd.read_parquet(REAL / "mo_county_centroids.parquet")
    yields = pd.read_parquet(REAL / "usda_corn_county_missouri_2001_2023.parquet")
    yields["county"] = yields["county"].str.lower()

    rows = []
    for _, c in centroids.iterrows():
        cname = c["county"].lower()
        fname = county_to_file(cname)
        dm_path = REAL / f"daymet_{fname}_2001_2023.parquet"
        nd_path = REAL / f"ndvi_modis_MOD13Q1_{fname.replace('_', ' ')}_2001_2023.parquet"
        if not nd_path.exists():
            nd_path = REAL / f"ndvi_modis_MOD13Q1_{fname}_2001_2023.parquet"

        if not dm_path.exists():
            continue
        wx = growing_season_aggregates(pd.read_parquet(dm_path))
        cy = yields[yields["county"] == cname][["year", "yield_value"]].dropna()
        df = wx.merge(cy, on="year", how="inner")

        nd = None
        if nd_path.exists():
            nd = ndvi_july_mean(pd.read_parquet(nd_path))
            df = df.merge(nd, on="year", how="left")

        if len(df) < 10:
            continue

        row = {"county": cname, "lat": c["lat"], "lon": c["lon"], "n_years": len(df)}
        if df["yield_value"].std() > 0:
            row["r_tmax_yield"]   = df[["tmax_july",   "yield_value"]].corr().iloc[0, 1]
            row["r_prcp_yield"]   = df[["prcp_may_aug", "yield_value"]].corr().iloc[0, 1]
        if "ndvi_july" in df.columns and df["ndvi_july"].notna().sum() >= 10:
            row["r_tmax_ndvi"]    = df[["tmax_july",   "ndvi_july"]].corr().iloc[0, 1]
        rows.append(row)

    return pd.DataFrame(rows)


def draw_panel(ax, corr: pd.DataFrame, col: str, title: str):
    sub = corr.dropna(subset=[col])
    sc = ax.scatter(
        sub["lon"], sub["lat"],
        c=sub[col], cmap="RdBu_r", vmin=-1, vmax=1,
        s=160, edgecolor="black", linewidth=0.5,
    )
    ax.set_title(title, fontsize=11)
    ax.set_xlabel("Longitude"); ax.set_ylabel("Latitude")
    ax.set_aspect("equal"); ax.grid(alpha=0.3)
    plt.colorbar(sc, ax=ax, label="Pearson r", shrink=0.8)


def main():
    FIG.mkdir(parents=True, exist_ok=True)
    corr = per_county_correlations()
    log.info("computed correlations for %d counties", len(corr))
    corr.to_parquet(FIG / "daymet_correlation_per_county.parquet", index=False)

    fig, axes = plt.subplots(1, 3, figsize=(20, 6.5))
    draw_panel(axes[0], corr, "r_tmax_yield",
               "r(TMAX July, corn yield)\n(blue = heat hurts yield)")
    draw_panel(axes[1], corr, "r_prcp_yield",
               "r(PRCP May-Aug, corn yield)\n(red = rain helps yield)")
    draw_panel(axes[2], corr, "r_tmax_ndvi",
               "r(TMAX July, NDVI July)\n(blue = heat suppresses canopy)")
    fig.suptitle(
        f"Per-county Daymet × yield / NDVI correlations, "
        f"Missouri 2001-2023 ({len(corr)} counties)",
        fontsize=13, y=1.02,
    )
    out = FIG / "daymet_correlation_maps.png"
    plt.tight_layout()
    plt.savefig(out, dpi=120, bbox_inches="tight")
    log.info("wrote %s", out)


if __name__ == "__main__":
    main()
