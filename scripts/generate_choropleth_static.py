#!/usr/bin/env python3
"""
Fallback static MO choropleth using county centroids as bubbles.
Kaleido + remote geojson hangs on headless renders, so this uses matplotlib
with `mo_county_centroids.parquet` for a reliable PNG.

Writes: figures/real/choropleth_corn_yield.png
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "data" / "real"
FIGS = REPO / "figures" / "real"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("choropleth_static")


def main():
    yields = pd.read_parquet(DATA / "usda_allcrops_county_missouri_2001_2023.parquet", engine="fastparquet")
    centroids = pd.read_parquet(DATA / "mo_county_centroids.parquet", engine="fastparquet")

    corn = yields[(yields["commodity"] == "CORN") & (yields["year"] >= 2015)]
    avg = corn.groupby(["county", "county_ansi"])["yield_value"].mean().reset_index()
    avg = avg.rename(columns={"yield_value": "yield_bu_acre"})

    merged = centroids.merge(avg, on=["county", "county_ansi"], how="left")

    fig, ax = plt.subplots(figsize=(11, 8))
    no_data = merged[merged["yield_bu_acre"].isna()]
    ax.scatter(no_data["lon"], no_data["lat"], s=80, facecolors="none",
               edgecolors="lightgrey", linewidths=0.8, label="no corn yield")

    has_data = merged[merged["yield_bu_acre"].notna()].copy()
    vmin, vmax = has_data["yield_bu_acre"].quantile([0.05, 0.95])
    sc = ax.scatter(has_data["lon"], has_data["lat"],
                    c=has_data["yield_bu_acre"], cmap="YlGn",
                    s=180, vmin=vmin, vmax=vmax,
                    edgecolors="#333", linewidths=0.6)

    top = has_data.nlargest(5, "yield_bu_acre")
    bot = has_data.nsmallest(5, "yield_bu_acre")
    for _, r in pd.concat([top, bot]).iterrows():
        ax.annotate(r["county"].title(), (r["lon"], r["lat"]),
                    xytext=(3, 3), textcoords="offset points",
                    fontsize=7, color="#222")

    cb = fig.colorbar(sc, ax=ax, shrink=0.7, label="avg corn yield (bu/acre, 2015-2023)")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title("Missouri corn yield by county — avg 2015-2023\n"
                 f"({len(has_data)} counties with USDA yield data, top/bottom 5 labeled)")
    ax.grid(alpha=0.3)
    ax.set_aspect(1.3)
    ax.legend(loc="lower left", fontsize=8)
    fig.tight_layout()

    out = FIGS / "choropleth_corn_yield.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved %s", out)


if __name__ == "__main__":
    main()
