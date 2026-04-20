#!/usr/bin/env python3
"""
Export a compact JSON describing each Missouri county's centroid and
primary crops (for career-ops geo scoring).

Writes: data/real/mo_crop_counties.json
  [{"county":"NODAWAY","lat":40.36,"lon":-94.88,"county_ansi":"147",
    "crops":["CORN","SOYBEANS","WHEAT"],"corn_rank":3, ...}, ...]

Also writes a copy to ~/Developer/jobs/career-ops/data/mo_crop_counties.json
if that directory exists.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "data" / "real"
OUT = DATA / "mo_crop_counties.json"
CAREER_OPS = Path.home() / "Developer" / "jobs" / "career-ops" / "data"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("export_mo_geo")


def main():
    centroids = pd.read_parquet(DATA / "mo_county_centroids.parquet")
    yields = pd.read_parquet(DATA / "usda_allcrops_county_missouri_2001_2023.parquet")

    # Average yield per (county, crop) across all years
    avg = yields.groupby(["county", "commodity"])["yield_value"].mean().reset_index()
    avg = avg.sort_values(["commodity", "yield_value"], ascending=[True, False])
    avg["rank"] = avg.groupby("commodity")["yield_value"].rank(ascending=False, method="min").astype(int)

    by_county: dict[str, dict] = {}
    for _, r in avg.iterrows():
        c = r["county"]
        by_county.setdefault(c, {"crops": [], "ranks": {}})
        by_county[c]["crops"].append(r["commodity"])
        by_county[c]["ranks"][r["commodity"]] = int(r["rank"])

    records = []
    for _, c in centroids.iterrows():
        meta = by_county.get(c["county"], {"crops": [], "ranks": {}})
        records.append({
            "county": c["county"],
            "county_ansi": c["county_ansi"],
            "lat": round(float(c["lat"]), 4),
            "lon": round(float(c["lon"]), 4),
            "crops": sorted(meta["crops"]),
            "corn_rank": meta["ranks"].get("CORN"),
            "soy_rank": meta["ranks"].get("SOYBEANS"),
            "wheat_rank": meta["ranks"].get("WHEAT"),
        })

    records.sort(key=lambda r: r["county"])
    OUT.write_text(json.dumps(records, indent=2))
    log.info("Wrote %s (%d counties)", OUT, len(records))

    if CAREER_OPS.exists():
        copy = CAREER_OPS / "mo_crop_counties.json"
        copy.write_text(json.dumps(records, indent=2))
        log.info("Also wrote %s", copy)


if __name__ == "__main__":
    main()
