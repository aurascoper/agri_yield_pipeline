#!/usr/bin/env python3
"""
Fetch USDA NASS county-level yield data for all major Missouri crops.

Writes data/real/usda_allcrops_county_missouri_<start>_<end>.parquet with columns:
    year | county | county_ansi | commodity | unit | yield_value

Runs independently of the NDVI/NOAA pipeline. Caches the full combined
parquet and one per-commodity parquet for inspection.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "data" / "real"
DATA.mkdir(parents=True, exist_ok=True)
load_dotenv(REPO / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fetch_usda_crops")

USDA_KEY = os.getenv("USDA_API_KEY") or os.getenv("USDA_TOKEN")
URL = "https://quickstats.nass.usda.gov/api/api_GET/"

# (commodity_desc, unit_desc). Units match what NASS publishes for MO yields.
CROPS = [
    ("CORN", "BU / ACRE"),
    ("SOYBEANS", "BU / ACRE"),
    ("WHEAT", "BU / ACRE"),
    ("COTTON", "LB / ACRE"),
    ("RICE", "LB / ACRE"),
    ("SORGHUM", "BU / ACRE"),
    ("OATS", "BU / ACRE"),
    ("HAY", "TONS / ACRE"),
    ("HAY, ALFALFA", "TONS / ACRE"),
]


def fetch_one(commodity: str, unit: str, state: str, start_year: int, end_year: int) -> pd.DataFrame:
    params = {
        "key": USDA_KEY,
        "source_desc": "SURVEY",
        "commodity_desc": commodity,
        "statisticcat_desc": "YIELD",
        "unit_desc": unit,
        "agg_level_desc": "COUNTY",
        "state_name": state,
        "year__GE": start_year,
        "year__LE": end_year,
        "format": "JSON",
    }
    for attempt in range(3):
        try:
            r = requests.get(URL, params=params, timeout=90)
            if r.status_code == 200:
                return pd.DataFrame(r.json().get("data", []))
            log.warning("%s HTTP %d: %s", commodity, r.status_code, r.text[:120])
            return pd.DataFrame()
        except Exception as e:
            log.warning("%s attempt %d error: %s", commodity, attempt + 1, e)
            time.sleep(15)
    return pd.DataFrame()


def clean(df: pd.DataFrame, commodity: str, unit: str) -> pd.DataFrame:
    if df.empty:
        return df
    df = df[df["Value"].astype(str).str.replace(",", "").str.match(r"^\d+\.?\d*$")].copy()
    if df.empty:
        return df
    df["year"] = df["year"].astype(int)
    df["yield_value"] = df["Value"].str.replace(",", "").astype(float)
    df = df[df["reference_period_desc"] == "YEAR"].copy()
    if "county_name" not in df.columns or df["county_name"].isna().all():
        return pd.DataFrame()
    df["county"] = df["county_name"].str.upper().str.strip()
    df["commodity"] = commodity
    df["unit"] = unit
    keep = ["year", "county", "county_ansi", "commodity", "unit", "yield_value"]
    return df[keep].drop_duplicates(["year", "county", "commodity"]).reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-year", type=int, default=2001)
    ap.add_argument("--end-year", type=int, default=2023)
    ap.add_argument("--state", default="MISSOURI")
    args = ap.parse_args()

    if not USDA_KEY:
        sys.exit("USDA_API_KEY / USDA_TOKEN missing in .env")

    frames = []
    for commodity, unit in CROPS:
        per_cache = DATA / f"usda_{commodity.replace(', ','_').replace(' ','_').lower()}_county_{args.state.lower()}_{args.start_year}_{args.end_year}.parquet"
        if per_cache.exists():
            log.info("%s cached", commodity)
            frames.append(pd.read_parquet(per_cache))
            continue
        log.info("Fetching %s (%s)…", commodity, unit)
        raw = fetch_one(commodity, unit, args.state, args.start_year, args.end_year)
        df = clean(raw, commodity, unit)
        if df.empty:
            log.warning("%s: 0 rows", commodity)
            continue
        df.to_parquet(per_cache, index=False)
        log.info("%s saved: %d rows, %d counties", commodity, len(df), df["county"].nunique())
        frames.append(df)
        time.sleep(1)  # be polite to NASS

    if not frames:
        sys.exit("no crop data fetched")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(["commodity", "county", "year"]).reset_index(drop=True)
    out = DATA / f"usda_allcrops_county_{args.state.lower()}_{args.start_year}_{args.end_year}.parquet"
    combined.to_parquet(out, index=False)

    log.info("Combined saved: %s", out)
    log.info("  rows: %d | crops: %d | counties: %d",
             len(combined), combined["commodity"].nunique(), combined["county"].nunique())
    summary = combined.groupby("commodity").agg(
        rows=("yield_value", "size"),
        counties=("county", "nunique"),
        years=("year", "nunique"),
    )
    log.info("\n%s", summary)


if __name__ == "__main__":
    main()
