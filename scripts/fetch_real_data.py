#!/usr/bin/env python3
"""
Replace the notebook's synthetic generators with real NDVI + weather + yield data.

Pulls monthly mean NDVI from Sentinel-2 L2A via Google Earth Engine,
NOAA GHCND daily weather, and USDA NASS county/state corn yields.
Caches each as parquet under data/real/ so reruns don't re-hit APIs.

Usage:
    .venv/bin/python scripts/fetch_real_data.py --start-year 2015 --end-year 2023
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv

# Core 2 Duo lacks SSE4.2/popcnt required by pyarrow — use fastparquet
PARQUET_ENGINE = "fastparquet"
_orig_to_parquet = pd.DataFrame.to_parquet
_orig_read_parquet = pd.read_parquet
pd.DataFrame.to_parquet = lambda self, path, **kw: _orig_to_parquet(self, path, engine=PARQUET_ENGINE, **{k:v for k,v in kw.items() if k!='engine'})
def _read(path, **kw): return _orig_read_parquet(path, engine=PARQUET_ENGINE, **{k:v for k,v in kw.items() if k!='engine'})
pd.read_parquet = _read

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "data" / "real"
DATA.mkdir(parents=True, exist_ok=True)

load_dotenv(REPO / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fetch_real_data")

# NW Missouri corn-belt bbox: Atchison/Nodaway/Holt counties
DEFAULT_BBOX = [-95.77, 40.0, -94.5, 40.6]

# Missouri corn-belt counties with centroids and NASS county codes (state_ansi=29)
# These are the top corn-producing NW counties where NDVI is most informative
DEFAULT_COUNTIES = [
    {"name": "ATCHISON",  "lat": 40.43, "lon": -95.43, "county_ansi": "005"},
    {"name": "NODAWAY",   "lat": 40.36, "lon": -94.88, "county_ansi": "147"},
    {"name": "HOLT",      "lat": 40.09, "lon": -95.21, "county_ansi": "087"},
    {"name": "ANDREW",    "lat": 39.98, "lon": -94.80, "county_ansi": "003"},
    {"name": "GENTRY",    "lat": 40.22, "lon": -94.41, "county_ansi": "075"},
    {"name": "WORTH",     "lat": 40.48, "lon": -94.42, "county_ansi": "227"},
]
GCP_PROJECT = os.getenv("GCP_PROJECT", "agri-yield-pipeline")
NOAA_TOKEN = os.getenv("NOAA_API_TOKEN") or os.getenv("NOAA_TOKEN")
USDA_KEY = os.getenv("USDA_API_KEY") or os.getenv("USDA_TOKEN")
NOAA_STATION = os.getenv("NOAA_STATION_ID", "GHCND:USW00013993")  # KC MCI


# ---------------------------------------------------------------------------
# NDVI via MODIS (ORNL DAAC REST subset — no auth needed)
# ---------------------------------------------------------------------------

MODIS_URL = "https://modis.ornl.gov/rst/api/v1"


def fetch_ndvi_modis(lat: float, lon: float, start_year: int, end_year: int,
                     km_radius: int = 5, product: str = "MOD13Q1",
                     cache_tag: str = "") -> pd.DataFrame:
    """
    Fetch MODIS 16-day NDVI composite for a lat/lon point ± km_radius.
    MOD13Q1 = Terra, 250m, 16-day. Returns one row per composite date
    with mean NDVI over the pixel window.
    Scale factor 0.0001; fill value -3000.
    """
    tag = f"_{cache_tag}" if cache_tag else ""
    cache = DATA / f"ndvi_modis_{product}{tag}_{start_year}_{end_year}.parquet"
    if cache.exists():
        log.info("MODIS NDVI cached: %s", cache)
        return pd.read_parquet(cache)

    # ORNL API caps at 10 composite dates per request. For yield modeling we
    # only need growing-season NDVI (Mar-Oct). Two chunks per year covers it.
    all_rows = []
    chunks = []
    for year in range(start_year, end_year + 1):
        chunks.append((f"A{year}065", f"A{year}177"))  # Mar 6 – Jun 26
        chunks.append((f"A{year}193", f"A{year}305"))  # Jul 12 – Nov 1

    for sd, ed in chunks:
        params = {
            "latitude": lat,
            "longitude": lon,
            "startDate": sd,
            "endDate": ed,
            "kmAboveBelow": km_radius,
            "kmLeftRight": km_radius,
            "band": "250m_16_days_NDVI",
        }
        subset = None
        for attempt in range(3):
            try:
                r = requests.get(f"{MODIS_URL}/{product}/subset", params=params, timeout=240)
                if r.status_code != 200:
                    log.warning("MODIS %s-%s HTTP %d: %s", sd, ed, r.status_code, r.text[:120])
                    break
                subset = r.json().get("subset", [])
                break
            except Exception as e:
                log.warning("MODIS request error %s-%s attempt %d: %s", sd, ed, attempt + 1, e)
                time.sleep(30)
        if subset is None:
            continue

        for rec in subset:
            raw = rec.get("data", [])
            # drop fill values (-3000), then scale
            valid = [v for v in raw if v > -3000]
            if not valid:
                continue
            mean_ndvi = (sum(valid) / len(valid)) * 0.0001
            cal = rec.get("calendar_date") or rec.get("calendarDate")
            all_rows.append({"date": cal, "NDVI": mean_ndvi, "n_pixels": len(valid)})
        log.info("  MODIS %s→%s: %d composite dates", sd, ed, len(subset))
        time.sleep(0.5)

    if not all_rows:
        raise RuntimeError("MODIS returned zero rows")

    df = pd.DataFrame(all_rows).drop_duplicates("date")
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df = df.sort_values("date").reset_index(drop=True)
    df.to_parquet(cache, index=False)
    log.info("MODIS NDVI saved: %s (%d rows)", cache, len(df))
    return df


# ---------------------------------------------------------------------------
# NOAA GHCND weather
# ---------------------------------------------------------------------------

def fetch_noaa_daily(station: str, start_year: int, end_year: int) -> pd.DataFrame:
    """
    Fetch GHCND daily weather via the NCEI bulk CSV endpoint (no token, no rate limit).
    Returns wide-format: year, date, TMAX, TMIN, PRCP (degrees C, mm).
    """
    # station format: GHCND:USW00013993 → USW00013993
    sid = station.split(":")[-1]
    cache = DATA / f"noaa_{sid}_{start_year}_{end_year}.parquet"
    if cache.exists():
        log.info("NOAA cached: %s", cache)
        return pd.read_parquet(cache)

    url = f"https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/{sid}.csv"
    log.info("Downloading GHCND bulk CSV: %s", url)
    r = requests.get(url, timeout=180)
    r.raise_for_status()

    import io
    df = pd.read_csv(io.StringIO(r.text))
    df["DATE"] = pd.to_datetime(df["DATE"])
    df = df[(df["DATE"].dt.year >= start_year) & (df["DATE"].dt.year <= end_year)].copy()
    df.rename(columns={"DATE": "date"}, inplace=True)
    df["year"] = df["date"].dt.year

    keep = ["year", "date"] + [c for c in ("TMAX", "TMIN", "PRCP") if c in df.columns]
    out = df[keep].copy()
    # GHCND bulk: TMAX/TMIN already in °C, PRCP in mm (new format — v2 was tenths; v3+ is SI)
    # If values look like tenths (|tmax| > 80 commonly), scale.
    if "TMAX" in out.columns and out["TMAX"].abs().median() > 80:
        for col in ("TMAX", "TMIN", "PRCP"):
            if col in out.columns:
                out[col] = out[col] / 10.0
    out.to_parquet(cache, index=False)
    log.info("NOAA saved: %s (%d rows)", cache, len(out))
    return out


# ---------------------------------------------------------------------------
# USDA NASS corn yields
# ---------------------------------------------------------------------------

def fetch_usda_corn_yields(state: str = "MISSOURI", start_year: int = 2010, end_year: int = 2023,
                           agg_level: str = "STATE") -> pd.DataFrame:
    tag = "state" if agg_level == "STATE" else "county"
    cache = DATA / f"usda_corn_{tag}_{state.lower()}_{start_year}_{end_year}.parquet"
    if cache.exists():
        log.info("USDA cached: %s", cache)
        return pd.read_parquet(cache)

    if not USDA_KEY:
        raise RuntimeError("USDA key missing")

    url = "https://quickstats.nass.usda.gov/api/api_GET/"
    params = {
        "key": USDA_KEY,
        "source_desc": "SURVEY",
        "commodity_desc": "CORN",
        "statisticcat_desc": "YIELD",
        "unit_desc": "BU / ACRE",
        "agg_level_desc": agg_level,
        "state_name": state,
        "year__GE": start_year,
        "year__LE": end_year,
        "format": "JSON",
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json().get("data", [])
    if not data:
        raise RuntimeError("USDA returned zero rows")

    df = pd.DataFrame(data)
    df = df[df["Value"].astype(str).str.replace(",", "").str.match(r"^\d+\.?\d*$")].copy()
    df["year"] = df["year"].astype(int)
    df["yield_bu_acre"] = df["Value"].str.replace(",", "").astype(float)
    df = df[df["reference_period_desc"] == "YEAR"].copy()

    if agg_level == "COUNTY":
        df["county"] = df["county_name"].str.upper().str.strip()
        out = df[["year", "county", "county_ansi", "yield_bu_acre"]].drop_duplicates(
            ["year", "county"]).sort_values(["county", "year"]).reset_index(drop=True)
    else:
        out = df[["year", "yield_bu_acre"]].drop_duplicates("year").sort_values("year").reset_index(drop=True)

    out.to_parquet(cache, index=False)
    log.info("USDA saved: %s (%d rows)", cache, len(out))
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-year", type=int, default=2001)
    ap.add_argument("--end-year", type=int, default=2023)
    ap.add_argument("--station", default=NOAA_STATION)
    ap.add_argument("--state", default="MISSOURI")
    ap.add_argument("--skip-ndvi", "--skip-ee", dest="skip_ndvi", action="store_true")
    ap.add_argument("--skip-noaa", action="store_true")
    ap.add_argument("--skip-usda", action="store_true")
    ap.add_argument("--ndvi-km", type=int, default=15, help="± km around each county centroid")
    ap.add_argument("--county-level", action="store_true", default=True,
                    help="Fetch per-county NDVI + county-level USDA yields")
    ap.add_argument("--state-level", dest="county_level", action="store_false",
                    help="Fall back to state-level aggregation")
    args = ap.parse_args()

    ndvi_list = []
    if not args.skip_ndvi:
        if args.county_level:
            for c in DEFAULT_COUNTIES:
                log.info("Fetching MODIS NDVI for %s county", c["name"])
                df = fetch_ndvi_modis(
                    c["lat"], c["lon"], args.start_year, args.end_year,
                    args.ndvi_km, cache_tag=c["name"].lower(),
                )
                df = df.copy()
                df["county"] = c["name"]
                df["county_ansi"] = c["county_ansi"]
                ndvi_list.append(df)
            ndvi = pd.concat(ndvi_list, ignore_index=True) if ndvi_list else None
        else:
            lat = sum(c["lat"] for c in DEFAULT_COUNTIES) / len(DEFAULT_COUNTIES)
            lon = sum(c["lon"] for c in DEFAULT_COUNTIES) / len(DEFAULT_COUNTIES)
            ndvi = fetch_ndvi_modis(lat, lon, args.start_year, args.end_year, args.ndvi_km)
    else:
        ndvi = None

    weather = None if args.skip_noaa else fetch_noaa_daily(args.station, args.start_year, args.end_year)

    if not args.skip_usda:
        agg = "COUNTY" if args.county_level else "STATE"
        yields = fetch_usda_corn_yields(args.state, args.start_year - 3, args.end_year, agg_level=agg)
    else:
        yields = None

    log.info("Done.")
    if ndvi is not None: log.info("  NDVI rows: %d (counties: %s)", len(ndvi),
                                  ndvi["county"].nunique() if "county" in ndvi.columns else 1)
    if weather is not None: log.info("  Weather rows: %d", len(weather))
    if yields is not None:
        counties = yields["county"].nunique() if "county" in yields.columns else 1
        log.info("  Yield rows: %d (counties: %d)", len(yields), counties)


if __name__ == "__main__":
    sys.path.insert(0, str(REPO))
    main()
