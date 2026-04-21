#!/usr/bin/env python3
"""
Fetch Daymet 1-km daily weather per Missouri county centroid.

Daymet is ORNL DAAC's gridded daily product (TMAX, TMIN, PRCP, etc.) with
1-km native resolution and a single-pixel REST endpoint that requires no
auth. Each county gets its own pixel, interpolated to the centroid lat/lon.

Writes: data/real/daymet_<county>_<start>_<end>.parquet
Columns: year | date | TMAX | TMIN | PRCP
(schema matches noaa_USW00013993_*.parquet so build_features works unchanged)

Run after scripts/fetch_real_data.py (which drops mo_county_centroids.parquet).
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import requests

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "data" / "real"
DATA.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fetch_daymet")

URL = "https://daymet.ornl.gov/single-pixel/api/data"


def fetch_county(lat: float, lon: float, start_year: int, end_year: int) -> pd.DataFrame:
    """Return a DataFrame(year, date, TMAX, TMIN, PRCP) for one lat/lon."""
    params = {
        "lat": f"{lat:.4f}",
        "lon": f"{lon:.4f}",
        "vars": "tmax,tmin,prcp",
        "start": f"{start_year}-01-01",
        "end": f"{end_year}-12-31",
        "format": "csv",
    }
    for attempt in range(3):
        try:
            r = requests.get(URL, params=params, timeout=120)
            if r.status_code == 200:
                lines = r.text.splitlines()
                # Daymet CSV header: a few metadata lines starting with "Latitude:" etc.,
                # then a blank line or directly the column header "year,yday,..."
                for i, line in enumerate(lines):
                    if line.lower().startswith("year,") and "yday" in line.lower():
                        body = "\n".join(lines[i:])
                        df = pd.read_csv(io.StringIO(body))
                        df.columns = [c.strip() for c in df.columns]
                        return df
                raise RuntimeError("no data header found")
            log.warning("HTTP %d @ %.3f,%.3f : %s", r.status_code, lat, lon, r.text[:120])
        except Exception as e:
            log.warning("attempt %d error @ %.3f,%.3f : %s", attempt + 1, lat, lon, e)
            time.sleep(10 * (attempt + 1))
    return pd.DataFrame()


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    prcp_col = next((c for c in df.columns if c.startswith("prcp")), None)
    tmax_col = next((c for c in df.columns if c.startswith("tmax")), None)
    tmin_col = next((c for c in df.columns if c.startswith("tmin")), None)
    if not all([prcp_col, tmax_col, tmin_col]):
        return pd.DataFrame()
    out = pd.DataFrame({
        "year": df["year"].astype(int),
        "yday": df["yday"].astype(int),
        "PRCP": df[prcp_col].astype(float),
        "TMAX": df[tmax_col].astype(float),
        "TMIN": df[tmin_col].astype(float),
    })
    out["date"] = pd.to_datetime(
        out["year"].astype(str) + out["yday"].astype(str).str.zfill(3),
        format="%Y%j",
    )
    return out[["year", "date", "TMAX", "TMIN", "PRCP"]].reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-year", type=int, default=2001)
    ap.add_argument("--end-year", type=int, default=2023)
    ap.add_argument("--sleep", type=float, default=1.5, help="delay between county requests")
    ap.add_argument("--only", default=None, help="run only this county (uppercase)")
    args = ap.parse_args()

    centroids_path = DATA / "mo_county_centroids.parquet"
    if not centroids_path.exists():
        sys.exit(f"{centroids_path} not found — run scripts/fetch_real_data.py first")
    centroids = pd.read_parquet(centroids_path)
    if args.only:
        centroids = centroids[centroids["county"] == args.only.upper()]

    total = len(centroids)
    done = skipped = failed = 0

    for i, c in enumerate(centroids.itertuples(index=False), 1):
        county = c.county
        cache = DATA / f"daymet_{county.lower().replace(' ', '_')}_{args.start_year}_{args.end_year}.parquet"
        if cache.exists():
            log.info("[%d/%d] %s — cached", i, total, county)
            skipped += 1
            continue

        log.info("[%d/%d] %s @ %.4f,%.4f", i, total, county, c.lat, c.lon)
        raw = fetch_county(c.lat, c.lon, args.start_year, args.end_year)
        df = normalize(raw)
        if df.empty:
            log.warning("  %s: empty response", county)
            failed += 1
            continue
        df.to_parquet(cache, index=False)
        log.info("  %s saved: %d days, TMAX̄=%.1f°C PRCP̄=%.2fmm/day",
                 county, len(df), df["TMAX"].mean(), df["PRCP"].mean())
        done += 1
        time.sleep(args.sleep)

    log.info("Daymet fetch complete — new: %d, cached: %d, failed: %d, total: %d",
             done, skipped, failed, total)


if __name__ == "__main__":
    main()
