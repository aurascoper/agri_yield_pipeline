#!/usr/bin/env python3
"""
Fetch MODIS NDVI for every Missouri county that appears in the USDA yield cache.

Centroids come from the US Census 2023 county gazetteer. Reuses
fetch_ndvi_modis() from scripts/fetch_real_data.py, which caches one parquet
per county under data/real/ndvi_modis_MOD13Q1_<county>_<start>_<end>.parquet.
Counties already cached are skipped.
"""

from __future__ import annotations

import io
import logging
import sys
import zipfile
from pathlib import Path

import pandas as pd
import requests

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))
from fetch_real_data import DATA, fetch_ndvi_modis  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fetch_all_counties")

GAZ_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2023_Gazetteer/2023_Gaz_counties_national.zip"
GAZ_CACHE = DATA / "mo_county_centroids.parquet"
START_YEAR = 2001
END_YEAR = 2023
KM = 15


def load_mo_centroids() -> pd.DataFrame:
    if GAZ_CACHE.exists():
        return pd.read_parquet(GAZ_CACHE)
    log.info("Downloading Census gazetteer…")
    r = requests.get(GAZ_URL, timeout=120)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    name = [n for n in z.namelist() if n.endswith(".txt")][0]
    df = pd.read_csv(z.open(name), sep="\t", dtype=str)
    df.columns = [c.strip() for c in df.columns]
    mo = df[df["USPS"] == "MO"].copy()
    mo["lat"] = mo["INTPTLAT"].astype(float)
    mo["lon"] = mo["INTPTLONG"].astype(float)
    mo["county"] = mo["NAME"].str.replace(" County", "", regex=False).str.upper().str.strip()
    mo["county_ansi"] = mo["GEOID"].str[-3:]
    out = mo[["county", "county_ansi", "lat", "lon"]].reset_index(drop=True)
    out.to_parquet(GAZ_CACHE, index=False)
    log.info("Cached %d MO county centroids → %s", len(out), GAZ_CACHE)
    return out


def main():
    yields_path = DATA / "usda_corn_county_missouri_1998_2023.parquet"
    yields = pd.read_parquet(yields_path)
    yield_counties = set(yields["county"].str.upper().unique())
    log.info("USDA has %d distinct counties with corn yield data", len(yield_counties))

    centroids = load_mo_centroids()
    todo = centroids[centroids["county"].isin(yield_counties)].sort_values("county").reset_index(drop=True)
    log.info("Fetching NDVI for %d counties (years %d-%d)", len(todo), START_YEAR, END_YEAR)

    for i, row in todo.iterrows():
        tag = row["county"].lower()
        cache = DATA / f"ndvi_modis_MOD13Q1_{tag}_{START_YEAR}_{END_YEAR}.parquet"
        if cache.exists():
            log.info("[%d/%d] %s: cached, skip", i + 1, len(todo), row["county"])
            continue
        log.info("[%d/%d] %s: fetching…", i + 1, len(todo), row["county"])
        try:
            fetch_ndvi_modis(row["lat"], row["lon"], START_YEAR, END_YEAR, KM, cache_tag=tag)
        except Exception as e:
            log.exception("Failed for %s: %s", row["county"], e)

    log.info("All done.")


if __name__ == "__main__":
    main()
