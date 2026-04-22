#!/usr/bin/env python3
"""
County-level Sentinel-2 NDVI exporter for all Missouri counties, CDL-masked
to row crops (corn / soy / winter wheat / sorghum).

Why county + CDL mask: county polygons are 500-2500 km² of mixed cover —
raw county-mean NDVI is a landcover-composition number, not a crop-health
number. Masking to the USDA Cropland Data Layer row-crop classes turns
it back into an agronomic signal.

Why no per-scene TIFs: at 114 counties × ~12 scenes/run, caching rasters
is wasteful. This exporter uses ee.Image.reduceRegions() to collapse each
scene directly to per-county stats server-side and only pulls the numbers.

Rolling window: by default, today - DAYS to today. Override with --days N
or --end YYYY-MM-DD.

Output: data/counties/mo/ndvi_series.parquet
  columns: date | fips | county | ndvi_mean | ndvi_p10 | ndvi_p90 | n_cropland_px

Prereqs: same as export_fields_ndvi.py. Uses src/ee_auth.init_ee().

Run:
    .venv/bin/python scripts/export_counties_ndvi.py --days 60
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import ee
import pandas as pd
import yaml

REPO = Path(__file__).resolve().parents[1]
OUT_DIR = REPO / "data" / "counties" / "mo"
OUT_PARQUET = OUT_DIR / "ndvi_series.parquet"

# Missouri row crops (USDA CDL class IDs).
# 1=Corn, 5=Soybeans, 24=Winter Wheat, 4=Sorghum.
CROP_CLASSES = [1, 5, 24, 4]

MO_STATE_FIPS = "29"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("export_counties_ndvi")

sys.path.insert(0, str(REPO / "src"))
from ee_auth import init_ee  # noqa: E402


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=60, help="rolling window (days back from --end)")
    p.add_argument("--end", default=None, help="end date YYYY-MM-DD (default: today)")
    p.add_argument("--cloud-max", type=int, default=40, help="max S2 cloud %")
    p.add_argument("--scale", type=int, default=30, help="reduceRegions scale (m)")
    return p.parse_args()


def window(args) -> tuple[str, str]:
    end = date.fromisoformat(args.end) if args.end else date.today()
    start = end - timedelta(days=args.days)
    return start.isoformat(), end.isoformat()


def mo_counties_fc() -> ee.FeatureCollection:
    return (
        ee.FeatureCollection("TIGER/2018/Counties")
        .filter(ee.Filter.eq("STATEFP", MO_STATE_FIPS))
    )


def cdl_crop_mask(year: int) -> ee.Image:
    """Binary mask: 1 where CDL class ∈ CROP_CLASSES, else 0."""
    cdl = ee.Image(f"USDA/NASS/CDL/{year}").select("cropland")
    mask = ee.Image.constant(0)
    for c in CROP_CLASSES:
        mask = mask.Or(cdl.eq(c))
    return mask.rename("crop_mask")


def latest_cdl_year() -> int:
    """CDL publishes ~Jan of year+1. Use year-1 for safety."""
    return date.today().year - 1


def ndvi_from_s2(img: ee.Image) -> ee.Image:
    scl = img.select("SCL")
    cloud_mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
    ndvi = img.normalizedDifference(["B8", "B4"]).rename("NDVI").updateMask(cloud_mask)
    return ndvi.copyProperties(img, ["system:time_start", "CLOUDY_PIXEL_PERCENTAGE"])


def scene_list(coll: ee.ImageCollection) -> list[dict]:
    ids = coll.aggregate_array("system:id").getInfo()
    times = coll.aggregate_array("system:time_start").getInfo()
    return [
        {"id": i, "date": pd.to_datetime(t, unit="ms").strftime("%Y-%m-%d")}
        for i, t in zip(ids, times)
    ]


def collapse_tiles(df: pd.DataFrame) -> pd.DataFrame:
    """Multiple S2 MGRS tiles can cover the same county on the same date.
    Collapse duplicates to one row per (fips, date) with a pixel-weighted
    mean; p10/p90 use simple means (percentiles don't compose exactly but
    this is fine at county scale)."""
    if df.empty:
        return df
    g = df.groupby(["fips", "county", "date"], as_index=False, group_keys=False)
    return g.apply(_weighted_row).reset_index(drop=True)


def _weighted_row(group: pd.DataFrame) -> pd.Series:
    w = group["n_cropland_px"].fillna(0).astype(float)
    if w.sum() == 0:
        # Fallback: unweighted mean when pixel counts are missing.
        return pd.Series({
            "fips":          group["fips"].iloc[0],
            "county":        group["county"].iloc[0],
            "date":          group["date"].iloc[0],
            "ndvi_mean":     group["ndvi_mean"].mean(),
            "ndvi_p10":      group["ndvi_p10"].mean(),
            "ndvi_p90":      group["ndvi_p90"].mean(),
            "n_cropland_px": int(w.sum()),
        })
    return pd.Series({
        "fips":          group["fips"].iloc[0],
        "county":        group["county"].iloc[0],
        "date":          group["date"].iloc[0],
        "ndvi_mean":     float((group["ndvi_mean"] * w).sum() / w.sum()),
        "ndvi_p10":      float(group["ndvi_p10"].mean()),
        "ndvi_p90":      float(group["ndvi_p90"].mean()),
        "n_cropland_px": int(w.sum()),
    })


def reduce_scene(image_id: str, counties_fc: ee.FeatureCollection,
                 crop_mask: ee.Image, scale: int) -> list[dict]:
    """One reduceRegions call → 114 county-level rows for one S2 scene."""
    ndvi = ee.Image(ndvi_from_s2(ee.Image(image_id))).select("NDVI").updateMask(crop_mask)
    # Combine mean + percentiles + valid-pixel count in one reducer.
    reducer = (
        ee.Reducer.mean()
        .combine(ee.Reducer.percentile([10, 90]), sharedInputs=True)
        .combine(ee.Reducer.count(), sharedInputs=True)
    )
    fc = ndvi.reduceRegions(collection=counties_fc, reducer=reducer, scale=scale)
    feats = fc.getInfo()["features"]
    out = []
    for f in feats:
        p = f["properties"]
        out.append({
            "fips": f"{MO_STATE_FIPS}{p.get('COUNTYFP', '')}",
            "county": p.get("NAME"),
            "ndvi_mean": p.get("mean"),
            "ndvi_p10": p.get("p10"),
            "ndvi_p90": p.get("p90"),
            "n_cropland_px": p.get("count"),
        })
    return out


def main():
    args = parse_args()
    start, end = window(args)
    init_ee()

    counties = mo_counties_fc()
    n_counties = counties.size().getInfo()
    log.info("MO counties loaded: %d", n_counties)

    cdl_year = latest_cdl_year()
    crop_mask = cdl_crop_mask(cdl_year)
    log.info("CDL mask: %d year, classes %s", cdl_year, CROP_CLASSES)

    mo_bounds = counties.geometry().bounds()
    coll = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(mo_bounds)
        .filterDate(start, end)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", args.cloud_max))
    )
    scenes = scene_list(coll)
    log.info("S2 scenes %s → %s over MO: %d", start, end, len(scenes))

    if not scenes:
        log.warning("no scenes — exiting without updating parquet")
        return

    rows = []
    for i, s in enumerate(scenes, 1):
        try:
            scene_rows = reduce_scene(s["id"], counties, crop_mask, args.scale)
            for r in scene_rows:
                r["date"] = s["date"]
                rows.append(r)
            log.info("  [%d/%d] %s: %d counties reduced", i, len(scenes), s["date"], len(scene_rows))
        except Exception as e:
            log.warning("  [%d/%d] %s failed: %s", i, len(scenes), s["date"], e)

    if not rows:
        log.warning("no rows reduced — exiting without updating parquet")
        return

    df = pd.DataFrame(rows).dropna(subset=["ndvi_mean"])
    df = collapse_tiles(df)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if OUT_PARQUET.exists():
        prev = pd.read_parquet(OUT_PARQUET)
        df = pd.concat([prev, df], ignore_index=True)
        df = collapse_tiles(df)
    df = df.sort_values(["fips", "date"]).reset_index(drop=True)

    df.to_parquet(OUT_PARQUET, index=False)
    log.info("wrote %s (%d rows, %d counties, %d scenes)",
             OUT_PARQUET, len(df), df["fips"].nunique(), df["date"].nunique())


if __name__ == "__main__":
    main()
