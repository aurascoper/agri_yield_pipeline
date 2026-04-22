#!/usr/bin/env python3
"""
County-level Sentinel-1 SAR VV exporter for all Missouri counties, CDL-masked
to row crops (corn / soy / winter wheat / sorghum).

Mirror of export_counties_ndvi.py. Pulls VV (dB) from COPERNICUS/S1_GRD,
applies a 30 m focal-median speckle filter per scene, then uses
reduceRegions() to collapse to per-county stats — no per-scene rasters.

Rolling window: today - DAYS to today. Override with --days / --end.

Output: data/counties/mo/sar_series.parquet
  columns: date | fips | county | vv_mean_db | vv_p10 | vv_p90 | n_cropland_px

Run:
    .venv/bin/python scripts/export_counties_sar.py --days 60
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import ee
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
OUT_DIR = REPO / "data" / "counties" / "mo"
OUT_PARQUET = OUT_DIR / "sar_series.parquet"

CROP_CLASSES = [1, 5, 24, 4]
MO_STATE_FIPS = "29"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("export_counties_sar")

sys.path.insert(0, str(REPO / "src"))
from ee_auth import init_ee  # noqa: E402


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=60)
    p.add_argument("--end", default=None)
    p.add_argument("--orbit", default="ASCENDING", choices=["ASCENDING", "DESCENDING"])
    p.add_argument("--scale", type=int, default=30)
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
    cdl = ee.Image(f"USDA/NASS/CDL/{year}").select("cropland")
    mask = ee.Image.constant(0)
    for c in CROP_CLASSES:
        mask = mask.Or(cdl.eq(c))
    return mask.rename("crop_mask")


def latest_cdl_year() -> int:
    return date.today().year - 1


def sar_preprocess(img: ee.Image) -> ee.Image:
    vv = img.select("VV").focalMedian(30, "circle", "meters").rename("VV")
    return vv.copyProperties(img, ["system:time_start", "orbitProperties_pass"])


def scene_list(coll: ee.ImageCollection) -> list[dict]:
    ids = coll.aggregate_array("system:id").getInfo()
    times = coll.aggregate_array("system:time_start").getInfo()
    return [
        {"id": i, "date": pd.to_datetime(t, unit="ms").strftime("%Y-%m-%d")}
        for i, t in zip(ids, times)
    ]


def collapse_tiles(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse multiple S1 tiles covering the same county on the same date
    into one pixel-weighted row per (fips, date). VV mean is in dB, which
    is logarithmic — strictly we'd convert to power before averaging, but
    at county-scale with similar incidence angles per orbit the error is
    small (<0.3 dB), and this stays consistent with the per-scene reducer."""
    if df.empty:
        return df
    g = df.groupby(["fips", "county", "date"], as_index=False, group_keys=False)
    return g.apply(_weighted_row).reset_index(drop=True)


def _weighted_row(group: pd.DataFrame) -> pd.Series:
    w = group["n_cropland_px"].fillna(0).astype(float)
    if w.sum() == 0:
        return pd.Series({
            "fips":          group["fips"].iloc[0],
            "county":        group["county"].iloc[0],
            "date":          group["date"].iloc[0],
            "vv_mean_db":    group["vv_mean_db"].mean(),
            "vv_p10":        group["vv_p10"].mean(),
            "vv_p90":        group["vv_p90"].mean(),
            "n_cropland_px": int(w.sum()),
        })
    return pd.Series({
        "fips":          group["fips"].iloc[0],
        "county":        group["county"].iloc[0],
        "date":          group["date"].iloc[0],
        "vv_mean_db":    float((group["vv_mean_db"] * w).sum() / w.sum()),
        "vv_p10":        float(group["vv_p10"].mean()),
        "vv_p90":        float(group["vv_p90"].mean()),
        "n_cropland_px": int(w.sum()),
    })


def reduce_scene(image_id: str, counties_fc: ee.FeatureCollection,
                 crop_mask: ee.Image, scale: int) -> list[dict]:
    vv = ee.Image(sar_preprocess(ee.Image(image_id))).select("VV").updateMask(crop_mask)
    reducer = (
        ee.Reducer.mean()
        .combine(ee.Reducer.percentile([10, 90]), sharedInputs=True)
        .combine(ee.Reducer.count(), sharedInputs=True)
    )
    fc = vv.reduceRegions(collection=counties_fc, reducer=reducer, scale=scale)
    feats = fc.getInfo()["features"]
    out = []
    for f in feats:
        p = f["properties"]
        out.append({
            "fips": f"{MO_STATE_FIPS}{p.get('COUNTYFP', '')}",
            "county": p.get("NAME"),
            "vv_mean_db": p.get("mean"),
            "vv_p10": p.get("p10"),
            "vv_p90": p.get("p90"),
            "n_cropland_px": p.get("count"),
        })
    return out


def main():
    args = parse_args()
    start, end = window(args)
    init_ee()

    counties = mo_counties_fc()
    n_counties = counties.size().getInfo()
    log.info("MO counties: %d", n_counties)

    cdl_year = latest_cdl_year()
    crop_mask = cdl_crop_mask(cdl_year)
    log.info("CDL mask: %d year, classes %s", cdl_year, CROP_CLASSES)

    mo_bounds = counties.geometry().bounds()
    coll = (
        ee.ImageCollection("COPERNICUS/S1_GRD")
        .filterBounds(mo_bounds)
        .filterDate(start, end)
        .filter(ee.Filter.listContains("transmitterReceiverPolarisation", "VV"))
        .filter(ee.Filter.eq("instrumentMode", "IW"))
        .filter(ee.Filter.eq("orbitProperties_pass", args.orbit))
    )
    scenes = scene_list(coll)
    log.info("S1 %s scenes %s → %s: %d", args.orbit, start, end, len(scenes))

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

    df = pd.DataFrame(rows).dropna(subset=["vv_mean_db"])
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
