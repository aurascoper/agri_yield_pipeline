#!/usr/bin/env python3
"""
One-shot Sentinel-2 NDVI exporter for a fixed set of fields.

Why not MODIS: MODIS is 250 m × 16-day — too coarse for single-field
monitoring. Sentinel-2 is 10 m × 5-day and resolves individual fields.

Why not keep calling GEE every time: for a known list of AOIs, the right
pattern is export-once-cache-locally. This script does that: each scene
becomes a GeoTIFF under data/fields/<name>/ndvi_<date>.tif and a rolling
time-series parquet. Reruns only pull new scenes.

Prereqs:
    pip install earthengine-api geemap rioxarray
    earthengine authenticate       # one-time OAuth
    cp fields.yml.example fields.yml && edit

Run:
    .venv/bin/python scripts/export_fields_ndvi.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import ee
import geemap
import pandas as pd
import rioxarray
import yaml

REPO = Path(__file__).resolve().parents[1]
CFG_PATH = REPO / "fields.yml"
OUT_BASE = REPO / "data" / "fields"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("export_fields_ndvi")

sys.path.insert(0, str(REPO / "src"))
from ee_auth import init_ee  # noqa: E402


def field_bbox(lat: float, lon: float, buffer_m: int) -> ee.Geometry:
    return ee.Geometry.Point([lon, lat]).buffer(buffer_m).bounds()


def ndvi_from_s2(img: ee.Image) -> ee.Image:
    # SCL band values: 3=cloud shadow, 8=medium cloud, 9=high cloud, 10=cirrus
    scl = img.select("SCL")
    cloud_mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
    ndvi = img.normalizedDifference(["B8", "B4"]).rename("NDVI").updateMask(cloud_mask)
    return ndvi.copyProperties(img, ["system:time_start", "CLOUDY_PIXEL_PERCENTAGE"])


def collection_for(aoi: ee.Geometry, start: str, end: str, cloud_max: int) -> ee.ImageCollection:
    # Raw S2 L2A — NDVI is computed per-scene at export time so we keep
    # native scene IDs (post-map() images lose them).
    return (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(aoi)
        .filterDate(start, end)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", cloud_max))
    )


def scene_list(coll: ee.ImageCollection) -> list[dict]:
    ids = coll.aggregate_array("system:id").getInfo()
    times = coll.aggregate_array("system:time_start").getInfo()
    clouds = coll.aggregate_array("CLOUDY_PIXEL_PERCENTAGE").getInfo()
    return [
        {
            "id": i,
            "date": pd.to_datetime(t, unit="ms").strftime("%Y-%m-%d"),
            "cloud_pct": c,
        }
        for i, t, c in zip(ids, times, clouds)
    ]


def export_scene(image_id: str, date: str, aoi: ee.Geometry, out_tif: Path):
    img = ee.Image(ndvi_from_s2(ee.Image(image_id))).select("NDVI")
    # getDownloadURL limit is ~50 MB per call; 10 m × 100 ha = ~80 KB, safe.
    geemap.ee_export_image(
        img, filename=str(out_tif),
        scale=10, region=aoi, file_per_band=False,
    )


def build_series(field_dir: Path) -> pd.DataFrame:
    rows = []
    for tif in sorted(field_dir.glob("ndvi_*.tif")):
        date = tif.stem.replace("ndvi_", "")
        da = rioxarray.open_rasterio(tif, masked=True).squeeze()
        rows.append({
            "date": date,
            "ndvi_mean": float(da.mean().values),
            "ndvi_p10": float(da.quantile(0.1).values),
            "ndvi_p90": float(da.quantile(0.9).values),
            "n_valid_px": int((~da.isnull()).sum().values),
        })
    return pd.DataFrame(rows)


def main():
    if not CFG_PATH.exists():
        sys.exit(f"{CFG_PATH} missing — copy fields.yml.example and edit")
    cfg = yaml.safe_load(CFG_PATH.read_text())
    init_ee()

    for field in cfg["fields"]:
        name = field["name"]
        fdir = OUT_BASE / name
        fdir.mkdir(parents=True, exist_ok=True)

        aoi = field_bbox(field["lat"], field["lon"], field["buffer_m"])
        coll = collection_for(aoi, cfg["start_date"], cfg["end_date"], cfg["cloud_max_pct"])
        scenes = scene_list(coll)
        log.info("[%s] %d scenes available between %s and %s",
                 name, len(scenes), cfg["start_date"], cfg["end_date"])

        for s in scenes:
            out_tif = fdir / f"ndvi_{s['date']}.tif"
            if out_tif.exists():
                continue
            log.info("  exporting %s (%.0f%% cloud)", s["date"], s.get("cloud_pct") or 0)
            try:
                export_scene(s["id"], s["date"], aoi, out_tif)
            except Exception as e:
                log.warning("  %s failed: %s", s["date"], e)

        series = build_series(fdir)
        series.to_parquet(fdir / "ndvi_series.parquet", index=False)
        log.info("[%s] series: %d dates, latest NDVI=%.3f",
                 name, len(series), series["ndvi_mean"].iloc[-1] if len(series) else float("nan"))


if __name__ == "__main__":
    main()
