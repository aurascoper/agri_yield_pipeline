#!/usr/bin/env python3
"""
Sentinel-1 SAR backscatter exporter for a fixed set of fields.

Pulls VV polarization (dB) from the Sentinel-1 GRD collection, filtered to
a single orbit direction (descending by default) and IW mode for
consistency. Applies a 3x3 focal-median speckle filter on each scene.

What this isn't: a quantitative soil-moisture product. Real retrieval
needs canopy correction (WCM), LST, and per-field calibration. What it
is: a per-field backscatter *anomaly* signal — drier surface → lower
VV. An agronomist learns what "normal" VV looks like for a specific
field in ~3 weeks and reads deviations as possible water stress or
crop-stage change.

Writes per field:
  data/fields/<name>/sar_<YYYY-MM-DD>.tif     # VV dB, 10 m, cloud-free (no optics)
  data/fields/<name>/sar_series.parquet       # date | vv_mean_db | vv_p10 | vv_p90 | n_valid_px

Prereqs: same as export_fields_ndvi.py. Uses the shared src/ee_auth.init_ee().
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
log = logging.getLogger("export_fields_sar")

sys.path.insert(0, str(REPO / "src"))
from ee_auth import init_ee  # noqa: E402


def field_bbox(lat: float, lon: float, buffer_m: int) -> ee.Geometry:
    return ee.Geometry.Point([lon, lat]).buffer(buffer_m).bounds()


def sar_preprocess(img: ee.Image) -> ee.Image:
    # Sentinel-1 GRD VV is already in dB on GEE after 2021 reprocessing.
    # Focal-median speckle filter (simple, preserves edges better than mean).
    vv = img.select("VV").focalMedian(30, "circle", "meters").rename("VV")
    return vv.copyProperties(img, [
        "system:time_start", "orbitProperties_pass", "resolution_meters",
    ])


def collection_for(aoi: ee.Geometry, start: str, end: str, orbit: str) -> ee.ImageCollection:
    # Raw S1 GRD — speckle filter applied per-scene at export time so we
    # keep native scene IDs (post-map() images lose them).
    return (
        ee.ImageCollection("COPERNICUS/S1_GRD")
        .filterBounds(aoi)
        .filterDate(start, end)
        .filter(ee.Filter.listContains("transmitterReceiverPolarisation", "VV"))
        .filter(ee.Filter.eq("instrumentMode", "IW"))
        .filter(ee.Filter.eq("orbitProperties_pass", orbit))
    )


def scene_list(coll: ee.ImageCollection) -> list[dict]:
    ids = coll.aggregate_array("system:id").getInfo()
    times = coll.aggregate_array("system:time_start").getInfo()
    orbits = coll.aggregate_array("orbitProperties_pass").getInfo()
    return [
        {
            "id": i,
            "date": pd.to_datetime(t, unit="ms").strftime("%Y-%m-%d"),
            "orbit": o,
        }
        for i, t, o in zip(ids, times, orbits)
    ]


def export_scene(image_id: str, aoi: ee.Geometry, out_tif: Path):
    img = ee.Image(sar_preprocess(ee.Image(image_id))).select("VV")
    geemap.ee_export_image(
        img, filename=str(out_tif),
        scale=10, region=aoi, file_per_band=False,
    )


def build_series(field_dir: Path) -> pd.DataFrame:
    rows = []
    for tif in sorted(field_dir.glob("sar_*.tif")):
        date = tif.stem.replace("sar_", "")
        da = rioxarray.open_rasterio(tif, masked=True).squeeze()
        rows.append({
            "date": date,
            "vv_mean_db": float(da.mean().values),
            "vv_p10":     float(da.quantile(0.1).values),
            "vv_p90":     float(da.quantile(0.9).values),
            "n_valid_px": int((~da.isnull()).sum().values),
        })
    return pd.DataFrame(rows)


def main():
    if not CFG_PATH.exists():
        sys.exit(f"{CFG_PATH} missing — copy fields.yml.example and edit")
    cfg = yaml.safe_load(CFG_PATH.read_text())
    orbit = cfg.get("sar_orbit", "DESCENDING").upper()
    init_ee()

    for field in cfg["fields"]:
        name = field["name"]
        fdir = OUT_BASE / name
        fdir.mkdir(parents=True, exist_ok=True)

        aoi = field_bbox(field["lat"], field["lon"], field["buffer_m"])
        coll = collection_for(aoi, cfg["start_date"], cfg["end_date"], orbit)
        scenes = scene_list(coll)
        log.info("[%s] %d %s-orbit S1 scenes between %s and %s",
                 name, len(scenes), orbit, cfg["start_date"], cfg["end_date"])

        for s in scenes:
            out_tif = fdir / f"sar_{s['date']}.tif"
            if out_tif.exists():
                continue
            log.info("  exporting %s", s["date"])
            try:
                export_scene(s["id"], aoi, out_tif)
            except Exception as e:
                log.warning("  %s failed: %s", s["date"], e)

        series = build_series(fdir)
        series.to_parquet(fdir / "sar_series.parquet", index=False)
        if len(series):
            log.info("[%s] SAR series: %d dates, latest VV=%.2f dB",
                     name, len(series), series["vv_mean_db"].iloc[-1])


if __name__ == "__main__":
    main()
