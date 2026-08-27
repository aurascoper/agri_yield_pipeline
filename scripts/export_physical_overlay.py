#!/usr/bin/env python3
"""scripts/export_physical_overlay.py

Rev 6A physical-state overlay export -- the narrow, honest first pass. Exports what's actually
real in this repo (NDVI anomaly, a Daymet weather anomaly, county tmax-yield sensitivity, SAR as
an explicitly-stale reference snapshot), not the wider VPD/ET/soil-moisture/Bowen-ratio family
the README's roadmap mentions but which has zero code or data anywhere in this repo. Those are
scoped as Rev 6A.1 (hydrology / SMAP) and 6A.2 (surface energy / ERA5-Land) in the plan this was
built from -- fully specified there, not implemented here.

Reads real parquet files directly under data/real/, data/counties/mo/, and figures/real/ -- NOT
through any of this repo's three non-agreeing DB layers (SQLAlchemy ORM in models.py, raw
mysql.connector in api/main.py, the docker-compose Postgres/InfluxDB/Kafka/Redis stack) -- those
parquet files are the only consistently-real data surface this repo has.

Read-only with respect to the rest of this repo: writes only under data/overlay/, one new dated,
content-hashed file per run, never overwritten in place. Meant to be read -- hash-verified, not
imported -- by a separate geolocator dashboard; see the plan's "two-product boundary" for why
that dashboard reads this file directly rather than importing this repo's code.

Usage:
    .venv/bin/python scripts/export_physical_overlay.py
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
REAL = REPO / "data" / "real"
COUNTIES = REPO / "data" / "counties" / "mo"
CORRELATION_TABLE = REPO / "figures" / "real" / "daymet_correlation_per_county.parquet"
OVERLAY_DIR = REPO / "data" / "overlay"

DOY_WINDOW = 7        # matches src/stress_alerts.py's NDVI baseline window exactly
JULY_DOY = 182         # July 1 on a non-leap year
JULY_WINDOW = 15       # July anomaly window: DOY 182 +/- 15 (~mid-June to mid-August)
MIN_BASELINE_YEARS = 3


def sha256_hex(obj) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def git_head_sha() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True, text=True, timeout=10
        )
        return out.stdout.strip() if out.returncode == 0 else None
    except Exception:
        return None


def load_centroids() -> pd.DataFrame:
    df = pd.read_parquet(REAL / "mo_county_centroids.parquet")
    df["county"] = df["county"].str.lower()
    return df


def load_correlation_table() -> pd.DataFrame:
    df = pd.read_parquet(CORRELATION_TABLE)
    df["county"] = df["county"].str.lower()
    return df.set_index("county")


def county_ndvi_baseline(county: str) -> pd.DataFrame | None:
    path = REAL / f"ndvi_modis_MOD13Q1_{county}_2001_2023.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    df["doy"] = pd.to_datetime(df["date"]).dt.dayofyear
    return df[["date", "doy", "NDVI"]].dropna()


def ndvi_doy_baseline(df: pd.DataFrame, doy: int) -> tuple[float, float, int]:
    """Same +/-DOY_WINDOW-day window logic as src/stress_alerts.py:county_doy_baseline --
    reimplemented here (not imported) so this export has no runtime dependency on the
    stress-alert CLI's import chain (which currently fails locally on a missing pyyaml) and so
    this file's behavior stays pinned even if that module changes independently."""
    lo, hi = doy - DOY_WINDOW, doy + DOY_WINDOW
    if lo < 1 or hi > 366:
        mask = ((df["doy"] - doy).abs() % 365) <= DOY_WINDOW
    else:
        mask = df["doy"].between(lo, hi)
    window = df.loc[mask, "NDVI"]
    return float(window.mean()), float(window.std(ddof=1)), int(len(window))


def county_daymet(county: str) -> pd.DataFrame | None:
    path = REAL / f"daymet_{county}_2001_2023.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    df["doy"] = df["date"].dt.dayofyear
    return df


def july_anomaly(df: pd.DataFrame) -> dict | None:
    """The last complete year's July (DOY 182+/-15) TMAX/PRCP vs the prior years' same-window
    baseline. July is this pipeline's established hinge month (ndvi_july and tmax_july_mean are
    its top two yield-model features) -- and a July anomaly is the most this repo's data can
    honestly support as a "weather" field: no weather feed here (Daymet or the single-station
    NOAA series) extends past 2023-12-31, so there is no current/live weather to anomaly against.
    This is a dated historical data point, not today's weather -- see as_of."""
    window = df[(df["doy"] - JULY_DOY).abs() <= JULY_WINDOW]
    if window.empty:
        return None
    last_year = int(window["year"].max())
    current = window[window["year"] == last_year]
    baseline = window[window["year"] < last_year]
    if current.empty or baseline["year"].nunique() < MIN_BASELINE_YEARS:
        return None
    tmax_mu = float(baseline["TMAX"].mean())
    tmax_sigma = float(baseline["TMAX"].std(ddof=1))
    prcp_mu = float(baseline["PRCP"].mean())
    tmax_now = float(current["TMAX"].mean())
    return {
        "tmax_anomaly_c": tmax_now - tmax_mu,
        "tmax_z": (tmax_now - tmax_mu) / tmax_sigma if tmax_sigma > 0 else None,
        "precip_anomaly_mm": float(current["PRCP"].mean()) - prcp_mu,
        "baseline_years": int(baseline["year"].nunique()),
        "as_of": f"{last_year}-07",
    }


def latest_by_county(df: pd.DataFrame, value_col: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    df = df.copy()
    df["county_lower"] = df["county"].str.lower()
    for county, g in df.groupby("county_lower"):
        row = g.sort_values("date").iloc[-1]
        out[county] = {
            "value": float(row[value_col]),
            "as_of": pd.Timestamp(row["date"]).strftime("%Y-%m-%d"),
        }
    return out


def build_cells() -> list[dict]:
    centroids = load_centroids()
    corr = load_correlation_table()
    ndvi_latest = latest_by_county(pd.read_parquet(COUNTIES / "ndvi_series.parquet"), "ndvi_mean")
    sar_latest = latest_by_county(pd.read_parquet(COUNTIES / "sar_series.parquet"), "vv_mean_db")

    cells = []
    for _, row in centroids.iterrows():
        county = row["county"]
        cell = {
            "id": f"county:mo:{county}",
            "kind": "county",
            "name": county.title(),
            "state": "MO",
            "lat": float(row["lat"]),
            "lon": float(row["lon"]),
            "ndvi": None,
            "weather": None,
            "yield_sensitivity": None,
            "sar_vv_db": None,
        }

        ndvi_hist = county_ndvi_baseline(county)
        if ndvi_hist is not None and county in ndvi_latest:
            obs = ndvi_latest[county]
            doy = datetime.strptime(obs["as_of"], "%Y-%m-%d").timetuple().tm_yday
            mu, sigma, n = ndvi_doy_baseline(ndvi_hist, doy)
            cell["ndvi"] = {
                "value": obs["value"],
                "baseline_mu": mu, "baseline_sigma": sigma,
                "z": (obs["value"] - mu) / sigma if sigma > 0 else None,
                "n_baseline_years": n,
                "as_of": obs["as_of"],
            }

        daymet = county_daymet(county)
        if daymet is not None:
            cell["weather"] = july_anomaly(daymet)

        if county in corr.index:
            r = corr.loc[county]
            cell["yield_sensitivity"] = {
                "r_tmax_yield": float(r["r_tmax_yield"]),
                "r_prcp_yield": float(r["r_prcp_yield"]),
                "n_years": int(r["n_years"]),
                "source": "figures/real/daymet_correlation_per_county.parquet",
            }

        if county in sar_latest:
            cell["sar_vv_db"] = sar_latest[county]

        cells.append(cell)

    return cells


def main() -> int:
    OVERLAY_DIR.mkdir(parents=True, exist_ok=True)
    cells = build_cells()
    cells_hash = sha256_hex(cells)
    generated_at = datetime.now(timezone.utc).isoformat()

    payload = {
        "schema_version": 1,
        "source_repo": "agri_yield_pipeline",
        "source_git_sha": git_head_sha(),
        "generated_at": generated_at,
        "cells_sha256": cells_hash,
        "provenance": {
            "ndvi": {
                "class": "reference",
                "note": (
                    "County-level MODIS-derived snapshot. The weekly GEE refresh automation "
                    "(.github/workflows/refresh_counties.yml) has failed every run since "
                    "2026-04-27 -- missing GEE service-account secrets, and the workflow never "
                    "installs python-dotenv. This is a one-time manual export, not a live feed."
                ),
                "vintage_field": "per-cell ndvi.as_of",
            },
            "sar": {
                "class": "reference",
                "note": "Same broken-automation caveat as ndvi -- see provenance.ndvi.note.",
                "vintage_field": "per-cell sar_vv_db.as_of",
            },
            "weather": {
                "class": "reference",
                "note": (
                    "July anomaly vs prior years, computed from the last complete year in the "
                    "2001-2023 Daymet corpus. No weather feed in this repo (Daymet or the "
                    "single-station NOAA series) extends past 2023-12-31 -- this is a dated "
                    "historical data point, not current weather."
                ),
                "vintage_field": "per-cell weather.as_of",
            },
            "yield_sensitivity": {
                "class": "reference",
                "note": "Static historical correlation table, 2001-2023, unchanged per export.",
                "source": "figures/real/daymet_correlation_per_county.parquet",
            },
        },
        "cells": cells,
    }

    fname = f"agri_physical_overlay_{generated_at[:10]}_{cells_hash[:12]}.json"
    path = OVERLAY_DIR / fname
    path.write_text(json.dumps(payload, indent=2, sort_keys=False, allow_nan=False) + "\n")

    # A stable pointer for external readers (the geolocator dashboard). Dated exports are
    # never overwritten in place -- this symlink is what "latest" means, retargeted each run.
    # A changed symlink target is a real stat-level change (different inode/mtime), so the
    # geolocator's reload-on-stat-change logic picks it up correctly without any polling.
    latest = OVERLAY_DIR / "latest.json"
    if latest.is_symlink() or latest.exists():
        latest.unlink()
    latest.symlink_to(fname)

    print(f"wrote {path}")
    print(f"latest.json -> {fname}")
    print(f"cells: {len(cells)}")
    with_ndvi = sum(1 for c in cells if c["ndvi"] is not None)
    with_weather = sum(1 for c in cells if c["weather"] is not None)
    with_sensitivity = sum(1 for c in cells if c["yield_sensitivity"] is not None)
    with_sar = sum(1 for c in cells if c["sar_vv_db"] is not None)
    print(f"ndvi={with_ndvi} weather={with_weather} yield_sensitivity={with_sensitivity} sar={with_sar}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
