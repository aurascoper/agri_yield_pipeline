"""
Layer 1 → Layer 2 stress alerts.

Layer 1 is the statewide historical corpus (MODIS MOD13Q1 county NDVI +
USDA yields, 2001–2023). Layer 2 is the live per-field Sentinel-2 NDVI
stream that `scripts/export_fields_ndvi.py` caches under `data/fields/`.

This module grounds Layer 2 observations in Layer 1 history:

    "Field X is μ σ away from its county's 2001–2023 NDVI at DOY N."

That z-score is the first-draft anomaly signal. It's intentionally
sensor-imperfect — MODIS is 250 m / 16-day and S2 is 10 m / 5-day — but
the field-vs-county deviation is what an agronomist already reasons in
when they say "this corn is behind where Boone usually is in mid-July."

Upgrade paths (not implemented here):
  - Swap the county MOD13Q1 baseline for the field's own S2 prior years
    once ≥3 years of field history exist (kills the cross-sensor bias).
  - Score against predicted yield from the GBR model (Layer 1 feature
    vector → expected yield; observed NDVI → expected-vs-actual delta).
  - Add a VV anomaly rule (SAR dry spike) once the field has enough SAR
    history to compute a DOY baseline.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
REAL = REPO / "data" / "real"
FIELDS = REPO / "data" / "fields"

DOY_WINDOW = 7         # ± days around target DOY for baseline
MIN_BASELINE_N = 4     # need ≥ 4 historical obs to trust a z-score
Z_THRESHOLD = 1.5      # |z| above this emits an alert


@dataclass
class Alert:
    field: str
    date: str
    ndvi: float
    county: str
    county_mu: float
    county_sigma: float
    z: float
    severity: str       # "info" | "warn" | "stress"

    def as_ping(self) -> dict:
        return {
            "field": self.field,
            "date": self.date,
            "ndvi": round(self.ndvi, 3),
            "county": self.county.title(),
            "county_doy_mu": round(self.county_mu, 3),
            "county_doy_sigma": round(self.county_sigma, 3),
            "z": round(self.z, 2),
            "severity": self.severity,
            "message": self._message(),
        }

    def _message(self) -> str:
        direction = "below" if self.z < 0 else "above"
        return (
            f"{self.field}: NDVI {self.ndvi:.2f} on {self.date} is "
            f"{abs(self.z):.1f}σ {direction} the 2001–2023 {self.county.title()} "
            f"County mean of {self.county_mu:.2f} for DOY "
            f"{_date_doy(self.date)}."
        )


def _date_doy(date: str) -> int:
    return datetime.strptime(date, "%Y-%m-%d").timetuple().tm_yday


@lru_cache(maxsize=1)
def _centroids() -> pd.DataFrame:
    df = pd.read_parquet(REAL / "mo_county_centroids.parquet")
    df["county"] = df["county"].str.lower()
    return df


def nearest_county(lat: float, lon: float) -> str:
    """Crude nearest-centroid lookup. Good enough for MO sketches; swap
    for point-in-polygon (USA_adm2 shapefile) when tenants span state lines."""
    c = _centroids()
    d2 = (c["lat"] - lat) ** 2 + (c["lon"] - lon) ** 2
    return c.loc[d2.idxmin(), "county"]


@lru_cache(maxsize=128)
def _county_ndvi(county: str) -> pd.DataFrame:
    path = REAL / f"ndvi_modis_MOD13Q1_{county}_2001_2023.parquet"
    if not path.exists():
        raise FileNotFoundError(f"no Layer-1 NDVI for county '{county}' at {path}")
    df = pd.read_parquet(path)
    df["doy"] = pd.to_datetime(df["date"]).dt.dayofyear
    return df[["date", "doy", "NDVI"]].dropna()


def county_doy_baseline(county: str, doy: int) -> tuple[float, float, int]:
    df = _county_ndvi(county)
    lo, hi = doy - DOY_WINDOW, doy + DOY_WINDOW
    if lo < 1 or hi > 366:
        mask = ((df["doy"] - doy).abs() % 365) <= DOY_WINDOW
    else:
        mask = df["doy"].between(lo, hi)
    window = df.loc[mask, "NDVI"]
    return float(window.mean()), float(window.std(ddof=1)), int(len(window))


def _severity(z: float) -> str:
    a = abs(z)
    if a >= 2.0:
        return "stress"
    if a >= Z_THRESHOLD:
        return "warn"
    return "info"


def score_field(field_name: str, lat: float, lon: float,
                lookback_days: int = 21) -> list[Alert]:
    """Score the most recent `lookback_days` of a field's S2 NDVI series
    against its nearest MO county's 2001–2023 MOD13Q1 baseline."""
    series_path = FIELDS / field_name / "ndvi_series.parquet"
    if not series_path.exists():
        return []

    series = pd.read_parquet(series_path)
    if not len(series):
        return []

    series["date"] = pd.to_datetime(series["date"])
    cutoff = series["date"].max() - pd.Timedelta(days=lookback_days)
    recent = series[series["date"] >= cutoff]

    county = nearest_county(lat, lon)
    alerts: list[Alert] = []
    for _, row in recent.iterrows():
        doy = int(row["date"].dayofyear)
        mu, sigma, n = county_doy_baseline(county, doy)
        if n < MIN_BASELINE_N or sigma <= 0 or math.isnan(sigma):
            continue
        z = (row["ndvi_mean"] - mu) / sigma
        if abs(z) < Z_THRESHOLD:
            continue
        alerts.append(Alert(
            field=field_name,
            date=row["date"].strftime("%Y-%m-%d"),
            ndvi=float(row["ndvi_mean"]),
            county=county,
            county_mu=mu,
            county_sigma=sigma,
            z=float(z),
            severity=_severity(z),
        ))
    return alerts
