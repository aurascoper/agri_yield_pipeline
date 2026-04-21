#!/usr/bin/env python3
"""
Emit Layer-1-vs-Layer-2 stress pings for every field in fields.yml.

Layer 1: 2001–2023 MOD13Q1 county NDVI baseline (USDA/NOAA era).
Layer 2: live per-field Sentinel-2 NDVI from data/fields/<name>/.

Output: JSON to stdout (pipeable to Slack / email / dashboard).

    .venv/bin/python scripts/field_stress_alerts.py
    .venv/bin/python scripts/field_stress_alerts.py --lookback 14 > pings.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
from stress_alerts import score_field  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback", type=int, default=21,
                    help="days of recent field NDVI to score (default 21)")
    ap.add_argument("--config", type=Path, default=REPO / "fields.yml")
    args = ap.parse_args()

    if not args.config.exists():
        sys.exit(f"{args.config} missing — copy fields.yml.example and edit")
    cfg = yaml.safe_load(args.config.read_text())

    pings = []
    for field in cfg["fields"]:
        alerts = score_field(
            field["name"], field["lat"], field["lon"],
            lookback_days=args.lookback,
        )
        pings.extend(a.as_ping() for a in alerts)

    json.dump({"n_pings": len(pings), "pings": pings}, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
