#!/usr/bin/env python3
"""Assert the numbers quoted in README.md still match the JSON artifacts.

Every claim checked here drifted at least once: the README carried variant B's
feature ranking as if it were the current model's, and residual counts from a
run that no longer exists. Prose is the thing that goes stale, so it gets a
check.

    python scripts/check_readme_numbers.py

Exits nonzero and names each mismatch. Add a line whenever the README starts
quoting a new number out of summary*.json. Matching is whitespace-insensitive,
so rewrapping a paragraph does not break it.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
FLAT = re.sub(r"\s+", " ", (REPO / "README.md").read_text())
KC = json.loads((REPO / "figures/real/summary.json").read_text())
DM = json.loads((REPO / "figures/real/summary_daymet.json").read_text())

failures: list[str] = []


def quoted(fragment: str, source: str) -> None:
    """README must contain `fragment`, which was formatted from `source`."""
    if re.sub(r"\s+", " ", fragment) not in FLAT:
        failures.append(f"{source}: README does not contain {fragment!r}")


d = DM["daymet_plus_year_fe"]
quoted(f"gives R² {d['cv_r2']:.3f}", "daymet_plus_year_fe.cv_r2")
quoted(f"gives **{DM['leave_one_year_out']['cv_r2']:.3f}**", "leave_one_year_out.cv_r2")
quoted(f"together **{DM['doubly_blocked']['cv_r2']:.3f}**", "doubly_blocked.cv_r2")

quoted(f"**{DM['counties_bias_within_2_buacre']} are within ±2 bu/acre of zero bias,"
       f" {DM['counties_bias_within_5_buacre']} within ±5, and"
       f" {DM['counties_bias_above_10_buacre']} exceed |10|**", "summary_daymet county bias counts")
quoted(f"mean residual is +{DM['statewide_mean_residual_buacre']:.1f} bu/acre",
       "summary_daymet.statewide_mean_residual_buacre")
quoted(f"worse on all three: {KC['counties_bias_within_2_buacre']} /"
       f" {KC['counties_bias_within_5_buacre']} /"
       f" {KC['counties_bias_above_10_buacre']}", "summary.json county bias counts")

for name, imp in list(d["top_features"].items())[:5]:
    quoted(f"`{name}` ({imp:.2f})", f"daymet_plus_year_fe.top_features.{name}")

# Structural: each file must still say which model it describes and where its
# residual counts came from. Without these the numbers above are unattributable.
if KC.get("superseded_by") != "summary_daymet.json":
    failures.append("summary.json: lost its superseded_by marker")
for blob, name in ((KC, "summary.json"), (DM, "summary_daymet.json")):
    if "held-out" not in blob.get("residual_basis", ""):
        failures.append(f"{name}: residual_basis missing or no longer says held-out")

if failures:
    print("README drifted from the artifacts:", file=sys.stderr)
    for f in failures:
        print("  -", f, file=sys.stderr)
    sys.exit(1)
print("README matches summary.json and summary_daymet.json")
