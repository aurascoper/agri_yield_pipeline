#!/usr/bin/env python3
"""
Pre-render per-field NDVI + SAR tile previews as static PNGs.

Why: the Render-deployed dashboard doesn't ship GeoTIFFs (too large and
tenant-specific). But blank preview boxes look broken. This script
rasterizes the latest TIF per field to a small PNG once, so the demo
deployment shows real satellite imagery without needing rioxarray or
the full tif cache on the server.

Outputs: data/fields/<name>/ndvi_preview.png, sar_preview.png
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import rioxarray

REPO = Path(__file__).resolve().parents[1]
FIELDS = REPO / "data" / "fields"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tile_previews")


def latest_tif(d: Path, kind: str) -> Path | None:
    tifs = sorted(d.glob(f"{kind}_*.tif"))
    return tifs[-1] if tifs else None


def render(tif: Path, out: Path, cmap: str, vmin: float | None, vmax: float | None):
    da = rioxarray.open_rasterio(tif, masked=True).squeeze()
    fig, ax = plt.subplots(figsize=(4, 4), dpi=100)
    im = ax.imshow(da.values, cmap=cmap, vmin=vmin, vmax=vmax)
    ax.axis("off")
    fig.colorbar(im, ax=ax, shrink=0.7)
    ax.set_title(tif.stem.replace("_", " "), fontsize=9)
    fig.savefig(out, format="png", bbox_inches="tight")
    plt.close(fig)
    log.info("wrote %s (%.1f KB)", out, out.stat().st_size / 1024)


def main():
    fields = [d for d in FIELDS.iterdir() if d.is_dir()] if FIELDS.exists() else []
    if not fields:
        sys.exit("no data/fields/<name>/ dirs found — run exporters first")

    for d in fields:
        ndvi = latest_tif(d, "ndvi")
        sar = latest_tif(d, "sar")
        if ndvi:
            render(ndvi, d / "ndvi_preview.png", "RdYlGn", 0, 1)
        if sar:
            render(sar, d / "sar_preview.png", "viridis", None, None)


if __name__ == "__main__":
    main()
