"""
Unified Earth Engine auth.

Preference order:
  1. Service account (EE_SERVICE_ACCOUNT + EE_SA_KEY_FILE in .env) — headless,
     works in cron / CI / dash-served / notebook alike.
  2. User OAuth (~/.config/earthengine/credentials, created by
     `earthengine authenticate`) — convenient for one-off exploration.

All export scripts call init_ee() and don't care which path won.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import ee
from dotenv import load_dotenv

REPO = Path(__file__).resolve().parents[1]
load_dotenv(REPO / ".env")

log = logging.getLogger(__name__)


def init_ee() -> None:
    project = os.getenv("GCP_PROJECT", "agri-yield-pipeline")
    sa_email = os.getenv("EE_SERVICE_ACCOUNT")
    sa_key = os.getenv("EE_SA_KEY_FILE")

    if sa_email and sa_key and Path(sa_key).exists():
        log.info("Earth Engine: authenticating as service account %s", sa_email)
        creds = ee.ServiceAccountCredentials(sa_email, sa_key)
        ee.Initialize(credentials=creds, project=project)
        return

    try:
        ee.Initialize(project=project)
        log.info("Earth Engine: using user OAuth credentials")
    except Exception as e:
        raise RuntimeError(
            "Earth Engine auth failed. Either:\n"
            f"  1. Set EE_SERVICE_ACCOUNT + EE_SA_KEY_FILE in {REPO}/.env and drop the JSON key there, or\n"
            "  2. Run `earthengine authenticate` once in your terminal.\n"
            f"Original error: {e}"
        ) from e
