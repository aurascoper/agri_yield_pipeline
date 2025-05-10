"""
Routes for NDVI data.
"""

from fastapi import APIRouter
import src.ndvi as ndvi_module
import src.config as config

router = APIRouter()

@router.get("/")
def read_ndvi():
    """
    Endpoint to retrieve NDVI data.
    """
    data = ndvi_module.get_ndvi(config.BBOX, config.START_DATE, config.END_DATE)
    return {"ndvi": data}