"""
Routes for weather data.
"""

from fastapi import APIRouter
import src.weather as weather_module
import src.config as config

router = APIRouter()

@router.get("/")
def read_weather():
    """
    Endpoint to retrieve weather data.
    """
    data = weather_module.get_weather(config.START_DATE, config.END_DATE)
    return {"weather": data}