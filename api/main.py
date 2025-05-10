"""
FastAPI application entrypoint.
"""

from fastapi import FastAPI
from api.routes.ndvi import router as ndvi_router
from api.routes.weather import router as weather_router

app = FastAPI(
    title="Agricultural Yield Pipeline API",
    description="API for fetching NDVI and weather data for yield modeling",
    version="0.1.0"
)

app.include_router(ndvi_router, prefix="/ndvi", tags=["ndvi"])
app.include_router(weather_router, prefix="/weather", tags=["weather"])