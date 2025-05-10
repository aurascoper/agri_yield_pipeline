"""
Fetch NDVI from Sentinel-2 using Google Earth Engine.
"""

import ee

def init_ee():
    """Initialize the Earth Engine client."""
    ee.Initialize()

def get_ndvi(bbox: list, start_date: str, end_date: str):
    """
    Fetch the NDVI time series for a given bounding box and date range.
    Returns a list of dicts with 'date' and 'NDVI' keys.
    """
    init_ee()
    geometry = ee.Geometry.Rectangle(bbox)
    collection = (
        ee.ImageCollection('COPERNICUS/S2_SR')
        .filterBounds(geometry)
        .filterDate(start_date, end_date)
        .map(lambda img: img.normalizedDifference(['B8', 'B4']).rename('NDVI'))
    )

    def reduce_region(img):
        stats = img.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geometry,
            scale=10
        )
        return ee.Feature(None, {
            'date': img.date().format(),
            'NDVI': stats.get('NDVI')
        })

    features = collection.map(reduce_region)
    return features.getInfo()