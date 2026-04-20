from sqlalchemy import (
    Column, String, Integer, Float, Date, ForeignKey, Numeric
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Location(Base):
    __tablename__ = 'location'
    location_id = Column(Integer, primary_key=True)
    county_name = Column(String(100))
    state = Column(String(50))
    fips_code = Column(String(10), unique=True)

    weather_stations = relationship("WeatherStation", back_populates="location")
    crop_yields = relationship("CropYield", back_populates="location")


class WeatherStation(Base):
    __tablename__ = 'weather_station'
    station_id = Column(String(20), primary_key=True)
    name = Column(String(100))
    latitude = Column(Float)
    longitude = Column(Float)
    location_id = Column(Integer, ForeignKey('location.location_id'))

    location = relationship("Location", back_populates="weather_stations")
    weather_events = relationship("WeatherEvent", back_populates="station")


class WeatherEvent(Base):
    __tablename__ = 'weather_event'
    weather_id = Column(Integer, primary_key=True)
    station_id = Column(String(20), ForeignKey('weather_station.station_id'))
    date = Column(Date, nullable=False)
    temperature_max = Column(Float)
    temperature_min = Column(Float)
    precipitation = Column(Float)
    wind_speed = Column(Float)

    station = relationship("WeatherStation", back_populates="weather_events")


class Season(Base):
    __tablename__ = 'season'
    season_id = Column(Integer, primary_key=True)
    name = Column(String(20))
    start_date = Column(Date)
    end_date = Column(Date)

    crop_yields = relationship("CropYield", back_populates="season")


class Crop(Base):
    __tablename__ = 'crop'
    crop_id = Column(Integer, primary_key=True)
    name = Column(String(50))
    type = Column(String(50))
    variety = Column(String(50))

    crop_yields = relationship("CropYield", back_populates="crop")


class CropYield(Base):
    __tablename__ = 'crop_yield'
    yield_id = Column(Integer, primary_key=True)
    location_id = Column(Integer, ForeignKey('location.location_id'))
    crop_id = Column(Integer, ForeignKey('crop.crop_id'))
    season_id = Column(Integer, ForeignKey('season.season_id'))
    year = Column(Integer)
    production = Column(Numeric)
    yield_per_acre = Column(Numeric)

    location = relationship("Location", back_populates="crop_yields")
    crop = relationship("Crop", back_populates="crop_yields")
    season = relationship("Season", back_populates="crop_yields")

