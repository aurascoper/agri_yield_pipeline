# Agricultural Yield Pipeline

This project implements a pipeline to fetch and analyze remote sensing NDVI and weather data for modeling grain yield in Missouri.

## Folder Structure

- src/: Core logic for data retrieval and modeling
- data/: Storage for raw and processed data
- notebooks/: Jupyter notebooks for exploration and analysis
- api/: FastAPI backend to serve NDVI and weather endpoints
- api/routes/: Route definitions
- README.md: Project overview and setup instructions

## Setup

1. Install dependencies:

```bash
pip install earthengine-api requests fastapi uvicorn pandas
```

2. Configure API keys and parameters in `src/config.py`.

3. Authenticate with Google Earth Engine:

```bash
earthengine authenticate
```

4. Start FastAPI server:

```bash
uvicorn api.main:app --reload
```

## API Usage

- `GET /ndvi/`: Fetch NDVI data for the configured bounding box and date range.
- `GET /weather/`: Fetch weather data for the configured station and date range.

## Contributing

Contributions welcome!