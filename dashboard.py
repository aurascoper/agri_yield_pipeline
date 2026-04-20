# dashboard.py
from flask import Flask, render_template
import requests
from dotenv import load_dotenv
import os
import openai

app = Flask(__name__)

# Configure API credentials
os.environ["USDA_API_KEY"] = "A43A1D4C-6EF0-32A8-A20F-A8AD1F05D236"
os.environ["NOAA_TOKEN"] = "iCsDRUrPOClfMaZJJAKgySPSQtVcKQFu"
openai.api_key = os.getenv("OPENAI_API_KEY")  # Set this in your environment

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/noaa/<station_id>')
def get_noaa_data(station_id):
    # Implementation similar to your ETL script
    params = {
        "datasetid": "GHCND",
        "datatypeid": ["TMAX", "TMIN", "PRCP", "AWND"],
        "stationid": station_id,
        "limit": 1000
    }
    response = requests.get(
        "https://www.ncei.noaa.gov/cdo-web/api/v2/data",
        headers={"token": os.environ["NOAA_TOKEN"]},
        params=params
    )
    return response.json()

@app.route('/usda/<fips>/<crop>')
def get_usda_data(fips, crop):
    # Implementation similar to your ETL script
    params = {
        "key": os.environ["USDA_API_KEY"],
        "commodity_desc": crop.upper(),
        "county_code": fips,
        "format": "JSON"
    }
    response = requests.get(
        "https://quickstats.nass.usda.gov/api/api_GET/",
        params=params
    )
    return response.json()

@app.route('/analyze/<prompt>')
def analyze_data(prompt):
    # Integrate with OpenAI Codex
    response = openai.Completion.create(
        engine="davinci-codex",
        prompt=f"Agricultural data analysis: {prompt}",
        max_tokens=100
    )
    return response.choices[0].text.strip()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
