import requests
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import time
import json

class USDANOAAIntegrator:
    def __init__(self, usda_api_key, noaa_api_key):
        """
        Initialize with API keys
        USDA API key: Register at api.data.gov
        NOAA API key: Register at ncdc.noaa.gov/cdo-web/token
        """
        self.usda_api_key = "A43A1D4C-6EF0-32A8-A20F-A8AD1F05D236"
        self.noaa_api_key = "iCsDRUrPOClfMaZJJAKgySPSQtVcKQFu"
        
        # API Base URLs
        self.usda_base_url = "https://quickstats.nass.usda.gov/api"
        self.noaa_base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2"
        
        # Headers
        self.noaa_headers = {"token": self.noaa_api_key}
        
    def get_usda_crop_data(self, state_code=None, commodity=None, year=None, county_code=None):
        """
        Fetch USDA crop production data
        """
        params = {
            "key": self.usda_api_key,
            "source_desc": "SURVEY",
            "format": "JSON"
        }
        
        if state_code:
            params["state_alpha"] = state_code
        if commodity:
            params["commodity_desc"] = commodity
        if year:
            params["year"] = year
        if county_code:
            params["county_code"] = county_code
            
        response = requests.get(f"{self.usda_base_url}/api_GET/", params=params)
        
        if response.status_code == 200:
            data = response.json()
            if 'data' in data:
                return pd.DataFrame(data['data'])
        return None
    
    def get_noaa_weather_data(self, dataset_id="GHCND", location_id=None, 
                            start_date=None, end_date=None, datatypes="TMAX,TMIN,PRCP"):
        """
        Fetch NOAA weather data
        Common datasets: GHCND (daily summaries), GSOM (monthly summaries)
        """
        params = {
            "datasetid": dataset_id,
            "limit": 1000,
            "format": "json"
        }
        
        if location_id:
            params["locationid"] = location_id
        if start_date:
            params["startdate"] = start_date
        if end_date:
            params["enddate"] = end_date
        if datatypes:
            params["datatypeid"] = datatypes
            
        response = requests.get(f"{self.noaa_base_url}/data", 
                              params=params, headers=self.noaa_headers)
        
        # NOAA API rate limiting
        time.sleep(0.2)
        
        if response.status_code == 200:
            data = response.json()
            if 'results' in data:
                return pd.DataFrame(data['results'])
        return None
    
    def get_noaa_stations_by_state(self, state_code):
        """
        Get NOAA weather stations for a specific state
        """
        params = {
            "datasetid": "GHCND",
            "locationid": f"FIPS:{state_code}",
            "limit": 1000,
            "format": "json"
        }
        
        response = requests.get(f"{self.noaa_base_url}/stations", 
                              params=params, headers=self.noaa_headers)
        time.sleep(0.2)
        
        if response.status_code == 200:
            data = response.json()
            if 'results' in data:
                return pd.DataFrame(data['results'])
        return None
    
    def create_database_schema(self, db_path="agri_weather.db"):
        """
        Create SQLite database with tables for USDA and NOAA data
        """
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # USDA Crop Data Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS usda_crop_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state_alpha TEXT,
                state_name TEXT,
                county_code TEXT,
                county_name TEXT,
                commodity_desc TEXT,
                class_desc TEXT,
                util_practice_desc TEXT,
                statisticcat_desc TEXT,
                unit_desc TEXT,
                year INTEGER,
                value REAL,
                cv_pct REAL,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # NOAA Weather Data Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS noaa_weather_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station TEXT,
                date TEXT,
                datatype TEXT,
                value REAL,
                measurement_flag TEXT,
                quality_flag TEXT,
                source_flag TEXT,
                obstime TEXT,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Combined Analysis Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS agri_weather_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state TEXT,
                county TEXT,
                year INTEGER,
                commodity TEXT,
                crop_yield REAL,
                avg_temp_max REAL,
                avg_temp_min REAL,
                total_precipitation REAL,
                growing_degree_days REAL,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        
    def store_usda_data(self, df, db_path="agri_weather.db"):
        """
        Store USDA data in SQLite database
        """
        if df is not None and not df.empty:
            conn = sqlite3.connect(db_path)
            
            # Select relevant columns
            columns_to_store = [
                'state_alpha', 'state_name', 'county_code', 'county_name',
                'commodity_desc', 'class_desc', 'util_practice_desc',
                'statisticcat_desc', 'unit_desc', 'year', 'Value', 'CV (%)'
            ]
            
            # Filter existing columns
            available_columns = [col for col in columns_to_store if col in df.columns]
            df_filtered = df[available_columns].copy()
            
            # Rename columns to match database schema
            column_mapping = {
                'Value': 'value',
                'CV (%)': 'cv_pct'
            }
            df_filtered.rename(columns=column_mapping, inplace=True)
            
            df_filtered.to_sql('usda_crop_data', conn, if_exists='append', index=False)
            conn.close()
            
    def store_noaa_data(self, df, db_path="agri_weather.db"):
        """
        Store NOAA data in SQLite database
        """
        if df is not None and not df.empty:
            conn = sqlite3.connect(db_path)
            df.to_sql('noaa_weather_data', conn, if_exists='append', index=False)
            conn.close()
            
    def calculate_growing_degree_days(self, tmax, tmin, base_temp=50):
        """
        Calculate Growing Degree Days (GDD)
        GDD = ((Tmax + Tmin) / 2) - Base Temperature
        """
        avg_temp = (tmax + tmin) / 2
        gdd = max(0, avg_temp - base_temp)
        return gdd
    
    def analyze_crop_weather_correlation(self, db_path="agri_weather.db"):
        """
        Analyze correlation between crop yields and weather data
        """
        conn = sqlite3.connect(db_path)
        
        # SQL query to join and aggregate data
        query = '''
        SELECT 
            u.state_alpha as state,
            u.county_name as county,
            u.year,
            u.commodity_desc as commodity,
            AVG(CASE WHEN u.statisticcat_desc = 'YIELD' THEN u.value END) as crop_yield,
            AVG(CASE WHEN w.datatype = 'TMAX' THEN w.value/10.0 END) as avg_temp_max_c,
            AVG(CASE WHEN w.datatype = 'TMIN' THEN w.value/10.0 END) as avg_temp_min_c,
            SUM(CASE WHEN w.datatype = 'PRCP' THEN w.value/10.0 END) as total_precipitation_mm
        FROM usda_crop_data u
        JOIN noaa_weather_data w ON 
            substr(w.date, 1, 4) = CAST(u.year AS TEXT)
        WHERE u.statisticcat_desc = 'YIELD'
        AND w.datatype IN ('TMAX', 'TMIN', 'PRCP')
        GROUP BY u.state_alpha, u.county_name, u.year, u.commodity_desc
        HAVING crop_yield IS NOT NULL 
        AND avg_temp_max_c IS NOT NULL 
        AND avg_temp_min_c IS NOT NULL
        '''
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Convert Celsius to Fahrenheit and calculate GDD
        if not df.empty:
            df['avg_temp_max_f'] = (df['avg_temp_max_c'] * 9/5) + 32
            df['avg_temp_min_f'] = (df['avg_temp_min_c'] * 9/5) + 32
            df['growing_degree_days'] = df.apply(
                lambda row: self.calculate_growing_degree_days(
                    row['avg_temp_max_f'], row['avg_temp_min_f']
                ), axis=1
            )
        
        return df
    
    def create_comprehensive_report(self, state_code, commodity, year_range, db_path="agri_weather.db"):
        """
        Generate comprehensive agricultural-weather report
        """
        start_year, end_year = year_range
        
        # Fetch USDA data
        print(f"Fetching USDA data for {commodity} in {state_code}...")
        for year in range(start_year, end_year + 1):
            usda_data = self.get_usda_crop_data(
                state_code=state_code, 
                commodity=commodity, 
                year=year
            )
            if usda_data is not None:
                self.store_usda_data(usda_data, db_path)
                print(f"Stored USDA data for {year}")
        
        # Fetch NOAA station data
        print(f"Fetching NOAA stations for {state_code}...")
        stations_df = self.get_noaa_stations_by_state(state_code)
        
        if stations_df is not None and not stations_df.empty:
            # Get first few active stations
            active_stations = stations_df.head(3)['id'].tolist()
            
            # Fetch weather data for each station
            for station in active_stations:
                print(f"Fetching weather data for station {station}...")
                weather_data = self.get_noaa_weather_data(
                    location_id=station,
                    start_date=f"{start_year}-01-01",
                    end_date=f"{end_year}-12-31"
                )
                if weather_data is not None:
                    self.store_noaa_data(weather_data, db_path)
                    print(f"Stored weather data for {station}")
        
        # Generate analysis
        print("Generating correlation analysis...")
        analysis_df = self.analyze_crop_weather_correlation(db_path)
        
        return analysis_df

# Example usage
if __name__ == "__main__":
    # Initialize with your API keys
    usda_key = "YOUR_USDA_API_KEY"  # Get from api.data.gov
    noaa_key = "YOUR_NOAA_API_KEY"  # Get from ncdc.noaa.gov/cdo-web/token
    
    integrator = USDANOAAIntegrator(usda_key, noaa_key)
    
    # Create database
    integrator.create_database_schema()
    
    # Example: Analyze corn production and weather in Iowa (2020-2022)
    # report = integrator.create_comprehensive_report(
    #     state_code="IA", 
    #     commodity="CORN", 
    #     year_range=(2020, 2022)
    # )
    
    # print(report.head())
    
    # Example correlation analysis
    # correlation_matrix = report[['crop_yield', 'avg_temp_max_f', 'total_precipitation_mm', 'growing_degree_days']].corr()
    # print("Correlation Matrix:")
    # print(correlation_matrix)
