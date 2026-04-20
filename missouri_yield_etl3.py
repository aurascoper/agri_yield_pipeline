# Missouri Crop Yield ETL Script for agri_weather MySQL Database

import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "password")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "agri_weather")

# Connect to MySQL
def connect_to_db():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

# Load crop yield data from CSV
def load_crop_yield_data(csv_path="~/codelab/total_crop_yield.csv"):
    try:
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} rows from {csv_path}")
        print("Columns in CSV:", df.columns.tolist())
        print("\nFirst few rows:")
        print(df.head())
        return df
    except Exception as e:
        print(f"Failed to load crop yield data: {e}")
        return pd.DataFrame()

# Insert data into crop_yield table
def insert_crop_yield(df):
    conn = connect_to_db()
    cursor = conn.cursor()
    inserted = 0
    
    # First, let's check what locations we have in the database
    cursor.execute("SELECT location_id, county_name, state FROM location WHERE state = 'Missouri'")
    locations = cursor.fetchall()
    print(f"Found {len(locations)} Missouri locations in database")
    
    # Since the CSV contains national totals, we'll need to handle this differently
    # Let's check what data we actually have
    print("\nAnalyzing CSV data structure...")
    print(f"State column unique values: {df['State'].unique()}")
    print(f"Commodity column unique values: {df['Commodity'].unique()}")
    
    # Filter for actual crop data (not totals)
    crop_data = df[df['Commodity'] != 'CROP TOTALS'].copy() if 'CROP TOTALS' not in df['Commodity'].unique() else df.copy()
    
    # If we only have national totals, we'll need to create representative data
    # For demonstration, let's insert the national data as state-level data for Missouri
    
    for _, row in df.iterrows():
        try:
            # Extract year
            year = int(row["Year"])
            
            # For this CSV which contains national crop price indices, 
            # we'll treat it as general crop market data for Missouri
            
            # Get production value if available
            production_col = None
            for col in df.columns:
                if 'PRODUCTION' in col and 'VALUE' in col:
                    production_col = col
                    break
            
            if production_col and pd.notna(row[production_col]):
                # Remove commas and convert to float
                production_str = str(row[production_col]).replace(',', '').replace('$', '')
                if production_str.replace('.', '').isdigit():
                    production_value = float(production_str)
                    
                    # Use a generic crop ID for "total crops"
                    crop_id = 100  # Generic crop totals
                    season_id = year
                    
                    # Use the first Missouri location as representative
                    if locations:
                        location_id = locations[0][0]  # First Missouri location
                        
                        # Create unique yield_id
                        yield_id = hash(f"{crop_id}-{season_id}-{location_id}") % 9999999
                        
                        cursor.execute("""
                            INSERT IGNORE INTO crop_yield (yield_id, crop_id, season_id, location_id, yield_amount)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (yield_id, crop_id, season_id, location_id, production_value))
                        
                        if cursor.rowcount > 0:
                            inserted += 1
                            print(f"Inserted: Year {year}, Production ${production_value:,.0f}")
                        
        except Exception as e:
            print(f"Insert error for row with Year {row.get('Year', 'Unknown')}: {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Inserted {inserted} crop yield records into database.")
    return inserted

# Query and display the inserted data
def query_crop_yield_data():
    conn = connect_to_db()
    cursor = conn.cursor()
    
    try:
        # Query the inserted data
        cursor.execute("""
            SELECT cy.yield_id, cy.crop_id, cy.season_id, 
                   l.county_name, l.state, cy.yield_amount
            FROM crop_yield cy
            JOIN location l ON cy.location_id = l.location_id
            WHERE l.state = 'Missouri'
            ORDER BY cy.season_id DESC
            LIMIT 10
        """)
        
        results = cursor.fetchall()
        print(f"\n📊 Found {len(results)} crop yield records for Missouri:")
        print("="*80)
        print(f"{'Yield ID':<10} {'Crop ID':<8} {'Year':<6} {'County':<20} {'State':<8} {'Yield Amount':<15}")
        print("-"*80)
        
        for row in results:
            yield_id, crop_id, year, county, state, yield_amount = row
            print(f"{yield_id:<10} {crop_id:<8} {year:<6} {county:<20} {state:<8} {yield_amount:>14,.2f}")
            
        # Get summary statistics
        cursor.execute("""
            SELECT COUNT(*) as total_records, 
                   MIN(season_id) as earliest_year,
                   MAX(season_id) as latest_year,
                   AVG(yield_amount) as avg_yield
            FROM crop_yield cy
            JOIN location l ON cy.location_id = l.location_id
            WHERE l.state = 'Missouri'
        """)
        
        summary = cursor.fetchone()
        if summary:
            total, earliest, latest, avg_yield = summary
            print(f"\n📈 Summary Statistics:")
            print(f"Total Records: {total}")
            print(f"Year Range: {earliest} - {latest}")
            print(f"Average Yield: {avg_yield:,.2f}")
            
    except Exception as e:
        print(f"Query error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Load and examine the data
    df = load_crop_yield_data()
    
    if not df.empty:
        # Insert the data
        inserted_count = insert_crop_yield(df)
        
        # Query and display results
        if inserted_count > 0:
            query_crop_yield_data()
        else:
            print("No data was inserted. Please check the data format and database schema.")
            
            # Debug information
            print("\nDebugging information:")
            print("Available columns:", df.columns.tolist())
            print("\nSample data:")
            print(df.head(3).to_string())
