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

# Load Missouri crop yield data from CSV (or API response transformed to DataFrame)
def load_crop_yield_data(csv_path="~/codelab/total_crop_yield.csv"):
    try:
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} rows from {csv_path}")
        return df
    except Exception as e:
        print(f"Failed to load crop yield data: {e}")
        return pd.DataFrame()

# Insert data into crop_yield table
def insert_crop_yield(df):
    conn = connect_to_db()
    cursor = conn.cursor()
    inserted = 0

    for _, row in df.iterrows():
        try:
            crop_name = row["commodity_desc"]
            county = row["county_name"]
            state = row["state_name"]
            year = int(row["year"])
            value = str(row["Value"]).replace(",", "")
            yield_value = float(value) if value.replace(".", "", 1).isdigit() else None

            crop_id = {"Corn": 101, "Soybeans": 102, "Wheat": 103, "Cotton": 104}.get(crop_name, 0)
            season_id = year

            # Match location_id from known Missouri counties
            cursor.execute("SELECT location_id FROM location WHERE county_name = %s AND state = 'Missouri'", (county,))
            result = cursor.fetchone()
            if not result:
                print(f"Skipping unknown location: {county}, Missouri")
                continue
            location_id = result[0]

            yield_id = hash(f"{crop_id}-{season_id}-{location_id}") % 9999999

            cursor.execute("""
                INSERT IGNORE INTO crop_yield (yield_id, crop_id, season_id, location_id, yield_amount)
                VALUES (%s, %s, %s, %s, %s)
            """, (yield_id, crop_id, season_id, location_id, yield_value))
            inserted += 1
        except Exception as e:
            print(f"Insert error for row {row.to_dict()}: {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Inserted {inserted} crop yield records into database.")

if __name__ == "__main__":
    df = load_crop_yield_data()
    if not df.empty:
        insert_crop_yield(df)

