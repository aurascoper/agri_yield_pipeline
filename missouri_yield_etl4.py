# Complete Missouri Agri-Weather Database Setup and ETL Script

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

def connect_to_db():
    """Connect to MySQL database"""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

def create_database_schema():
    """Create all necessary tables for the agri_weather database"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    print("Creating database schema...")
    
    # Create location table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS location (
            location_id INT PRIMARY KEY AUTO_INCREMENT,
            county_name VARCHAR(100),
            state VARCHAR(50),
            latitude DECIMAL(10, 8),
            longitude DECIMAL(11, 8),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ Created location table")
    
    # Create crop table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crop (
            crop_id INT PRIMARY KEY,
            crop_name VARCHAR(100) NOT NULL,
            crop_type VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ Created crop table")
    
    # Create crop_yield table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crop_yield (
            yield_id INT PRIMARY KEY,
            crop_id INT,
            season_id INT,
            location_id INT,
            yield_amount DECIMAL(15, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (crop_id) REFERENCES crop(crop_id),
            FOREIGN KEY (location_id) REFERENCES location(location_id),
            UNIQUE KEY unique_yield (crop_id, season_id, location_id)
        )
    """)
    print("✓ Created crop_yield table")
    
    # Create weather_data table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            weather_id INT PRIMARY KEY AUTO_INCREMENT,
            location_id INT,
            date DATE,
            temperature_high DECIMAL(5, 2),
            temperature_low DECIMAL(5, 2),
            precipitation DECIMAL(6, 3),
            humidity DECIMAL(5, 2),
            wind_speed DECIMAL(5, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (location_id) REFERENCES location(location_id),
            UNIQUE KEY unique_weather (location_id, date)
        )
    """)
    print("✓ Created weather_data table")
    
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Database schema created successfully!")

def populate_sample_data():
    """Populate sample data for Missouri locations and crops"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    print("Populating sample data...")
    
    # Insert Missouri counties (sample major agricultural counties)
    missouri_counties = [
        ("Boone", "Missouri", 38.9517, -92.3341),
        ("Cole", "Missouri", 38.5031, -92.2018),
        ("Jackson", "Missouri", 39.0119, -94.3268),
        ("St. Louis", "Missouri", 38.6270, -90.1994),
        ("Greene", "Missouri", 37.2089, -93.2923),
        ("Clay", "Missouri", 39.2394, -94.4191),
        ("Jefferson", "Missouri", 38.4589, -90.3954),
        ("St. Charles", "Missouri", 38.7881, -90.4974),
        ("Buchanan", "Missouri", 39.7391, -94.8469),
        ("Cass", "Missouri", 38.6106, -94.3441)
    ]
    
    for county, state, lat, lon in missouri_counties:
        cursor.execute("""
            INSERT IGNORE INTO location (county_name, state, latitude, longitude)
            VALUES (%s, %s, %s, %s)
        """, (county, state, lat, lon))
    
    print(f"✓ Inserted {len(missouri_counties)} Missouri counties")
    
    # Insert crop types
    crops = [
        (1, "Corn", "Grain"),
        (2, "Soybeans", "Legume"),
        (3, "Wheat", "Grain"),
        (4, "Cotton", "Fiber"),
        (5, "Rice", "Grain"),
        (100, "Crop Totals", "Aggregate"),
        (101, "All Crops", "Aggregate")
    ]
    
    for crop_id, crop_name, crop_type in crops:
        cursor.execute("""
            INSERT IGNORE INTO crop (crop_id, crop_name, crop_type)
            VALUES (%s, %s, %s)
        """, (crop_id, crop_name, crop_type))
    
    print(f"✓ Inserted {len(crops)} crop types")
    
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Sample data populated successfully!")

def load_crop_yield_data(csv_path="total_crop_yield.csv"):
    """Load crop yield data from CSV"""
    try:
        # Try different possible paths
        possible_paths = [
            csv_path,
            f"~/{csv_path}",
            f"~/codelab/{csv_path}",
            f"./{csv_path}"
        ]
        
        df = None
        for path in possible_paths:
            try:
                expanded_path = os.path.expanduser(path)
                df = pd.read_csv(expanded_path)
                print(f"✓ Loaded {len(df)} rows from {expanded_path}")
                break
            except FileNotFoundError:
                continue
        
        if df is None:
            raise FileNotFoundError(f"Could not find CSV file at any of these paths: {possible_paths}")
        
        print("Columns in CSV:", df.columns.tolist())
        print("\nFirst few rows:")
        print(df.head())
        return df
    except Exception as e:
        print(f"Failed to load crop yield data: {e}")
        return pd.DataFrame()

def insert_crop_yield_data(df):
    """Insert crop yield data into database"""
    if df.empty:
        print("No data to insert")
        return 0
        
    conn = connect_to_db()
    cursor = conn.cursor()
    inserted = 0
    
    # Get the first Missouri location to use as representative
    cursor.execute("SELECT location_id FROM location WHERE state = 'Missouri' LIMIT 1")
    location_result = cursor.fetchone()
    
    if not location_result:
        print("❌ No Missouri locations found in database!")
        cursor.close()
        conn.close()
        return 0
    
    location_id = location_result[0]
    print(f"Using location_id {location_id} for crop yield data")
    
    # Process the production value column
    production_col = "CROP TOTALS, (EXCL HORTICULTURE) - PRODUCTION, MEASURED IN $  -  <b>VALUE</b>"
    
    for _, row in df.iterrows():
        try:
            year = int(row["Year"])
            
            # Check if we have production data for this year
            if production_col in row and pd.notna(row[production_col]):
                # Clean and convert production value
                production_str = str(row[production_col]).replace(',', '').replace('$', '').strip()
                
                if production_str and production_str.replace('.', '').isdigit():
                    production_value = float(production_str)
                    
                    # Create unique yield_id
                    yield_id = hash(f"100-{year}-{location_id}") % 9999999
                    if yield_id < 0:
                        yield_id = abs(yield_id)
                    
                    # Insert into crop_yield table
                    cursor.execute("""
                        INSERT IGNORE INTO crop_yield (yield_id, crop_id, season_id, location_id, yield_amount)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (yield_id, 100, year, location_id, production_value))
                    
                    if cursor.rowcount > 0:
                        inserted += 1
                        print(f"✓ Inserted: Year {year}, Production ${production_value:,.0f}")
            
            # Also try to insert price index data
            price_index_col = "CROP TOTALS - INDEX FOR PRICE RECEIVED, 2011  -  <b>VALUE</b>"
            if price_index_col in row and pd.notna(row[price_index_col]):
                price_index = float(row[price_index_col])
                
                # Create different yield_id for price index data
                yield_id = hash(f"101-{year}-{location_id}") % 9999999
                if yield_id < 0:
                    yield_id = abs(yield_id)
                
                cursor.execute("""
                    INSERT IGNORE INTO crop_yield (yield_id, crop_id, season_id, location_id, yield_amount)
                    VALUES (%s, %s, %s, %s, %s)
                """, (yield_id, 101, year, location_id, price_index))
                
                if cursor.rowcount > 0:
                    inserted += 1
                    print(f"✓ Inserted: Year {year}, Price Index {price_index}")
                        
        except Exception as e:
            print(f"Insert error for row with Year {row.get('Year', 'Unknown')}: {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Inserted {inserted} crop yield records into database.")
    return inserted

def query_and_display_results():
    """Query and display the inserted crop yield data"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    try:
        # Query the inserted data
        cursor.execute("""
            SELECT cy.yield_id, c.crop_name, cy.season_id, 
                   l.county_name, l.state, cy.yield_amount
            FROM crop_yield cy
            JOIN location l ON cy.location_id = l.location_id
            JOIN crop c ON cy.crop_id = c.crop_id
            WHERE l.state = 'Missouri'
            ORDER BY cy.season_id DESC, c.crop_name
            LIMIT 20
        """)
        
        results = cursor.fetchall()
        print(f"\n📊 Found {len(results)} crop yield records for Missouri:")
        print("="*100)
        print(f"{'Yield ID':<10} {'Crop':<15} {'Year':<6} {'County':<20} {'State':<8} {'Amount':<20}")
        print("-"*100)
        
        for row in results:
            yield_id, crop_name, year, county, state, yield_amount = row
            if crop_name == "Crop Totals":
                amount_str = f"${yield_amount:>15,.0f}"
            else:
                amount_str = f"{yield_amount:>15.1f}"
            print(f"{yield_id:<10} {crop_name:<15} {year:<6} {county:<20} {state:<8} {amount_str:<20}")
            
        # Get summary statistics
        cursor.execute("""
            SELECT c.crop_name,
                   COUNT(*) as total_records, 
                   MIN(cy.season_id) as earliest_year,
                   MAX(cy.season_id) as latest_year,
                   AVG(cy.yield_amount) as avg_yield
            FROM crop_yield cy
            JOIN location l ON cy.location_id = l.location_id
            JOIN crop c ON cy.crop_id = c.crop_id
            WHERE l.state = 'Missouri'
            GROUP BY c.crop_name
            ORDER BY c.crop_name
        """)
        
        summary = cursor.fetchall()
        if summary:
            print(f"\n📈 Summary Statistics by Crop:")
            print("="*80)
            print(f"{'Crop':<15} {'Records':<8} {'Years':<12} {'Average':<20}")
            print("-"*80)
            for crop_name, total, earliest, latest, avg_yield in summary:
                year_range = f"{earliest}-{latest}"
                if crop_name == "Crop Totals":
                    avg_str = f"${avg_yield:>15,.0f}"
                else:
                    avg_str = f"{avg_yield:>15.1f}"
                print(f"{crop_name:<15} {total:<8} {year_range:<12} {avg_str:<20}")
            
    except Exception as e:
        print(f"Query error: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    """Main execution function"""
    print("🌾 Missouri Agri-Weather Database Setup and ETL")
    print("=" * 50)
    
    try:
        # Step 1: Create database schema
        create_database_schema()
        
        # Step 2: Populate sample data
        populate_sample_data()
        
        # Step 3: Load crop yield data from CSV
        df = load_crop_yield_data()
        
        if not df.empty:
            # Step 4: Insert crop yield data
            inserted_count = insert_crop_yield_data(df)
            
            # Step 5: Query and display results
            if inserted_count > 0:
                query_and_display_results()
            else:
                print("❌ No data was inserted. Please check the data format.")
        else:
            print("❌ No CSV data loaded. Please check the file path.")
            
    except Exception as e:
        print(f"❌ Error in main execution: {e}")

if __name__ == "__main__":
    main()
