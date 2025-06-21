# ===================================================================================
# DHL Logistics Data Pipeline - End-to-End ETL Project
# ===================================================================================
#
# Author: Your Name
# Project: DHL Logistics & Supply Chain Analysis
# Version: 2.2
#
# Description:
# This script performs a full ETL (Extract, Transform, Load) process.
# 1. GENERATES 10,000 rows of realistic, messy logistics data.
# 2. CLEANS and TRANSFORMS the data using the pandas library.
# 3. LOADS the cleaned, normalized data into a MySQL database, creating a
#    star schema with fact and dimension tables.
#
# Change Log (v2.2):
# - Made the date parsing logic more robust to handle multiple date formats
#   (YYYY-MM-DD and DD-MM-YYYY) to prevent accidental dropping of valid rows.
# ===================================================================================

import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
import random
from datetime import datetime, timedelta
import json
import os

print("ETL Script Started...")

# --- PART 1: DATA GENERATION ---
# ===================================================================================

def generate_messy_dhl_data(num_rows=10000):
    """
    Generates a Pandas DataFrame with messy, realistic logistics data.
    """
    print(f"Step 1: Generating {num_rows} rows of messy data...")
    
    cities = [
        ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"), ("Houston", "TX"),
        ("Phoenix", "AZ"), ("Philadelphia", "PA"), ("San Antonio", "TX"), ("San Diego", "CA"),
        ("Dallas", "TX"), ("San Jose", "CA"), ("Miami", "FL"), ("Denver", "CO"),
        ("Seattle", "WA"), ("Boston", "MA"), ("Atlanta", "GA")
    ]
    
    carriers = ["SpeedyShip", "speedyship", "Reliable Freight", "Global Cargo", "Quick Haul", "quick haul", ""]
    
    data = []
    start_date = datetime(2023, 1, 1)
    
    for i in range(num_rows):
        origin_city, origin_state = random.choice(cities)
        dest_city, dest_state = random.choice([c for c in cities if c != (origin_city, origin_state)])
        
        ship_date = start_date + timedelta(days=random.randint(0, 500))
        if i % 10 == 0:
            ship_date_str = ship_date.strftime('%d-%m-%Y') # Messy format 1
        elif i % 5 == 0:
            ship_date_str = None # Missing date
        else:
            ship_date_str = ship_date.strftime('%Y-%m-%d') # Standard format
        
        promised_delivery_days = random.randint(3, 10)
        promised_date = ship_date + timedelta(days=promised_delivery_days) if ship_date else None
        
        delay_chance = random.random()
        if delay_chance < 0.2:
            actual_delivery_date = promised_date + timedelta(days=random.randint(1, 3)) if promised_date else None
        elif delay_chance < 0.25:
             actual_delivery_date = promised_date - timedelta(days=1) if promised_date else None
        elif delay_chance < 0.3:
            actual_delivery_date = None
        else:
            actual_delivery_date = promised_date

        cost = random.uniform(50, 500)
        if i % 20 == 0:
            cost = -cost
        elif i % 25 == 0:
            cost = np.nan
            
        row = {
            'ShipmentID': f'DHL-{10000 + i}',
            'ShipDate': ship_date_str,
            'PromisedDeliveryDate': promised_date.strftime('%Y-%m-%d') if promised_date else None,
            'ActualDeliveryDate': actual_delivery_date.strftime('%Y-%m-%d') if actual_delivery_date else None,
            'OriginCity': origin_city, 'OriginState': origin_state,
            'DestinationCity': dest_city, 'DestinationState': dest_state,
            'ShippingCost': round(cost, 2), 'CarrierName': random.choice(carriers),
            'ShipmentStatus': None
        }
        data.append(row)
        
    print("Data generation complete.")
    return pd.DataFrame(data)

# --- PART 2: DATA CLEANING & TRANSFORMATION ---
# ===================================================================================

def clean_and_transform_data(df):
    """
    Cleans and transforms the raw DataFrame.
    """
    print("\nStep 2: Cleaning and transforming data...")
    
    # 1. Handle dates with a more robust custom parser
    # THIS IS THE CORRECTED LOGIC
    def parse_mixed_formats(date_str):
        if date_str is None:
            return pd.NaT
        try:
            # First, try the standard format
            return datetime.strptime(str(date_str), '%Y-%m-%d')
        except ValueError:
            try:
                # If that fails, try the messy format
                return datetime.strptime(str(date_str), '%d-%m-%Y')
            except ValueError:
                # If all fail, return Not a Time
                return pd.NaT

    df['ShipDate'] = df['ShipDate'].apply(parse_mixed_formats)
    df['PromisedDeliveryDate'] = pd.to_datetime(df['PromisedDeliveryDate'], errors='coerce')
    df['ActualDeliveryDate'] = pd.to_datetime(df['ActualDeliveryDate'], errors='coerce')
    
    # Drop rows where ShipDate is missing, as it's critical
    df.dropna(subset=['ShipDate'], inplace=True)

    # 2. Clean Carrier Names
    df['CarrierName'] = df['CarrierName'].str.title().replace('', 'Unknown')
    df['CarrierName'] = df['CarrierName'].fillna('Unknown')
    
    # 3. Clean Shipping Costs
    df['ShippingCost'] = df['ShippingCost'].abs()
    median_costs = df.groupby('CarrierName')['ShippingCost'].transform('median')
    df['ShippingCost'] = df['ShippingCost'].fillna(median_costs)
    df['ShippingCost'] = df['ShippingCost'].fillna(df['ShippingCost'].median())

    # 4. Derive ShipmentStatus
    conditions = [
        (df['ActualDeliveryDate'].isna()),
        (df['ActualDeliveryDate'] <= df['PromisedDeliveryDate']),
        (df['ActualDeliveryDate'] > df['PromisedDeliveryDate'])
    ]
    choices = ['In-Transit', 'On-Time', 'Delayed']
    df['ShipmentStatus'] = np.select(conditions, choices, default='Unknown')
    
    print("Data cleaning complete.")
    return df

# --- PART 3: DATABASE LOADING (ETL - Load) ---
# ===================================================================================

def get_db_connection(creds_path):
    """
    Reads credentials from a JSON file and returns a MySQL connection object.
    """
    print("\nStep 3.1: Connecting to MySQL database...")
    try:
        with open(creds_path, 'r') as f:
            creds = json.load(f)
        
        connection = mysql.connector.connect(
            host=creds['host'],
            user=creds['user'],
            password=creds['password'],
            database=creds['database']
        )
        if connection.is_connected():
            print("MySQL connection successful.")
            return connection
    except FileNotFoundError:
        print(f"ERROR: Credentials file not found at '{creds_path}'")
        return None
    except Error as e:
        print(f"ERROR: Could not connect to MySQL database: {e}")
        return None

def create_database_schema(cursor):
    """
    Creates the normalized star schema tables if they don't already exist.
    """
    print("Step 3.2: Creating database schema (if not exists)...")
    
    tables = {
        "dim_locations": """
            CREATE TABLE IF NOT EXISTS dim_locations (
                LocationID INT AUTO_INCREMENT PRIMARY KEY,
                City VARCHAR(255) NOT NULL,
                State VARCHAR(50) NOT NULL,
                UNIQUE KEY city_state (City, State)
            ) ENGINE=InnoDB;
        """,
        "dim_carriers": """
            CREATE TABLE IF NOT EXISTS dim_carriers (
                CarrierID INT AUTO_INCREMENT PRIMARY KEY,
                CarrierName VARCHAR(255) UNIQUE NOT NULL
            ) ENGINE=InnoDB;
        """,
        "dim_dates": """
            CREATE TABLE IF NOT EXISTS dim_dates (
                DateKey INT PRIMARY KEY,
                FullDate DATE NOT NULL,
                Year INT NOT NULL,
                Quarter INT NOT NULL,
                Month INT NOT NULL,
                Day INT NOT NULL,
                Weekday INT NOT NULL,
                UNIQUE KEY full_date_unique (FullDate)
            ) ENGINE=InnoDB;
        """,
        "fact_shipments": """
            CREATE TABLE IF NOT EXISTS fact_shipments (
                ShipmentFactID INT AUTO_INCREMENT PRIMARY KEY,
                ShipmentID VARCHAR(50) UNIQUE NOT NULL,
                ShipDateKey INT,
                PromisedDateKey INT,
                ActualDeliveryDateKey INT,
                OriginLocationID INT,
                DestinationLocationID INT,
                CarrierID INT,
                ShippingCost DECIMAL(10, 2),
                ShipmentStatus VARCHAR(50),
                FOREIGN KEY (ShipDateKey) REFERENCES dim_dates(DateKey),
                FOREIGN KEY (PromisedDateKey) REFERENCES dim_dates(DateKey),
                FOREIGN KEY (ActualDeliveryDateKey) REFERENCES dim_dates(DateKey),
                FOREIGN KEY (OriginLocationID) REFERENCES dim_locations(LocationID),
                FOREIGN KEY (DestinationLocationID) REFERENCES dim_locations(LocationID),
                FOREIGN KEY (CarrierID) REFERENCES dim_carriers(CarrierID)
            ) ENGINE=InnoDB;
        """
    }
    
    try:
        for table_name, table_sql in tables.items():
            print(f"Creating table {table_name}...")
            cursor.execute(table_sql)
        print("Schema creation successful.")
    except Error as e:
        print(f"ERROR: Could not create schema: {e}")

def load_dimensions(cursor, conn, df):
    """
    Loads data into all dimension tables from the DataFrame.
    """
    print("Step 3.3: Loading dimension tables...")
    try:
        # dim_locations
        locations = pd.concat([
            df[['OriginCity', 'OriginState']].rename(columns={'OriginCity': 'City', 'OriginState': 'State'}),
            df[['DestinationCity', 'DestinationState']].rename(columns={'DestinationCity': 'City', 'DestinationState': 'State'})
        ]).drop_duplicates().reset_index(drop=True)
        location_tuples = [tuple(x) for x in locations.to_numpy()]
        cursor.executemany("INSERT IGNORE INTO dim_locations (City, State) VALUES (%s, %s)", location_tuples)
        
        # dim_carriers
        carriers = df['CarrierName'].unique()
        carrier_tuples = [(c,) for c in carriers]
        cursor.executemany("INSERT IGNORE INTO dim_carriers (CarrierName) VALUES (%s)", carrier_tuples)
        
        # dim_dates
        all_dates = pd.concat([df['ShipDate'], df['PromisedDeliveryDate'], df['ActualDeliveryDate']]).dropna().unique()
        date_df = pd.DataFrame({'FullDate': all_dates})
        date_df['DateKey'] = date_df['FullDate'].dt.strftime('%Y%m%d').astype(int)
        date_df['Year'] = date_df['FullDate'].dt.year
        date_df['Quarter'] = date_df['FullDate'].dt.quarter
        date_df['Month'] = date_df['FullDate'].dt.month
        date_df['Day'] = date_df['FullDate'].dt.day
        date_df['Weekday'] = date_df['FullDate'].dt.weekday # Monday=0, Sunday=6
        date_tuples = [tuple(x) for x in date_df.to_numpy()]
        cursor.executemany("INSERT IGNORE INTO dim_dates (FullDate, DateKey, Year, Quarter, Month, Day, Weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)", date_tuples)

        conn.commit()
        print("Dimension tables loaded successfully.")
    except Error as e:
        print(f"ERROR: Could not load dimension tables: {e}")
        conn.rollback()


def load_fact_table(cursor, conn, df):
    """
    Loads data into the fact_shipments table, looking up foreign keys.
    """
    print("Step 3.4: Loading fact table...")
    try:
        cursor.execute("SELECT LocationID, City, State FROM dim_locations")
        loc_map = {(city, state): loc_id for loc_id, city, state in cursor.fetchall()}
        
        cursor.execute("SELECT CarrierID, CarrierName FROM dim_carriers")
        carrier_map = {name: c_id for c_id, name in cursor.fetchall()}
        
        cursor.execute("SELECT DateKey, FullDate FROM dim_dates")
        date_map = {pd.to_datetime(date).date(): key for key, date in cursor.fetchall()}

        fact_data = []
        for _, row in df.iterrows():
            ship_date_key = date_map.get(row['ShipDate'].date())
            promised_date_key = date_map.get(row['PromisedDeliveryDate'].date()) if pd.notna(row['PromisedDeliveryDate']) else None
            actual_date_key = date_map.get(row['ActualDeliveryDate'].date()) if pd.notna(row['ActualDeliveryDate']) else None
            origin_loc_id = loc_map.get((row['OriginCity'], row['OriginState']))
            dest_loc_id = loc_map.get((row['DestinationCity'], row['DestinationState']))
            carrier_id = carrier_map.get(row['CarrierName'])
            
            fact_data.append((
                row['ShipmentID'], ship_date_key, promised_date_key, actual_date_key,
                origin_loc_id, dest_loc_id, carrier_id,
                row['ShippingCost'], row['ShipmentStatus']
            ))

        sql = """
            INSERT IGNORE INTO fact_shipments (
                ShipmentID, ShipDateKey, PromisedDateKey, ActualDeliveryDateKey,
                OriginLocationID, DestinationLocationID, CarrierID,
                ShippingCost, ShipmentStatus
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(sql, fact_data)
        conn.commit()
        print(f"Fact table loaded successfully. {cursor.rowcount} new rows inserted.")

    except Error as e:
        print(f"ERROR: Could not load fact table: {e}")
        conn.rollback()


# --- MAIN EXECUTION ---
# ===================================================================================

if __name__ == "__main__":
    
    creds_folder_path = r"C:\Users\Eier\Desktop\Data Analytics Project for GitHub\MySQL Credentials"
    creds_file_path = os.path.join(creds_folder_path, "mysql_creds.json")
    
    raw_df = generate_messy_dhl_data(num_rows=10000)
    cleaned_df = clean_and_transform_data(raw_df)
    
    print(f"\nINFO: Number of cleaned rows to be loaded: {len(cleaned_df)}")

    connection = get_db_connection(creds_file_path)
    
    if connection and connection.is_connected():
        cursor = connection.cursor()
        create_database_schema(cursor)
        load_dimensions(cursor, connection, cleaned_df)
        load_fact_table(cursor, connection, cleaned_df)
        cursor.close()
        connection.close()
        print("\nDatabase connection closed.")

    print("\nETL Script Finished.")
# ===================================================================================