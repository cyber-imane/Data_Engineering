import os
import json
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import argparse

# ----------------------------- #
# Database Configuration (No Password)
# ----------------------------- #
DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'host': 'localhost',
    'port': 5432
}

# ----------------------------- #
# Schema Definition
# ----------------------------- #
SCHEMA_SQL = """
DROP TABLE IF EXISTS BreadCrumb;
DROP TABLE IF EXISTS Trip;
DROP TYPE IF EXISTS service_type;
DROP TYPE IF EXISTS tripdir_type;

CREATE TYPE service_type AS ENUM ('Weekday', 'Saturday', 'Sunday');
CREATE TYPE tripdir_type AS ENUM ('Out', 'Back');

CREATE TABLE Trip (
    trip_id INTEGER PRIMARY KEY,
    route_id INTEGER,
    vehicle_id INTEGER,
    service_key service_type,
    direction tripdir_type
);

CREATE TABLE BreadCrumb (
    tstamp TIMESTAMP,
    latitude FLOAT,
    longitude FLOAT,
    speed FLOAT,
    trip_id INTEGER REFERENCES Trip
);
"""

# ----------------------------- #
# Helper Functions
# ----------------------------- #

def get_service_key(date_str):
    base_date = datetime.strptime(date_str.split(":")[0], "%d%b%Y")
    weekday = base_date.weekday()
    return "Weekday" if weekday <= 4 else "Saturday" if weekday == 5 else "Sunday"

def load_json_files(json_dir):
    all_data = []
    for filename in os.listdir(json_dir):
        if filename.endswith('.json'):
            with open(os.path.join(json_dir, filename)) as f:
                content = json.load(f)
                if isinstance(content, dict):
                    all_data.append(content)
                elif isinstance(content, list):
                    all_data.extend(content)
    return pd.DataFrame(all_data)

def process_data(df):
    df['base_date'] = df['OPD_DATE'].apply(lambda d: datetime.strptime(d.split(":")[0], "%d%b%Y"))
    df['tstamp'] = df.apply(lambda row: row['base_date'] + timedelta(seconds=row['ACT_TIME']), axis=1)
    df = df.sort_values(by=['EVENT_NO_TRIP', 'ACT_TIME'])
    df['speed'] = 0.0

    for trip_id, group in df.groupby('EVENT_NO_TRIP'):
        group = group.sort_values('ACT_TIME').copy()
        group['distance_diff'] = group['METERS'].diff()
        group['time_diff'] = group['ACT_TIME'].diff()
        group['speed'] = group['distance_diff'] / group['time_diff']
        group['speed'].iloc[0] = group['speed'].iloc[1]
        df.loc[group.index, 'speed'] = group['speed']

    trip_records = {}
    for trip_id, group in df.groupby('EVENT_NO_TRIP'):
        first = group.iloc[0]
        trip_records[trip_id] = {
            'trip_id': trip_id,
            'route_id': None,
            'vehicle_id': first['VEHICLE_ID'],
            'service_key': get_service_key(first['OPD_DATE']),
            'direction': None
        }
    trip_df = pd.DataFrame(trip_records.values())

    breadcrumb_df = df[['tstamp', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'speed', 'EVENT_NO_TRIP']].copy()
    breadcrumb_df.columns = ['tstamp', 'latitude', 'longitude', 'speed', 'trip_id']

    return trip_df, breadcrumb_df

def create_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    print("✅ Database schema created.")

def insert_into_postgres(trip_df, breadcrumb_df, conn):
    cur = conn.cursor()

    trip_data = [
        (int(r['trip_id']), r['route_id'], int(r['vehicle_id']), r['service_key'], r['direction'])
        for _, r in trip_df.iterrows()
    ]
    trip_query = """
        INSERT INTO Trip (trip_id, route_id, vehicle_id, service_key, direction)
        VALUES %s ON CONFLICT (trip_id) DO NOTHING
    """
    execute_values(cur, trip_query, trip_data)

    bc_data = [
        (r['tstamp'], float(r['latitude']), float(r['longitude']), float(r['speed']), int(r['trip_id']))
        for _, r in breadcrumb_df.iterrows()
    ]
    bc_query = """
        INSERT INTO BreadCrumb (tstamp, latitude, longitude, speed, trip_id)
        VALUES %s
    """
    execute_values(cur, bc_query, bc_data)

    conn.commit()
    cur.close()
    print(" Data inserted into Trip and BreadCrumb tables.")

# ----------------------------- #
# Main CLI Entry Point
# ----------------------------- #

def main():
    parser = argparse.ArgumentParser(description="Load breadcrumb JSON files into PostgreSQL.")
    parser.add_argument("json_dir", help="Directory containing JSON files")
    parser.add_argument("--create-schema", action="store_true", help="Drop & recreate schema before inserting")

    args = parser.parse_args()
    print(f"Loading JSON from: {args.json_dir}")

    df = load_json_files(args.json_dir)
    trip_df, breadcrumb_df = process_data(df)

    conn = psycopg2.connect(**DB_CONFIG)
    if args.create_schema:
        create_schema(conn)
    insert_into_postgres(trip_df, breadcrumb_df, conn)
    conn.close()

if __name__ == "__main__":
    main()

