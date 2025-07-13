# weather-pipeline/2_etl/load_to_snowflake.py

from mongo_to_df import read_mongo
from transform import clean_weather_data
import snowflake.connector
from config import *
import pandas as pd

def load_to_snowflake(df, table_name="weather_data"):
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    cursor = conn.cursor()

    # Création de la table si elle n'existe pas
    create_stmt = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        city STRING,
        temperature FLOAT,
        humidity INT,
        pressure INT,
        weather_main STRING,
        weather_description STRING,
        fetched_at TIMESTAMP
    )
    """
    cursor.execute(create_stmt)

    # Insertion ligne par ligne
    for _, row in df.iterrows():
        insert_stmt = f"""
        INSERT INTO {table_name} (city, temperature, humidity, pressure, weather_main, weather_description, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_stmt, (
            row['city'],
            row['temperature'],
            row['humidity'],
            row['pressure'],
            row['weather_main'],
            row['weather_description'],
            row['fetched_at']
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"{len(df)} lignes insérées dans {table_name} ✅")

if __name__ == "__main__":
    df_mongo = read_mongo(limit=100)
    df_clean = clean_weather_data(df_mongo)
    load_to_snowflake(df_clean)
