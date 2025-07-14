from mongo_to_df import read_mongo
from transform import clean_weather_data
import snowflake.connector
from config import *
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

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

    # Créer la table si elle n'existe pas
    create_stmt = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        city STRING,
        temperature FLOAT,
        humidity INT,
        pressure INT,
        weather_main STRING,
        weather_description STRING,
        fetched_at TIMESTAMP,
        country STRING,
        wind_speed FLOAT,
        temp_min FLOAT,
        temp_max FLOAT,
        PRIMARY KEY (city)
    )
    """
    cursor.execute(create_stmt)

    # Créer une table temporaire pour charger les données
    temp_table = f"{table_name}_temp"
    cursor.execute(f"CREATE TEMPORARY TABLE {temp_table} LIKE {table_name}")

    # Charger le DataFrame dans la table temporaire
    success, nchunks, nrows, _ = write_pandas(conn, df, temp_table)
    if not success:
        print(f"❌ Échec du chargement dans la table temporaire {temp_table}")
        cursor.close()
        conn.close()
        return

    # Fusionner les données de la table temporaire vers la table finale
    merge_stmt = f"""
    MERGE INTO {table_name} AS target
    USING {temp_table} AS source
    ON target.city = source.city
    WHEN NOT MATCHED THEN
        INSERT (city, temperature, humidity, pressure, weather_main, weather_description, 
                fetched_at, country, wind_speed, temp_min, temp_max)
        VALUES (source.city, source.temperature, source.humidity, source.pressure, 
                source.weather_main, source.weather_description, source.fetched_at,
                source.country, source.wind_speed, source.temp_min, source.temp_max)
    """
    cursor.execute(merge_stmt)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ {nrows} lignes traitées dans {table_name} (doublons ignorés).")

if __name__ == "__main__":
    df_mongo = read_mongo()  # Tous les documents, sans limite
    df_clean = clean_weather_data(df_mongo)

    # Convertir les colonnes datetime en string pour Snowflake
    for col in df_clean.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
        df_clean[col] = df_clean[col].astype(str)

    load_to_snowflake(df_clean)