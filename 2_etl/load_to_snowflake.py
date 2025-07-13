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

    # Créer la table si elle n'existe pas
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

    # Insertion sécurisée avec gestion d’erreur
    inserted = 0
    for _, row in df.iterrows():
        try:
            insert_stmt = f"""
            INSERT INTO {table_name} (city, temperature, humidity, pressure, weather_main, weather_description, fetched_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_stmt, (
                row.get('city'),
                row.get('temperature'),
                row.get('humidity'),
                row.get('pressure'),
                row.get('weather_main'),
                row.get('weather_description'),
                row.get('fetched_at')
            ))
            inserted += 1
        except Exception as e:
            print(f"❌ Échec insertion pour {row.get('city')} → {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ {inserted} lignes insérées dans {table_name}.")

if __name__ == "__main__":
    df_mongo = read_mongo(limit=100)
    df_clean = clean_weather_data(df_mongo)

    # 🔧 Corriger les datetime → string (Snowflake ne supporte pas tous les types)
    for col in df_clean.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
        df_clean[col] = df_clean[col].astype(str)

    load_to_snowflake(df_clean)
