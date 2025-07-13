# weather-pipeline/2_etl/load_to_snowflake.py

import pandas as pd
from sqlalchemy import create_engine
from config import SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE
from mongo_to_df import read_mongo
from transform import clean_weather_data

def get_engine():
    url = (
        f"snowflake://{TIMOTHEE}:{22205731@Timothee}@{SNOWFLAKE_ACCOUNT}/"
        f"{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}"
    )
    return create_engine(url)

def load_to_snowflake(df, table_name="weather_data"):
    engine = get_engine()
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"{len(df)} lignes insérées dans {table_name}.")

if __name__ == "__main__":
    df_mongo = read_mongo(limit=100)
    df_clean = clean_weather_data(df_mongo)
    load_to_snowflake(df_clean)
