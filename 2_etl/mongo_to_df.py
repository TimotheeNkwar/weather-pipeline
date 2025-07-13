# weather-pipeline/2_etl/mongo_to_df.py

import pandas as pd
from pymongo import MongoClient
from config import MONGO_URI, MONGO_DB, MONGO_COLLECTION

def read_mongo(limit=100):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    cursor = collection.find().sort("fetched_at", -1).limit(limit)
    data = list(cursor)
    return pd.DataFrame(data)

if __name__ == "__main__":
    df = read_mongo()
    print(df.head())
