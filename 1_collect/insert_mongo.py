# weather-pipeline/1_collect/insert_mongo.py
from pymongo import MongoClient
from fetch_weather import collect_all
from config import MONGO_URI, MONGO_DB, MONGO_COLLECTION
from datetime import datetime

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

def insert_data():
    data = collect_all()
    for doc in data:
        doc["fetched_at"] = datetime.utcnow()
    if data:
        collection.insert_many(data)
        print(f"{len(data)} documents insérés.")
    else:
        print("Aucune donnée à insérer.")

if __name__ == "__main__":
    insert_data()

