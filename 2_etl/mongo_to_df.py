import pandas as pd
from pymongo import MongoClient
from config import MONGO_URI, MONGO_DB, MONGO_COLLECTION

def read_mongo(limit=100):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    # Lire les dernières entrées
    cursor = collection.find().sort("fetched_at", -1).limit(limit)
    data = list(cursor)

    if not data:
        print("⚠️ Aucune donnée trouvée dans la collection MongoDB.")
        return pd.DataFrame()

    # Convertir en DataFrame
    df = pd.DataFrame(data)

    # Supprimer les colonnes Mongo inutiles
    df.drop(columns=["_id"], inplace=True, errors="ignore")

    return df

if __name__ == "__main__":
    df = read_mongo()
    print(df.head())
