from pymongo import MongoClient
from fetch_weather import collect_all
from config import MONGO_URI, MONGO_DB, MONGO_COLLECTION
from datetime import datetime

# Connexion à MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

def insert_data():
    data = collect_all()
    inserted = 0
    today_str = datetime.utcnow().strftime("%Y-%m-%d")

    for doc in data:
        doc["fetched_at"] = datetime.utcnow()
        city = doc.get("name") or doc.get("city")

        if not city:
            print("❌ Aucune ville trouvée dans le document, ignoré.")
            continue

        # Vérifie s'il existe déjà une entrée pour cette ville aujourd'hui
        existing = collection.find_one({
            "city": city,
            "fetched_at": {
                "$gte": datetime.strptime(today_str, "%Y-%m-%d"),
                "$lt": datetime.strptime(today_str, "%Y-%m-%d") + timedelta(days=1)
            }
        })

        if not existing:
            collection.insert_one(doc)
            inserted += 1
        else:
            print(f"⚠️ Donnée déjà présente pour {city} aujourd’hui.")

    print(f"\n✅ {inserted} nouvelles données insérées.")

if __name__ == "__main__":
    insert_data()
