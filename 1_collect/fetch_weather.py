import os
import time
import requests
import pandas as pd

from pymongo import MongoClient
from config import API_KEY, MONGO_URI, MONGO_DB, MONGO_COLLECTION
from datetime import datetime, timezone

# Charger les villes
cities_df = pd.read_csv(
    "C:\\Users\\KOVVO\\OneDrive\\Documents\\GitHub\\Data-Science-Projects\\projet1\\Schooll_Project\\Pipeline_Weather\\cities5000.txt",
    sep="\t", header=None, names=[
        "geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude",
        "feature class", "feature code", "country code", "cc2", "admin1 code", "admin2 code",
        "admin3 code", "admin4 code", "population", "elevation", "dem", "timezone", "modification date"
    ]
)

cities = cities_df[["name", "latitude", "longitude"]].dropna().drop_duplicates().to_dict(orient="records")

# Connexion MongoDB
client = MongoClient(MONGO_URI)
collection = client[MONGO_DB][MONGO_COLLECTION]

# Fonction d'appel API
def fetch_current_weather(lat, lon):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ {lat},{lon} : {response.status_code}")
    except Exception as e:
        print(f"⚠️ Network error for {lat},{lon}: {e}")
    return None

# Boucle principale
for i, city in enumerate(cities):
    name = city["name"]
    lat = city["latitude"]
    lon = city["longitude"]
    print(f"\n🌍 {i+1}/{len(cities)} - {name}")

    # Vérifier si la donnée est déjà présente pour cette ville aujourd’hui
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    exists = collection.find_one({
        "city": name,
        "$expr": {
            "$eq": [{"$dateToString": {"format": "%Y-%m-%d", "date": "$fetched_at"}}, today_str]
        }
    })

    if exists:
        print(f"⚠️ Donnée déjà existante pour {name} le {today_str}")
        continue

    # Récupérer et insérer si non existant
    weather_data = fetch_current_weather(lat, lon)
    if weather_data:
        weather_data.update({
            "city": name,
            "latitude": lat,
            "longitude": lon,
            "fetched_at": datetime.now(timezone.utc)
        })
        collection.insert_one(weather_data)
        print(f"✅ Donnée insérée pour {name}")
    else:
        print(f"❌ Aucune donnée pour {name}")

    time.sleep(1.2)  # Respecter la limite de l'API gratuite
