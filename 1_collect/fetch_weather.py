import os
import time
import requests
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from config import API_KEY, MONGO_URI, MONGO_DB, MONGO_COLLECTION

# Charger les villes
cities_df = pd.read_csv("C:\\Users\\KOVVO\\OneDrive\\Documents\\GitHub\\Data-Science-Projects\\projet1\\Schooll_Project\\Pipeline_Weather\\cities5000.txt", sep="\t", header=None, names=[
    "geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude",
    "feature class", "feature code", "country code", "cc2", "admin1 code", "admin2 code",
    "admin3 code", "admin4 code", "population", "elevation", "dem", "timezone", "modification date"
])

cities = cities_df[["name", "latitude", "longitude"]].dropna().drop_duplicates().to_dict(orient="records")

# Connexion MongoDB
client = MongoClient(MONGO_URI)
collection = client[MONGO_DB][MONGO_COLLECTION]

# Fonction d'appel API (actuelle)
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

# Boucle principale (une seule observation par ville)
for i, city in enumerate(cities):
    name = city["name"]
    lat = city["latitude"]
    lon = city["longitude"]
    print(f"\n🌍 {i+1}/{len(cities)} - {name}")

    weather_data = fetch_current_weather(lat, lon)
    if weather_data:
        weather_data.update({
            "city": name,
            "latitude": lat,
            "longitude": lon,
            "fetched_at": datetime.utcnow()
        })
        collection.insert_one(weather_data)

    time.sleep(1.2)  # respecter la limite gratuite
