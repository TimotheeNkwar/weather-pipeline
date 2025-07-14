import os
import time
import requests
import pandas as pd
from pymongo import MongoClient
from config import API_KEY, MONGO_URI, MONGO_DB, MONGO_COLLECTION
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Créer un index unique sur 'city' pour éviter les doublons
try:
    collection.create_index([("city", 1)], unique=True)
except Exception as e:
    print(f"⚠️ Erreur lors de la création de l'index unique: {e}")
    print(
        "Vérifiez s'il existe des doublons dans la collection. Vous pouvez les supprimer avec une commande MongoDB si nécessaire.")


# Fonction d'appel API avec ajout des champs demandés
def fetch_current_weather(city):
    lat = city["latitude"]
    lon = city["longitude"]
    name = city["name"]
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if "main" in data and "temp" in data["main"]:
                data.update({
                    "city": name,
                    "latitude": lat,
                    "longitude": lon,
                    "fetched_at": datetime.now(timezone.utc),
                    "country": data.get("sys", {}).get("country"),
                    "wind_speed": data.get("wind", {}).get("speed"),
                    "temp_min": data.get("main", {}).get("temp_min"),
                    "temp_max": data.get("main", {}).get("temp_max")
                })
                return data
            else:
                print(f"❌ Données incomplètes pour {name} ({lat},{lon})")
        else:
            print(f"❌ {name} ({lat},{lon}) : {response.status_code}")
    except Exception as e:
        print(f"⚠️ Erreur réseau pour {name} ({lat},{lon}): {e}")
    return None


# Fonction pour traiter un lot de villes
def process_batch(batch_cities, existing_cities):
    batch_data = []
    errors = []
    for city in batch_cities:
        name = city["name"]
        if name in existing_cities:
            print(f"⚠️ Donnée déjà existante pour {name}, ignorée")
            continue

        weather_data = fetch_current_weather(city)
        if weather_data:
            batch_data.append(weather_data)
        else:
            errors.append(f"{name},{city['latitude']},{city['longitude']}")
    return batch_data, errors


# Boucle principale
def main():
    # Récupérer toutes les villes existantes dans la collection
    existing_cities = set(collection.distinct("city"))
    print(f"📋 {len(existing_cities)} villes déjà présentes dans la collection.")

    # Paramètres pour la parallélisation
    max_workers = 10  # Nombre de threads (max 60 requêtes/minute)
    batch_size = 50  # Taille des lots pour insertion
    rate_limit_delay = 1.0  # Délai pour respecter 60 requêtes/minute

    # Diviser les villes en lots pour l'insertion
    batches = [cities[i:i + batch_size] for i in range(0, len(cities), batch_size)]
    total_inserted = 0
    all_errors = []

    for batch_idx, batch in enumerate(batches):
        print(f"\n📊 Traitement du lot {batch_idx + 1}/{len(batches)} ({len(batch)} villes)")

        # Traiter le lot en parallèle
        batch_data = []
        batch_errors = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_city = {executor.submit(process_batch, [city], existing_cities): city for city in batch}
            for future in as_completed(future_to_city):
                data, errors = future.result()
                batch_data.extend(data)
                batch_errors.extend(errors)

        # Insérer les données en masse
        if batch_data:
            try:
                result = collection.insert_many(batch_data, ordered=False)
                inserted_count = len(result.inserted_ids)
                total_inserted += inserted_count
                print(f"✅ {inserted_count} données insérées pour ce lot")
            except Exception as e:
                print(f"❌ Erreur lors de l'insertion en masse: {e}")
                for doc in batch_data:
                    try:
                        collection.insert_one(doc)
                        total_inserted += 1
                        print(f"✅ Donnée insérée pour {doc['city']}")
                    except Exception as e:
                        print(f"❌ Échec insertion pour {doc['city']}: {e}")

        # Ajouter les erreurs au fichier avec encodage UTF-8
        if batch_errors:
            with open("erreurs_villes.txt", "a", encoding="utf-8") as f:
                f.write("\n".join(batch_errors) + "\n")
            all_errors.extend(batch_errors)

        # Respecter la limite de l'API
        time.sleep(rate_limit_delay)

    print(f"\n✅ Total: {total_inserted} nouvelles données insérées.")
    if all_errors:
        print(f"❌ {len(all_errors)} villes ont échoué (voir erreurs_villes.txt).")

    client.close()  # Fermer la connexion MongoDB


if __name__ == "__main__":
    main()