import os
import time
import requests
import pandas as pd
from pymongo import MongoClient
from config import API_KEY, MONGO_URI, MONGO_DB, MONGO_COLLECTION
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load cities
cities_df = pd.read_csv(
    "C:\\Users\\KOVVO\\OneDrive\\Documents\\GitHub\\Data-Science-Projects\\projet1\\Schooll_Project\\Pipeline_Weather\\cities5000.txt",
    sep="\t", header=None, names=[
        "geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude",
        "feature class", "feature code", "country code", "cc2", "admin1 code", "admin2 code",
        "admin3 code", "admin4 code", "population", "elevation", "dem", "timezone", "modification date"
    ]
)

cities = cities_df[["name", "latitude", "longitude"]].dropna().drop_duplicates().to_dict(orient="records")

# MongoDB connection
client = MongoClient(MONGO_URI)
collection = client[MONGO_DB][MONGO_COLLECTION]

# Create unique index on 'city' to avoid duplicates
try:
    collection.create_index([("city", 1)], unique=True)
except Exception as e:
    print(f"⚠️ Error creating unique index: {e}")
    print("Check for duplicates in the collection. You may need to remove them with a MongoDB command.")

# API call function with added fields
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
                print(f"❌ Incomplete data for {name} ({lat},{lon})")
        else:
            print(f"❌ {name} ({lat},{lon}): HTTP {response.status_code}")
    except Exception as e:
        print(f"⚠️ Network error for {name} ({lat},{lon}): {e}")
    return None

# Process batch of cities
def process_batch(batch_cities, existing_cities):
    batch_data = []
    errors = []
    for city in batch_cities:
        name = city["name"]
        if name in existing_cities:
            print(f"⚠️ Data already exists for {name}, skipping")
            continue

        weather_data = fetch_current_weather(city)
        if weather_data:
            batch_data.append(weather_data)
        else:
            errors.append(f"{name},{city['latitude']},{city['longitude']}")
    return batch_data, errors

# Main loop
def main():
    # Get all existing cities in the collection
    existing_cities = set(collection.distinct("city"))
    print(f"📋 {len(existing_cities)} cities already in the collection.")

    # Parallelization parameters
    max_workers = 10  # Number of threads (max 60 requests/min)
    batch_size = 50   # Batch size for insertion
    rate_limit_delay = 1.0  # Delay to respect 60 requests/min

    # Split cities into batches
    batches = [cities[i:i + batch_size] for i in range(0, len(cities), batch_size)]
    total_inserted = 0
    all_errors = []

    for batch_idx, batch in enumerate(batches):
        print(f"\n📊 Processing batch {batch_idx + 1}/{len(batches)} ({len(batch)} cities)")

        batch_data = []
        batch_errors = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_city = {executor.submit(process_batch, [city], existing_cities): city for city in batch}
            for future in as_completed(future_to_city):
                data, errors = future.result()
                batch_data.extend(data)
                batch_errors.extend(errors)

        # Bulk insert data
        if batch_data:
            try:
                result = collection.insert_many(batch_data, ordered=False)
                inserted_count = len(result.inserted_ids)
                total_inserted += inserted_count
                print(f"✅ {inserted_count} documents inserted for this batch")
            except Exception as e:
                print(f"❌ Bulk insertion error: {e}")
                for doc in batch_data:
                    try:
                        collection.insert_one(doc)
                        total_inserted += 1
                        print(f"✅ Document inserted for {doc['city']}")
                    except Exception as e:
                        print(f"❌ Failed insertion for {doc['city']}: {e}")

        # Log errors to file with UTF-8 encoding
        if batch_errors:
            with open("city_errors.txt", "a", encoding="utf-8") as f:
                f.write("\n".join(batch_errors) + "\n")
            all_errors.extend(batch_errors)

        # Respect API rate limit
        time.sleep(rate_limit_delay)

    print(f"\n✅ Total: {total_inserted} new documents inserted.")
    if all_errors:
        print(f"❌ {len(all_errors)} cities failed (see city_errors.txt).")

    client.close()  # Close MongoDB connection

if __name__ == "__main__":
    main()
