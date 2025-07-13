# weather-pipeline/2_etl/transform.py

def clean_weather_data(df):
    # Sélection des colonnes importantes
    records = []
    for _, row in df.iterrows():
        city = row.get("name")
        main = row.get("main", {})
        weather = row.get("weather", [{}])[0]
        dt = row.get("fetched_at")

        record = {
            "city": city,
            "temperature": main.get("temp"),
            "humidity": main.get("humidity"),
            "pressure": main.get("pressure"),
            "weather_main": weather.get("main"),
            "weather_description": weather.get("description"),
            "fetched_at": dt
        }
        records.append(record)

    return pd.DataFrame(records)
