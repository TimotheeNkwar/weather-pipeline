import pandas as pd

def clean_weather_data(df):
    records = []
    for _, row in df.iterrows():
        city = row.get("name") or row.get("city")
        main = row.get("main", {})
        sys = row.get("sys", {})  
        weather = row.get("weather", [{}])[0]
        dt = row.get("fetched_at")

        record = {
            "city": city,
            "temperature": main.get("temp"),
            "humidity": main.get("humidity"),
            "pressure": main.get("pressure"),
            "weather_main": weather.get("main"),
            "weather_description": weather.get("description"),
            "fetched_at": dt,
            "country": sys.get("country"),
            "wind_speed": row.get("wind", {}).get("speed"),
            "temp_min": main.get("temp_min"),
            "temp_max": main.get("temp_max")
        }
        records.append(record)

    return pd.DataFrame(records)
