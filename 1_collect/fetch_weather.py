# weather-pipeline/1_collect/fetch_weather.py

import requests
from config import API_KEY, CITIES

def fetch_weather(city):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    return response.json() if response.status_code == 200 else None

def collect_all():
    data = []
    for city in CITIES:
        weather = fetch_weather(city)
        if weather:
            data.append(weather)
    return data

if __name__ == "__main__":
    results = collect_all()
    print(results)
