import requests

def fetch_historical_weather(latitude, longitude, start_date, end_date):
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&hourly=temperature_2m,relative_humidity_2m,precipitation"
    return requests.get(url).json()

def fetch_realtime_weather(latitude, longitude):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
    return requests.get(url).json()
