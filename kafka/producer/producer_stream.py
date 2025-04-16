from kafka import KafkaProducer
import json
import time
from data_fetcher.open_meteo_api import fetch_realtime_weather

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_stream_data():
    data = fetch_realtime_weather(11.01, 76.96)
    weather = data["current_weather"]
    weather["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
    producer.send('weather_stream', value=weather)
    print(f"Sent stream: {weather}")
