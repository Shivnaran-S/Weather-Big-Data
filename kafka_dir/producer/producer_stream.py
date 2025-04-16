from kafka import KafkaProducer
import json
import time
from datetime import datetime
import requests

producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',  # use host.docker.internal if this doesn't work
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=5,
    request_timeout_ms=20000
)

while True:
    # Replace with your actual lat/lon
    response = requests.get(
        "https://api.open-meteo.com/v1/forecast?latitude=11.0168&longitude=76.9558&current=temperature_2m,windspeed_10m,winddirection_10m,is_day,weathercode"
    )

    data = response.json()
    current = data.get("current", {})

    weather = {
        "time": current.get("time"),
        "temperature": current.get("temperature_2m"),
        "windspeed": current.get("windspeed_10m"),
        "winddirection": current.get("winddirection_10m"),
        "is_day": current.get("is_day"),
        "weathercode": current.get("weathercode"),
        "timestamp": str(datetime.now())
    }

    print("Sent stream:", weather)
    producer.send("weather_stream", value=weather)
    producer.flush()  # ensures it gets sent now

    time.sleep(10)

'''
from kafka import KafkaProducer
import json
import time
from data_fetcher.open_meteo_api import fetch_realtime_weather

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=5,
    request_timeout_ms=20000
)

def send_stream_data():
    data = fetch_realtime_weather(11.01, 76.96)
    weather = data["current_weather"]
    weather["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
    producer.send('weather_stream', value=weather)
    print(f"Sent stream: {weather}")

if __name__=="__main__":
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    send_stream_data()
'''