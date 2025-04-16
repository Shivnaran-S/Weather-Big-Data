from kafka import KafkaProducer
import json
import time
from data_fetcher.open_meteo_api import fetch_historical_weather

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_batch_data():
    data = fetch_historical_weather(11.01, 76.96, "2023-01-01", "2023-01-05")
    for i in range(len(data['hourly']['time'])):
        weather = {
            "time": data['hourly']['time'][i],
            "temp": data['hourly']['temperature_2m'][i],
            "humidity": data['hourly']['relative_humidity_2m'][i],
            "precip": data['hourly']['precipitation'][i],
        }
        producer.send('weather_batch', value=weather)
        print(f"Sent batch: {weather}")
        time.sleep(0.1)

if __name__=="__main__":
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    send_batch_data()
