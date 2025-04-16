from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'weather_stream',#'weather_batch',  # or 'weather_stream'
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consuming...")
for message in consumer:
    print(message.value)

