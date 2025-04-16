import schedule
import time
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from kafka_dir.producer.producer_stream import send_stream_data

schedule.every(5).minutes.do(send_stream_data)

while True:
    schedule.run_pending()
    time.sleep(1)
