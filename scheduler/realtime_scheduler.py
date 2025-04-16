import schedule
import time
from kafka.producer.producer_stream import send_stream_data

schedule.every(5).minutes.do(send_stream_data)

while True:
    schedule.run_pending()
    time.sleep(1)
