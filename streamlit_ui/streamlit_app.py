# streamlit_ui/streamlit_app.py

import streamlit as st
import threading
from kafka import KafkaConsumer
import json

# Initialize session state
if "messages" not in st.session_state:
    st.session_state["messages"] = []

# Kafka consumer function
def start_kafka_consumer():
    consumer = KafkaConsumer(
        'weather_stream',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        consumer_timeout_ms=10000  # 10 seconds wait before timeout
    )
    print("Started consuming weather data...\n")
    for msg in consumer:
        st.session_state["messages"].append(msg.value)
    consumer.close()

# UI
st.title("🌤️ Real-time Weather Data Stream")

if st.button("Start Streaming"):
    if "consumer_thread" not in st.session_state:
        consumer_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
        consumer_thread.start()
        st.session_state["consumer_thread"] = consumer_thread
    st.success("Started Kafka Consumer.")

st.write("### Weather Data Stream:")
if st.session_state["messages"]:
    for msg in reversed(st.session_state["messages"][-10:]):  # show last 10 messages
        st.json(msg)
else:
    st.info("Waiting for data...")
