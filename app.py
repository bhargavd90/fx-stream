from kafka import KafkaConsumer
import streamlit as st
import pandas as pd
import json


prices = {}
consumer_sink = KafkaConsumer(
    "resultfxrates",
    bootstrap_servers=['localhost:9092'],
    group_id='resultfxrates',
    value_deserializer=lambda x: x.decode('utf-8')
)
st.title("streaming fx-rates data")
with st.empty():
    for record in consumer_sink:
        sink_data = json.loads(record.value)
        if sink_data["time_diff"] > 9620520:
            status = "Inactive"
        else:
            status = "Active"
        prices[sink_data["ccy_couple"]] = [sink_data["event_time"], sink_data["rate"], sink_data["time_diff"], status]
        df = pd.DataFrame.from_dict(prices, orient='index', columns=["event_time", "rate", "time_diff", "status"])
        st.dataframe(df)

