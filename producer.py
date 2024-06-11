import pandas as pd
import time
from kafka import KafkaProducer
import json


class Producer:

    def __init__(self):
        self.topic_name = "fxrates"
        self.data_path = "rates_sample.csv"
        self.kafka_servers = ['localhost:9092']

    def produce_data(self):
        producer = KafkaProducer(bootstrap_servers=self.kafka_servers)
        df = pd.read_csv(self.data_path)
        for index, row in df.iterrows():
            time.sleep(0.2)
            data = {
                "event_time_unix": row["event_time"],
                "ccy_couple": row["ccy_couple"],
                "rate": row["rate"]
            }
            producer.send(self.topic_name, json.dumps(data).encode('utf-8'))
        producer.flush()


if __name__ == "__main__":
    producer = Producer()
    producer.produce_data()
