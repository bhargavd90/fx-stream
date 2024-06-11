# fx-stream

**docker_compose.yml** : This runs the kafka cluster to create, push, fetch data from the topics

**producer.py** : This loads the data from the source, in this case a csv file and pushes the data in to the kafka topic

**consumer.py** : This runs a Flink cluster to fetch the data from the kafka topic, do necessary processing on the streamoing data and pushes the Sink data to a different kafka topic

**app.py** : This runs the streamlit app to display the streaming fx-rates data from the Sink kafka topic
