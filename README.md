# fx-stream

1) This app shows the streaming fx-rates data and its status
2) We could also eliminate records based on the time_diff and show only the active ones
3) The stream of data is mimiced by pushing data from a dataframe to a kafka cluster with a time interval of 0.2 seconds


**docker_compose.yml** : This runs the kafka cluster to create, push, fetch data from the topics

**producer.py** : This loads the data from the source, in this case a csv file and pushes the data in to the kafka topic

**consumer.py** : This runs a Flink cluster to fetch the data from the kafka topic, do necessary processing on the streaming data and pushes the Sink data to a different kafka topic

**app.py** : This runs the streamlit app to display the streaming fx-rates data from the Sink kafka topic


<img width="1154" alt="Screenshot 2024-06-11 at 6 18 45 PM" src="https://github.com/bhargavd90/fx-stream/assets/63107204/de328215-8974-4e05-8090-e42ffab7a7cb">
