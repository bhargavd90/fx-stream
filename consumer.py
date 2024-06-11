from pyflink.table import EnvironmentSettings, TableEnvironment
import os
from kafka.admin import KafkaAdminClient, NewTopic


class Consumer:

    def __init__(self):
        self.table_env = None
        self.current_directory = os.getcwd()
        self.jar_file_path = "jars/flink-sql-connector-kafka-3.1.0-1.18.jar"
        self.final_result = None
        self.bootstrap_server_source = 'localhost:9092'
        self.topic_name_source = 'fxrates'
        self.num_partitions_source = 1
        self.replication_factor_source = 1

    def create_topic_source(self):
        admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_server_source)
        topic = NewTopic(name=self.topic_name_source, num_partitions=self.num_partitions_source, replication_factor=self.replication_factor_source)
        admin_client.create_topics([topic])

    def create_table_env(self):
        env_settings = EnvironmentSettings.in_streaming_mode()
        table_env = TableEnvironment.create(env_settings)
        jar_url = f"file:///{self.current_directory}/{self.jar_file_path}"
        table_env.get_config().get_configuration().set_string("pipeline.jars", jar_url)
        # table_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "5000")
        table_env.get_config().get_configuration().set_string("parallelism.default", "4")
        table_env.get_config().get_configuration().set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
        table_env.get_config().get_configuration().set_string("execution.checkpointing.checkpoints-directory", self.current_directory)
        self.table_env = table_env

    def fetch_stream_data(self):
        fx_data = """
        CREATE TABLE IF NOT EXISTS fx_data (
            event_time_unix BIGINT,
            ccy_couple STRING,
            rate FLOAT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'fxrates',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'fxrates',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        );
        """
        self.table_env.execute_sql(fx_data)

    def send_stream_data_to_kafka_topic(self):
        sql = """
            SELECT
                TO_TIMESTAMP_LTZ(event_time_unix, 3) AS event_time,
                ccy_couple,
                rate,
                TIMESTAMPDIFF(SECOND, TO_TIMESTAMP_LTZ(event_time_unix, 3), NOW()) AS time_diff
            FROM fx_data
            """
        fx_tbl = self.table_env.sql_query(sql)

        sink_ddl = """
                CREATE TABLE result_data_table (
                    event_time TIMESTAMP(3),
                    ccy_couple VARCHAR,
                    rate FLOAT,
                    time_diff FLOAT
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = 'resultfxrates',
                    'properties.bootstrap.servers' = 'localhost:9092',
                    'properties.group.id' = 'resultfxrates',
                    'format' = 'json'
                )
            """
        self.table_env.execute_sql(sink_ddl)
        print("ready to consume data")
        fx_tbl.execute_insert('result_data_table').wait()
        self.table_env.execute('windowed-fx-rates')

    def consume_data(self):
        self.create_topic_source()
        self.create_table_env()
        self.fetch_stream_data()
        self.send_stream_data_to_kafka_topic()


if __name__ == "__main__":
    consumer = Consumer()
    consumer.consume_data()
