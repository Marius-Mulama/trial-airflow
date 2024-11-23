# from airflow import DAG
# from airflow.providers.apache.kafka.operators.kafka_producer import KafkaProducerOperator
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# from utils.file_utils import count_lines

# # Default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# KAFKA_SERVER = '102.210.148.191:9092'
# KAFKA_TOPIC = 'airflow-test-movies'
# DATA_SOURCE = 'movielens_data.csv'


# def get_batches_from_file(filename, batch_size=10000):
#     """Utility function to read the file in batches and return each batch as a list of lines."""
#     with open(filename, 'rb') as file:
#         batch = []
#         for line in file:
#             batch.append(line)
#             if len(batch) == batch_size:
#                 yield batch
#                 batch = []
#         if batch:
#             yield batch


# def prepare_batches():
#     """This function prepares batches from the file for KafkaProducerOperator."""
#     batches = []
#     for batch in get_batches_from_file(DATA_SOURCE):
#         batches.append({
#             'topic': KAFKA_TOPIC,
#             'value': b''.join(batch),
#             'key': None,
#             'partition': None,
#             'headers': None,
#         })
#     return batches


# with DAG(
#     'kafka_publish_dag',
#     default_args=default_args,
#     description='A simple Kafka publisher DAG using KafkaProducerOperator',
#     schedule_interval=None,  # Triggered manually
#     start_date=datetime(2024, 11, 20),
#     catchup=False,
# ) as dag:

#     # Task to publish data to Kafka using KafkaProducerOperator
#     publish_task = KafkaProducerOperator(
#         task_id='publish_to_kafka_task',
#         kafka_config={
#             'bootstrap_servers': KAFKA_SERVER,
#         },
#         producer_callable=prepare_batches,  # The callable that prepares the batches for Kafka
#     )
    
#     # Additional tasks can be added here if needed.
