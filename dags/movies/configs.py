KAFKA_SERVER = '102.210.148.191:9092'
KAFKA_TOPIC = 'airflow-test-movies'

DATA_SOURCE = '/opt/airflow/dags/movies/movielens_data.csv'

filename = DATA_SOURCE
topic = KAFKA_TOPIC
bootstrap_servers = KAFKA_SERVER




    #hdfs_path = '/sentences-data-lake/movies-rating6.csv'  # Path to datalake and storage location
hdfs_path = '/movies/movies-rating6.csv'  # Path to datalake and storage location


    # Create the consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # Start reading from the beginning of the topic
    #enable_auto_commit=True,
    group_id='defaulter3-consumer-group' #this should be changed on every run
)
    
hdfs_client = InsecureClient('http://102.210.148.191:9870', user='hadoop')

try:
    consume_and_write(consumer, hdfs_client, hdfs_path)
except Exception as e:
    print("An error occurred:", str(e))
finally:
        consumer.close()