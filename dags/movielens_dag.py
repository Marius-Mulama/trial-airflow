from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator, BranchPythonOperator # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.decorators import task

#from kafka import KafkaProducer
from kafka import KafkaConsumer


from datetime import datetime

#from movies.producer import thepublisher

import movies.configs as conf


# def _produce_from_csv_to_kafka():    
#     bootstrap_servers = conf.KAFKA_SERVER
#     topic = conf.KAFKA_TOPIC
#     filename = conf.DATA_SOURCE
    
#     producer = KafkaProducer(bootstrap_servers= bootstrap_servers)
#     thepublisher(producer, topic, filename)
#     producer.close()


def _consume_from_kafka_to_hadoop():
    print("Consmuer Called")
    return True

with DAG("movie-lens-dag", start_date=datetime(2024,1,1),
         schedule_interval="@daily", catchup=False) as dag:
    
    @task(task_id="move_from_csv_to_kafka")
    def produce_to_kafka():
        
        #do imports
        from kafka import KafkaProducer
        from movies.producer import thepublisher
        
        
        bootstrap_servers = conf.KAFKA_SERVER
        topic = conf.KAFKA_TOPIC
        filename = conf.DATA_SOURCE

        producer = KafkaProducer(bootstrap_servers= bootstrap_servers)
        thepublisher(producer, topic, filename)
        producer.close()
        
    
    @task(task_id="move_from_csv_to_kafka")
    def consume_to_kafka():
        print(f"Consume to kafka started")
        
        from hdfs import InsecureClient 
        import movies.consumer as cs
        
        bootstrap_servers = 'localhost:9092'
        topic = conf.KAFKA_TOPIC

        #hdfs_path = '/sentences-data-lake/movies-rating6.csv'  # Path to datalake and storage location
        hdfs_path = '/movies/movies-rating6.csv'  # Path to datalake and storage location
        
        
        #consumer = conf.KAFKA_CONSUMER
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',  # Start reading from the beginning of the topic
            #enable_auto_commit=True,
            group_id='defaulter3-consumer-group' #this should be changed on every run
            )


        
        hdfs_client = InsecureClient('http://localhost:9870', user='hadoop')

        try:
            cs.consume_and_write(consumer, hdfs_client, hdfs_path)
        except Exception as e:
            print("An error occurred:", str(e))
        finally:
            consumer.close()
            
    
    run_python_script = BashOperator(
        task_id='move_from_kafka_to_hadoop',
        bash_command=f"python {SCRIPT_PATH}"  # Use Python to execute the script
    )
    
    
    produce_to_kafka() >> consume_to_kafka()
    