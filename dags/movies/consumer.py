from kafka import KafkaConsumer
from hdfs import InsecureClient
import logging

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),  # Log to a file
        logging.StreamHandler()  # Output to notebook cell
    ]
)


KAFKA_SERVER = 'localhost:9092'
KAFKA_TOPIC = 'airflow-test-movies-1'



topic = KAFKA_TOPIC
bootstrap_servers = KAFKA_SERVER



def write_to_hdfs(message, hdfs_client, hdfs_path):
    print("write to hdfs started")
    try:
        # Check if the file exists
        if not hdfs_client.status(hdfs_path, strict=False):
            # If the file does not exist, create a new file
            with hdfs_client.write(hdfs_path, overwrite=True) as writer:
                writer.write(message.value + b'\n')
            print("Created new file and added data")
        else:
            # If the file exists, append to it
            with hdfs_client.write(hdfs_path, append=True) as writer:
                writer.write(message.value + b'\n')
                print("Added data to file")

    except Exception as e:
        print(f"Error writing to HDFS: {e}")
        logging.ERROR(f"Exception Raised: {e}")
    finally:
        print("write to hdfs ended")
def check_if_consumed_everything(consumer):
    """
    Check if the consumer has consumed all messages in the topic.
    """
    end_offsets = consumer.end_offsets(consumer.assignment())  # Get the latest offsets
    current_offsets = {tp: consumer.position(tp) for tp in consumer.assignment()}  # Get the current offsets

    for partition, end_offset in end_offsets.items():
        current_offset = current_offsets[partition]
        if current_offset < end_offset:
            print(f"Partition {partition}: Not yet fully consumed. Current: {current_offset}, End: {end_offset}")
            return False
    print("All partitions fully consumed.")
    return True

def consume_and_write(consumer, hdfs_client, hdfs_path):
    print("comsume write started")
    try:
        while True:
            # Poll for messages with a timeout of 1 second
            records = consumer.poll(timeout_ms=1000)
            if records:
                for partition, record_list in records.items():
                    #print(2)
                    for record in record_list:
                        logging.info("New Record read")
                        write_to_hdfs(record, hdfs_client, hdfs_path)
            else:
                print(f"No records to consume")
                
            # Check if all messages have been consumed
            if check_if_consumed_everything(consumer):
                print("All messages consumed. Exiting.")
                break
            
    except KeyboardInterrupt:
        print("Consumer stopped.")
        consumer.close()
        


if __name__ == "__main__":
    bootstrap_servers = 'localhost:9092'
    topic = 'test-movies' 

    #hdfs_path = '/sentences-data-lake/movies-rating6.csv'  # Path to datalake and storage location
    hdfs_path = '/movies/movies-rating.csv'  # Path to datalake and storage location


    # Create the consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',  # Start reading from the beginning of the topic
        #enable_auto_commit=True,
        group_id='defaulter99-consumer-group' #this should be changed on every run i dontknow why
    )
    
    hdfs_client = InsecureClient('http://localhost:9870', user='hadoop')

    try:
        consume_and_write(consumer, hdfs_client, hdfs_path)
    except Exception as e:
        print("An error occurred:", str(e))
    finally:
        consumer.close()