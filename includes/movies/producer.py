from kafka import KafkaProducer
from utils import count_lines
import csv

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

KAFKA_SERVER = '102.210.148.191:9092'
KAFKA_TOPIC = 'test-movies'
DATA_SOURCE = 'movielens_data.csv'


filename = DATA_SOURCE
topic = KAFKA_TOPIC
bootstrap_servers = KAFKA_SERVER



def thepublisher(producer, topic, filename):
    logging.info("Publisher started")
    
    batch_size = 10000
    
    # Count the total lines to calculate total batches
    total_lines = count_lines(filename)
    total_batches = (total_lines + batch_size - 1) // batch_size  # Calculate total batches needed
    logging.info(f"Total lines: {total_lines}, Batch size: {batch_size}, Total batches: {total_batches}")
    
    line_count = 0  # Total lines processed
    batch_count = 0  # Track batch number

    with open(filename, "rb") as file:
        batch = []
        
        for line in file:
            batch.append(line)
            line_count += 1
            
            # When batch is full, send to Kafka
            if len(batch) == batch_size:
                producer.send(topic, b''.join(batch))
                batch_count += 1
                remaining_batches = total_batches - batch_count
                logging.info(f"Batch {batch_count} sent with {len(batch)} records. Remaining batches: {remaining_batches}")
                batch.clear()  # Clear the batch for the next set
        
        # Send any remaining lines in the last batch
        if batch:
            producer.send(topic, b''.join(batch))
            batch_count += 1
            remaining_batches = total_batches - batch_count
            logging.info(f"Final batch {batch_count} sent with {len(batch)} records. Remaining batches: {remaining_batches}")
    
    logging.info(f"Total lines processed: {line_count}")
    logging.info("Publishing completed")