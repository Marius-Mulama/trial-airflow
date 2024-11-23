import os
import logging
import psycopg2
from psycopg2.extras import execute_batch
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),  # Log to a file
        logging.StreamHandler()  # Output to notebook cell
    ]
)

os.environ["HADOOP_USER_NAME"] = "hadoop"

# Create or get the existing Spark session
spark = SparkSession.builder.config("spark.hadoop.fs.defaultFS",
                                    "hdfs://localhost:9000").appName("HadoopToSpark").getOrCreate()

# PostgreSQL connection details
db_config = {
    "host": "localhost",
    "port": 5432,
    "database": "test_db",
    "user": "bans",
    "password": "T3st3r"
}

# HDFS file path
hdfs_file_path = "hdfs://localhost:9000/movies/movies-rating.csv"


def clean_data(hdfs_file_path):
    try:
        # Read the MovieLens data from HDFS into a DataFrame
        movie_df = spark.read.csv(hdfs_file_path, header=True, inferSchema=True)
    except Exception as e:
        logging.exception(f"Error reading file: {e}")
        raise Exception
    finally:
        logging.info("Reading ended here")
        
    # Convert the 'rating' column from float to int
    movie_df = movie_df.withColumn('rating', col('rating').cast('int'))

    # Check for missing values
    movie_df.describe().show()

    # Drop rows with missing values (if necessary)
    movie_df_clean = movie_df.dropna()

    # Remove duplicates
    movie_df_clean = movie_df_clean.dropDuplicates()

    # Show cleaned data (logging the first 5 rows)
    movie_df_clean.show(5)

    logging.info("Data cleaning completed successfully.")
    
    return movie_df_clean



def create_table_if_not_exists(db_config):
    """Creates the movie_ratings table if it does not already exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS movie_ratings (
        movieId INT,
        title VARCHAR(255),
        genres VARCHAR(255),
        userId INT,
        rating INT,
        timestamp BIGINT
    );
    """
    try:
        # Establish connection to PostgreSQL
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            database=db_config["database"],
            user=db_config["user"],
            password=db_config["password"]
        )
        cursor = conn.cursor()
        
        # Execute table creation query
        cursor.execute(create_table_query)
        conn.commit()
        logging.info("Table created successfully (or already exists).")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def insert_spark_df_to_postgres(spark_df, table_name, db_config, batch_size=10000):

    # Convert PySpark DataFrame to Pandas DataFrame
    pandas_df = spark_df.toPandas()
    
    # Prepare insert query
    columns = list(pandas_df.columns)
    insert_query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
    """

    # Helper function to execute batch insert for each partition
    def insert_partition(partition_df):
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            database=db_config["database"],
            user=db_config["user"],
            password=db_config["password"]
        )
        conn.autocommit = False  # Disable autocommit for better performance
        cursor = conn.cursor()

        try:
            # Convert partition DataFrame to list of tuples
            batch_data = partition_df.to_records(index=False).tolist()
            execute_batch(cursor, insert_query, batch_data)
            conn.commit()  # Commit the transaction
        except Exception as e:
            logging.error(f"Error during batch insert: {e}")
        finally:
            cursor.close()
            conn.close()

    # Split the DataFrame into partitions
    num_partitions = 4  # Can be adjusted based on the system
    partitions = [pandas_df.iloc[i::num_partitions] for i in range(num_partitions)]

    # Parallel insertion using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=num_partitions) as executor:
        futures = [executor.submit(insert_partition, partition) for partition in partitions]
        
        # Wait for all futures to complete
        for future in as_completed(futures):
            future.result()

    logging.info("Data successfully inserted into PostgreSQL.")



def main():
    # Step 1: Clean the data
    cleaned_data = clean_data(hdfs_file_path)

    logging.info("Step 1 complete")

    #Performed if table was not created
    create_table_if_not_exists(db_config)

    logging.info("Step 2 complete")

    # Step 2: Insert cleaned data into PostgreSQL
    insert_spark_df_to_postgres(cleaned_data, "movie_ratings", db_config)

    logging.info("Step 3 complete")
    
if __name__ == "__main__":
    main()
