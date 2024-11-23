from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
import os
import logging
import psycopg2


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

# Read the MovieLens data from HDFS into a DataFrame
hdfs_file_path = "hdfs://localhost:9000//movies/movies-rating.csv"


def clean_data(hdfs_file_path):
    try:
        movie_df = spark.read.csv(hdfs_file_path, header=True, inferSchema=True)
    except Exception:
        logging.exception(Exception)
        raise Exception
    finally:
        logging.info("reading ended here")
        
    # Convert the 'rating' column from float to int
    movie_df = movie_df.withColumn('rating', col('rating').cast('int'))

    # Check for missing values
    movie_df.describe().show()

    # Drop rows with missing values (if necessary)
    movie_df_clean = movie_df.dropna()

    # Remove duplicates
    movie_df_clean = movie_df_clean.dropDuplicates()

    # Show cleaned data
    x = movie_df_clean.show(5)

    logging.info(x)
    
    return movie_df_clean
    
    
def move_all_to_postgress(cleaned_df):
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="",
        user="",
        password="",
        host="localhost",
        port="5432"
    )
    
    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Define the SQL query to create the table (if it doesn't exist)
    create_table_query = """
    CREATE TABLE movie_ratings (
        id SERIAL PRIMARY KEY,
        movie_id INT NOT NULL,
        title VARCHAR(255) NOT NULL,
        genres VARCHAR(255) NOT NULL,
        user_id INT NOT NULL,
        rating NUMERIC(2, 1) NOT NULL,
        timestamp BIGINT NOT NULL
    );

    """
    
    # Execute the create table query
    cur.execute(create_table_query)
    logging.info("Table Creation check was executed")

    # Commit the transaction
    conn.commit()
    logging.info("Create table commited")
    
    
    




clean_data(hdfs_file_path)

