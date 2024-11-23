from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime

SCRIPT_PATH ="/home/marius/airflow/dags/movies/cleaner.py"

with DAG("movie-trial-dag", start_date=datetime(2024,1,1),
         schedule_interval="@daily", catchup=False) as dag:
    
    perform_cleaning = BashOperator(
        task_id='perform_cleaning',
        bash_command=f"python {SCRIPT_PATH}"  # Use Python to execute the script
    )
    
    perform_cleaning