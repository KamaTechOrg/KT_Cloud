from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
# from ELTS import X
from ELTS import AlbumTotalTimeDownloadsDailyELT
from ELTS import RevenuePerCustomerGenreDailyELT
# Define your Python functions here
def run_table_1():
    # Code to generate Table 1
    pass

def run_table_2():
    # Code to generate Table 2
    pass

def run_table_3():
    # Code to generate Table 3
    pass

# More functions for other tasks as necessary
def run_album_totals():
    AlbumTotalTimeDownloadsDailyELT.incremental_load()
    
def run_customer_genre_revenue():
    RevenuePerCustomerGenreDailyELT.incremental_load()
    
# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialize DAG
with DAG(
    'etl_orchestration',
    default_args=default_args,
    description='ETL Process Orchestration DAG',
    schedule_interval=None,  # Set to None for manual runs
    catchup=False,
) as dag:

    # Task 1 (Independent tasks that run first)
    task_1 = PythonOperator(
        task_id='run_table_1',
        python_callable=run_table_1,
    )
    
    task_3 = PythonOperator(
        task_id='run_table_3',
        python_callable=run_table_3,
    )
    
    # Add more independent tasks here
    task_5 = PythonOperator(
        task_id='run_table_5',
        python_callable=run_table_5,
    )
    
    task_album_totals = PythonOperator(
        task_id = 'task_album_totals_daily_elt',
        python_callable=run_album_totals
    )
    
    task_revenue_customer_genre = PythonOperator(
        task_id = 'task_revenue_customer_genre_daily_elt',
        python_callable=run_customer_genre_revenue
    )

    # Dependent tasks that run after Table 1, 3, 5
    task_2 = PythonOperator(
        task_id='run_table_2',
        python_callable=run_table_2,
    )

    # Define dependencies
    task_1 >> task_2
    task_3 >> task_4
    task_5 >> task_6

    # You can add more tasks and dependencies following this pattern.
