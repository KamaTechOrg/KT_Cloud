from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from DB.ELTS.TrackPlayCountandRevenueContributionDailyELT import incremental_load as incrementel_load_tk_1
from DB.ELTS.BestSellingAlbumsandTrackPopularitybyCountryDailyELT import incremental_load as incrementel_load_tk_2
from DB.ELTS.EmployeeCustomerSatisfactionAndAverageSalesValueDailyELT import load as incrementel_load_employee_customer_satisfaction_sales
from DB.ELTS.RepeatCustomerAnalysisByArtistAndPurchaseFrequencyDailyELT import load as incrementel_load_artist_repeat_customer_analysis
from DB.ELTS.CustomersInvoicesAvgDailyELT import load_average_purchase_value_elt_increment
# import DB.ELTS.TrackPlayCountandRevenueContributionDailyELT
# from ELTS import X
from ..ETLS.AlbumTotalTimeDownloadsDaily import incremental_load
from ..ELTS.RevenuePerCustomerGenreDailyELT import incremental_load as incremental_load_revenue
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

def  load_track_play_count():
    incrementel_load_tk_1()

def  load_best_selling_albums():
    incrementel_load_tk_2()

def load_customers_Invoices_average_of_month():
    load_average_purchase_value_elt_increment()

def  load_employee_customer_satisfaction_sales():
    incrementel_load_employee_customer_satisfaction_sales()

def  load_artist_repeat_customer_analysis():
    incrementel_load_artist_repeat_customer_analysis()
    
def run_album_popularity_and_revenue():
    # AlbumPopularityDailyELT.album_popularity_incremental_elt()
    print("1 hello")

def run_genres_popularity():
    # PopularGenresDailyELT.popular_genres_by_city_incremental_elt()
    print("2 hello")
    

# More functions for other tasks as necessary
def run_album_totals():
    incremental_load()
    
def run_customer_genre_revenue():
    incremental_load_revenue()
    
# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 11),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}
# 
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
    track_play_count = PythonOperator(
        task_id='load_track_play_count',
        python_callable=load_track_play_count,
    )
    
    task_album_totals = PythonOperator(
        task_id = 'task_album_totals_daily_elt',
        python_callable=run_album_totals
    )
    
    task_revenue_customer_genre = PythonOperator(
        task_id = 'task_revenue_customer_genre_daily_elt',
        python_callable=run_customer_genre_revenue
    )

    best_selling_albums = PythonOperator(
        task_id='load_best_selling_albums',
        python_callable=load_best_selling_albums,
    )

    employee_customer_satisfaction_sales = PythonOperator(
        task_id='employee_customer_satisfaction_sales',
        python_callable=load_employee_customer_satisfaction_sales,
    )

    artist_repeat_customer_analysis = PythonOperator(
        task_id='artist_repeat_customer_analysis',
        python_callable=load_artist_repeat_customer_analysis,
    )

    customers_Invoices_average_of_month = PythonOperator(
        task_id='customers_Invoices_average_of_month',
        python_callable=load_customers_Invoices_average_of_month,
    )

    # Dependent tasks that run after Table 1, 3, 5
    task_2 = PythonOperator(
        task_id='run_table_2',
        python_callable=run_table_2,
    )
    
    task_album_popularity_and_revenue = PythonOperator(
        task_id = 'run_album_popularity_and_revenue',
        python_callable = run_album_popularity_and_revenue
    )
    
    task_genres_popularity = PythonOperator(
        task_id = 'run_genres_popularity',
        python_callable = run_genres_popularity
    )

    # Define dependencies
    # task_1 >> task_2
    # task_3 >> task_4
    # task_5 >> task_6

    # You can add more tasks and dependencies following this pattern.
