from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from src.extract import extract
from src.load import load
from src.transform import (
    query_delivery_date_difference,
    query_freight_value_weight_relationship,
    query_orders_per_day_and_holidays_2017,
    # Otros queries
)
from src.config import get_csv_to_table_mapping, DATASET_ROOT_PATH, PUBLIC_HOLIDAYS_URL
from sqlalchemy import create_engine

def run_extract(**kwargs):
    csv_table_mapping = get_csv_to_table_mapping()
    csv_dataframes = extract(DATASET_ROOT_PATH, csv_table_mapping, PUBLIC_Holidays_URL)
    return csv_dataframes

def run_load(**kwargs):
    engine = create_engine("sqlite:///olist.db")
    csv_dataframes = kwargs['ti'].xcom_pull(task_ids='extract')
    load(data_frames=csv_dataframes, database=engine)

def run_transform(**kwargs):
    engine = create_engine("sqlite:///olist.db")
    # Aquí puedes ejecutar diferentes queries y guardarlos
    query_freight_value_weight_relationship(engine)
    query_orders_per_day_and_holidays_2017(engine)
    # Otros queries...

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'elt_dag',
    default_args=default_args,
    description='A simple ELT DAG',
    schedule_interval='@daily',
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=run_extract,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=run_load,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=run_transform,
    provide_context=True,
    dag=dag,
)

extract_task >> load_task >> transform_task
