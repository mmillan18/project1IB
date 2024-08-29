from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'olist_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

def extract_data(**kwargs):
    from src.extract import extract
    from src.config import get_csv_to_table_mapping, DATASET_ROOT_PATH, PUBLIC_HOLIDAYS_URL
    
    csv_table_mapping = get_csv_to_table_mapping()
    csv_dataframes = extract(DATASET_ROOT_PATH, csv_table_mapping, PUBLIC_HOLIDAYS_URL)
    return csv_dataframes

def load_data(**kwargs):
    from src.load import load
    from sqlalchemy import create_engine
    
    engine = create_engine("sqlite:///olist.db")
    csv_dataframes = kwargs['ti'].xcom_pull(task_ids='extract_data')
    load(data_frames=csv_dataframes, database=engine)

def transform_data(**kwargs):
    from src.transform import (
        query_delivery_date_difference,
        query_freight_value_weight_relationship,
        query_orders_per_day_and_holidays_2017,
    )
    from sqlalchemy import create_engine
    
    engine = create_engine("sqlite:///olist.db")
    query_freight_value_weight_relationship(engine)
    query_orders_per_day_and_holidays_2017(engine)
    # Otros queries...

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

end_task = DummyOperator(
    task_id='end_task',
    trigger_rule='all_done',
    dag=dag
)

extract_task >> load_task >> transform_task >> end_task