from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import csv

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_jsonplaceholder_pipeline',
    default_args=default_args,
    description='An ETL DAG for JSONPlaceholder Data',
    schedule_interval=timedelta(days=1),
)

def extract(**kwargs):
    """Extracts dummy posts data from JSONPlaceholder"""
    url = 'https://jsonplaceholder.typicode.com/posts'
    response = requests.get(url)
    data = response.json()
    return data

def transform(**kwargs):
    """Extracts relevant fields from the dummy posts data"""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract')
    if data:
        transformed_data = [{'userId': post['userId'], 'title': post['title'], 'body': post['body']} for post in data]
        return transformed_data
    return None

def load(**kwargs):
    """Loads the transformed data into a CSV file"""
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform')
    if transformed_data:
        with open('/opt/airflow/dags/jsonplaceholder_data.csv', mode='w') as file:
            writer = csv.DictWriter(file, fieldnames=['userId', 'title', 'body'])
            writer.writeheader()
            writer.writerows(transformed_data)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task
