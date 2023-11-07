from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from user import user_api

default_args={
    'owner':'vinay',
    'start_date':datetime(2023,11,2),
    'schedule':"@daily",
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

user_dag=DAG(
    dag_id='user_dag',
    default_args=default_args,
    description='Etl Pipeline  api --> s3 --> crawler --> glue Catalog --> Athena'    
)

api_to_s3=PythonOperator(
    task_id='run_user_etl',
    python_callable=user_api,
    dag=user_dag
)
s3_to_glue_catalog = GlueJobOperator(
    task_id='glue_etl_job',
    job_name='s3_to_glue_catalog', 
    dag=user_dag
)

api_to_s3 >> s3_to_glue_catalog
