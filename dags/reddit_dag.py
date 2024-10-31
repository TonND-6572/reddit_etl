from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

import os 
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.reddit_pipeline import reddit_pipeline
from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.aws_glue_pipeline import upload_glue_pipeline

def on_failure_callback(context):
    # Optional: Custom function to execute on failure
    print(f"Job failed: {context['task_instance'].task_id}")

default_args = {
    'owner': 'toan-wakuwa',
    'start_date': datetime(2024, 10, 24),
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args = default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline']
)

#extraction from reddit
extract = PythonOperator(
    task_id = 'reddit_extraction',
    python_callable = reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'dataengineering',
        'time_filter': 'day',
        'limit': 100
    },
    dag=dag
)

#upload to s3 storage
upload_s3 = PythonOperator(
    task_id='s3_upload',
    python_callable=upload_s3_pipeline,
    dag=dag
)

#load to glue data catalog
glue_etl_task = GlueJobOperator(
    task_id = 'run_glue_etl_job',
    job_name = 'aws_glue_visual_etl',
    script_location = 's3://aws-glue-assets-913524935371-ap-northeast-1/scripts/aws_glue_visual_etl.py',
    wait_for_completion=True,
    # Set the execution timeout to your desired maximum ETL job runtime
    execution_timeout=timedelta(minutes=15),
    on_failure_callback=on_failure_callback,
    dag=dag
)

glue_crawler_task = PythonOperator(
    task_id = 'run_crawler_job',
    python_callable = upload_glue_pipeline,
    op_kwargs={
        'crawler_name': 'reddit_glue_crawler_2',
        'client': 'glue'
    },
    dag=dag
)

extract >> upload_s3 >> glue_etl_task >> glue_crawler_task