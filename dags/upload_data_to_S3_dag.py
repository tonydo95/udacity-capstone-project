import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (UploadToS3Operator)
from datetime import datetime, timedelta

# Configurations
BUCKET_NAME = "input-data-project"

default_args = {
    'owner' : 'tindo',
    'start_date' : datetime(2020, 11, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False   
}

dag = DAG('upload_file_to_s3_dag',
            default_args=default_args,
            description='Upload data files to s3',
            schedule_interval='@monthly',
            catchup=False
)

folder_list = os.listdir('./data/')
dir_list = filter(
    lambda x: os.path.isdir(os.path.join('./data/', x)), folder_list)

start_upload_files = DummyOperator(task_id='start_upload_files', dag=dag)

upload_data_to_s3 = UploadToS3Operator(
    task_id='upload_data_to_s3',
    dag=dag,
    s3_bucket_name=BUCKET_NAME,
    folder_names=dir_list,
    input_path='./data/',
)

upload_scripts_to_s3 = UploadToS3Operator(
    task_id='upload_scrits_s3',
    dag=dag,
    s3_bucket_name=BUCKET_NAME,
    folder_names=['spark'],
    input_path='./dags/scripts/'
)

end_upload_files = DummyOperator(task_id='start_upload', dag=dag)

start_upload_files >> upload_data_to_s3 >> upload_scripts_to_s3 >> end_upload_files