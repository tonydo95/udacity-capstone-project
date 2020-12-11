from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)


# Configurations
BUCKET_NAME = "input-data-project"

LOAD_US_STATES_STEP = [
    {
        "Name": "Load us states dimenson",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://{{ params.BUCKET_NAME }}/spark/load_us_states_dimension.py",
            ],
        },
    },
]

JOB_FLOW_OVERRIDES = {
    "Name": "Immigration ETL",
    "ReleaseLabel": "emr-5.30.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
                }
            ],
        },
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "Ec2KeyName" : "my_key",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "BootstrapActions" : [
        {
            "Name" : "AddBoto3",
            "ScriptBootstrapAction" : 
                {
                    "Path" : "s3://input-data-project/spark/bootstrap.sh"
                },
        },
    ],
    "LogUri" : "s3://output-data-project/log",
}


# Setting the default parameters of DAG
default_args = {
    'owner': 'tin do',
    'start_date': datetime(2020, 11, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('immigration_project_dag',
          default_args=default_args,
          description='Load and Transform immigration data',
          schedule_interval='@monthly',
          catchup=False
         )

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)


# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

# Add your steps to the EMR cluster
load_us_states_step = EmrAddStepsOperator(
    task_id="load_us_states_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=LOAD_US_STATES_STEP,
    params={ # these params are used to fill the paramterized values in SPARK_STEPS json
        "BUCKET_NAME": BUCKET_NAME,
    },
    dag=dag,
)

# wait for the steps to complete
us_states_step_checker = EmrStepSensor(
    task_id="us_states_step_checker",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='load_us_states_steps', key='return_value')[0] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

LOAD_TEMPERATURE_STEP = [
    {
        "Name": "Load Temperature dimension",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://{{ params.BUCKET_NAME }}/spark/load_temperature.py",
            ],
        },
    },
]

# Add your steps to the EMR cluster
load_temperature_step = EmrAddStepsOperator(
    task_id="load_temperature_step",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=LOAD_TEMPERATURE_STEP,
    params={
        "BUCKET_NAME": BUCKET_NAME,
    },
    dag=dag,
)

# wait for the steps to complete
temperature_step_checker = EmrStepSensor(
    task_id="temperature_step_checker",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='load_temperature_step', key='return_value')[0] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

LOAD_IMMIGRATION_STEP = [
    {
        "Name": "Load Immigration Fact",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://{{ params.BUCKET_NAME }}/spark/load_immigration_fact.py",
            ],
        },
    },
]

# Add your steps to the EMR cluster
load_immigration_fact = EmrAddStepsOperator(
    task_id="load_immigration_fact",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=LOAD_IMMIGRATION_STEP,
    params={
        "BUCKET_NAME": BUCKET_NAME,
    },
    dag=dag,
)

# wait for the steps to complete
immigration_step_checker = EmrStepSensor(
    task_id="immigration_step_checker",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='load_immigration_fact', key='return_value')[0] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

DATA_QUALITY_STEPS = [
    {
        "Name": "Dimension Data Quality Check",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://{{ params.BUCKET_NAME }}/spark/dimension_data_quality.py",
            ],
        },
    },
    {
        "Name": "Fact Data Quality Check",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://{{ params.BUCKET_NAME }}/spark/fact_data_quality_check.py",
            ],
        },
    },
]

# Add your steps to the EMR cluster
data_quality_check = EmrAddStepsOperator(
    task_id="data_quality_check",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=DATA_QUALITY_STEPS,
    params={ # these params are used to fill the paramterized values in SPARK_STEPS json
        "BUCKET_NAME": BUCKET_NAME,
    },
    dag=dag,
)

last_step = len(DATA_QUALITY_STEPS) - 1 # this value will let the sensor know the last step to watch
# wait for the steps to complete
data_quality_checker = EmrStepSensor(
    task_id="data_quality_step_checker",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='data_quality_check', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

start_data_pipeline >> create_emr_cluster
create_emr_cluster >> load_us_states_step >> us_states_step_checker >> load_immigration_fact
create_emr_cluster >> load_temperature_step >> temperature_step_checker >> load_immigration_fact
load_immigration_fact >> immigration_step_checker >> data_quality_check
data_quality_check >> data_quality_checker >> terminate_emr_cluster >> end_data_pipeline