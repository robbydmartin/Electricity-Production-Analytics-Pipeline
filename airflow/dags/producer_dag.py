from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
        "owner" : "electricity_production_team",
        "start_date" : datetime(2026,3,12),
        "retries" : 3,
        "retry_delay": timedelta(minutes=1),
    }

def run_electricity_production_producer():
    pass

with DAG(
    dag_id = "electricity_production_pipeline_producer",
    description = "A pipeline for streaming electricity production events",
    schedule="@hourly",
    catchup = False,
    default_args = default_args,
    tags = ['electricity production', 'team project']

) as dag:

    start = EmptyOperator(task_id = "start")
    end = EmptyOperator(task_id = "end")

    run_electricity_production_producer_task = PythonOperator(
        task_id = "start_producer"
        python_callable = 
    )

    start >> run_electricity_production_producer_task >> end