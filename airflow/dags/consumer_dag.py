from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
        "owner" : "electricity_production_team",
        "start_date" : datetime(2026,3,12),
        "retries" : 3,
        "retry_delay": timedelta(minutes=1),
    }

with DAG(
    dag_id = "electricity_production_pipeline_consumer",
    description = "A pipeline for streaming electricity production events",
    schedule="@hourly",
    catchup = False,
    default_args = default_args,
    tags = ['electricity production', 'team project']
) as dag:
    
    start = EmptyOperator(task_id = "start")
    end = EmptyOperator(task_id = "end")

    run_consumer = BashOperator(
        task_id = "start_spark_consumer",
        bash_command= """
            echo "Starting Spark consumer..."

            spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            /opt/airflow/spark_jobs/electricity_production_consumer.py \
        """
    )

    run_rdd_elt_task = BashOperator(
        task_id= "rdd_etl",
        bash_command= """
            spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/airflow/spark_jobs/batch_rdd_etl.py
        """
    )

start >> run_consumer >> end