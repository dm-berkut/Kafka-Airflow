from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/home/d.berkut/Kafka-Airflow"

SPARK_SUBMIT_CMD = f"""
cd {PROJECT_DIR} && \
/usr/local/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2,org.postgresql:postgresql:42.7.5 \
  scripts/consumer_spark.py
"""

with DAG(
    dag_id="kafka_consumer_berkut",
    start_date=datetime(2026, 6, 24),
    schedule="0 19 * * *",
    catchup=False,
    tags=["kafka", "consumer", "berkut"],
) as dag:

    run_consumer = BashOperator(
        task_id="run_consumer_spark",
        bash_command=SPARK_SUBMIT_CMD,
    )
