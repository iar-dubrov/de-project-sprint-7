from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SCRIPTS = "/lessons/scripts"
EVENTS = "/user/master/data/geo/events"
GEO = "/user/s15105168/data/geo/geo.csv"
OUTPUT = "/user/s15105168/data/analytics"

# 0 = вся история, иначе не посчитать домашний город и "никогда не переписывались"
DEPTH = "0"

default_args = {
    "owner": "s15105168",
    "start_date": datetime(2022, 6, 21),
}

conf = {
    "spark.executor.memory": "2g",
    "spark.executor.cores": "2",
    "spark.dynamicAllocation.maxExecutors": "4",
    "spark.sql.shuffle.partitions": "50",
}

with DAG(
    "geo_marts",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    users_mart = SparkSubmitOperator(
        task_id="users_mart",
        name="users_mart",
        conn_id="yarn_spark",
        application=f"{SCRIPTS}/users_mart.py",
        py_files=f"{SCRIPTS}/utils.py",
        application_args=["{{ ds }}", DEPTH, EVENTS, GEO, f"{OUTPUT}/users_mart"],
        conf=conf,
    )

    zones_mart = SparkSubmitOperator(
        task_id="zones_mart",
        name="zones_mart",
        conn_id="yarn_spark",
        application=f"{SCRIPTS}/zones_mart.py",
        py_files=f"{SCRIPTS}/utils.py",
        application_args=["{{ ds }}", DEPTH, EVENTS, GEO, f"{OUTPUT}/zones_mart"],
        conf=conf,
    )

    friends_mart = SparkSubmitOperator(
        task_id="friends_mart",
        name="friends_mart",
        conn_id="yarn_spark",
        application=f"{SCRIPTS}/friends_mart.py",
        py_files=f"{SCRIPTS}/utils.py",
        application_args=["{{ ds }}", DEPTH, EVENTS, GEO, f"{OUTPUT}/friends_mart"],
        conf=conf,
    )

    # запускаем по очереди, чтобы не занимать весь кластер сразу
    users_mart >> zones_mart >> friends_mart
