import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime
from airflow.models.baseoperator import chain

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

# Пути к данным в HDFS
path_geo = "/user/avzhuravle/geo_.csv"
path_events = "/user/master/data/geo/events"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
}

dag_spark = DAG(
    dag_id="datalake_etl",
    default_args=default_args,
    schedule_interval=None,
)

dm_user = SparkSubmitOperator(
    task_id='dm_user',
    dag=dag_spark,
    application='/home/avzhuravle/dm_user.py',
    conn_id='yarn_spark',
    application_args=[path_geo, path_events],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=2,
    executor_memory='2g'
)

dm_zone = SparkSubmitOperator(
    task_id='dm_zone',
    dag=dag_spark,
    application='/home/avzhuravle/dm_zone.py',
    conn_id='yarn_spark',
    application_args=[path_geo, path_events],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=2,
    executor_memory='2g'
)

dm_recom = SparkSubmitOperator(
    task_id='dm_recom',
    dag=dag_spark,
    application='/home/avzhuravle/dm_recom.py',
    conn_id='yarn_spark',
    application_args=[path_geo, path_events],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=2,
    executor_memory='2g'
)

dm_user >> dm_zone >> dm_recom