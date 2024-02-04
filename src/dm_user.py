import findspark
findspark.init()
findspark.find()
import os
import sys
from datetime import datetime
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, DateType
from tools import get_geo, get_city, write_df_dm

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

def main():
    path_geo = sys.argv[1]
    path_events = sys.argv[2]
    spark = SparkSession.builder.master('yarn').appName('dm_user_zav').getOrCreate()
    # Получение событий с координатами
    df_message = spark.read.parquet(path_events) \
        .sample(0.03) \
        .where("event_type == 'message'") \
        .withColumn('user_id', F.col('event.message_from')) \
        .withColumn('event_id', F.monotonically_increasing_id())
    # Получение координат городов
    df_geo = get_geo(path_geo)
    # Привязка события к городу
    df_message_city = get_city(
        df_events=df_message,
        df_geo=df_geo
    )
    # Выбор активного города и локального времени
    window_act_city = Window().partitionBy('user_id').orderBy(F.col('date').desc())
    df_act_city = df_message_city \
        .withColumn('row_number', F.row_number().over(window_act_city)) \
        .filter(F.col('row_number') == 1) \
        .drop('row_number') \
        .withColumn('TIME', F.col('event.datetime').cast('Timestamp')) \
        .withColumn('local_time', F.from_utc_timestamp(F.col('TIME'), F.col('timezone'))) \
        .selectExpr('TIME', 'local_time', 'city as act_city', 'user_id')
    # Посещения городов
    window_travel = Window().partitionBy('user_id', 'id').orderBy(F.col('date'))
    df_travels = df_message_city \
        .withColumn('dense_rank', F.dense_rank().over(window_travel)) \
        .withColumn('date_diff',
                    F.datediff(F.col('date').cast(DateType()), F.to_date(F.col('dense_rank').cast('string'), 'd'))) \
        .selectExpr('date_diff', 'user_id', 'date', 'id', 'city') \
        .groupBy('user_id', 'date_diff', 'id', 'city') \
        .agg(F.countDistinct(F.col('date')).alias('cnt_days'))
    df_travels_array = df_travels.groupBy('user_id') \
        .agg(F.collect_list('city').alias('travel_array')) \
        .select('user_id', 'travel_array', F.size('travel_array').alias('travel_count'))
    # Выбор домашнего города
    df_home_city = df_travels.filter((F.col('cnt_days') > 27)) \
        .withColumn('max_dt', F.max(F.col('date_diff')).over(Window().partitionBy('user_id'))) \
        .where(F.col('date_diff') == F.col('max_dt')) \
        .selectExpr('user_id', 'city as home_city')
    # Расчет итогового датафрейма
    df_dm_users = df_act_city \
        .join(df_home_city, ['user_id'], 'left') \
        .join(df_travels_array, ['user_id'], 'left') \
        .selectExpr('user_id', 'act_city', 'home_city', 'travel_count', 'travel_array', 'local_time')
    # Сохранение в HDFS
    write_df_dm(df_dm_users, 'dm_users', datetime.now().date().strftime('%Y-%m-%d'))

if __name__ == '__main__':
    main()