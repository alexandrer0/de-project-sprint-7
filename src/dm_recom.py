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
from tools import get_geo, get_city, write_df_dm, dist

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
    df_events = spark.read.parquet(path_events).sample(0.03)
    # Получение координат городов
    df_geo = get_geo(path_geo)
    # Подписки
    df_s_0 = df_events.where("event_type == 'subscription'") \
        .selectExpr('event.datetime', 'event.user as user_id', 'event.subscription_channel', 'lat', 'lon')
    df_s = get_city(
        df_events=df_s_0,
        df_geo=df_geo
    )
    # Сообщения
    df_m_0 = df_events.where("event_type == 'message'") \
        .selectExpr('event.datetime', 'event.message_from', 'event.message_to', 'event.message_from as user_id', 'lat', 'lon')
    df_m = get_city(
        df_events=df_m_0,
        df_geo=df_geo
    )
    # Подписаны на один канал
    df_ss = df_s \
        .selectExpr('user_id as user_left', 'event.subscription_channel as subsc_ch').distinct() \
        .join(df_s.selectExpr('event.user as user_right', 'event.subscription_channel as subsc_ch').distinct(), \
            'subsc_ch', 'full') \
        .where(F.col('user_left') < F.col('user_right')) \
        .selectExpr('user_left', 'user_right').distinct()
    # Переписывались
    df_mm = df_m.selectExpr('event.message_from as user_left', 'event.message_to as user_right') \
        .filter("user_right is not null").distinct()
    # Подписаны на один канал, но не переписывались
    df_ss_mm = df_ss.exceptAll(df_mm)
    # Актуальные координаты юзеров
    window_act = Window().partitionBy('user_id').orderBy(F.col('date').desc())
    df_l = df_m \
        .withColumn('row_number', F.row_number().over(window_act)) \
        .filter(F.col('row_number') == 1) \
        .drop('row_number') \
        .withColumn('TIME', F.col('event.datetime').cast('Timestamp')) \
        .withColumn('local_time', F.from_utc_timestamp(F.col('TIME'), F.col('timezone'))) \
        .selectExpr('user_id', 'lat', 'lon', 'id as zone_id', 'local_time').distinct()
    # Расчет итогового датафрейма
    df_dm_recom = df_ss_mm \
        .join(df_l.selectExpr('zone_id', 'local_time', 'user_id as user_left', 'lat as lat_left', 'lon as lon_left'),
              ['user_left'], 'left') \
        .join(df_l.selectExpr('user_id as user_right', 'lat as lat_right', 'lon as lon_right'),
              ['user_right'], 'left') \
        .withColumn('dist', dist) \
        .filter(F.col('dist') < 1) \
        .withColumn('processed_dttm', F.lit(datetime.now().date())) \
        .selectExpr('user_left', 'user_right', 'processed_dttm', 'id as zone_id', 'local_time')

    # Сохранение в HDFS
    write_df_dm(df_dm_recom, 'dm_recom', datetime.now().date().strftime('%Y-%m-%d'))

if __name__ == '__main__':
    main()