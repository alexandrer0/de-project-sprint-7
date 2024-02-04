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
    df_events = spark.read.parquet(path_events).sample(0.03)
    # Получение координат городов
    df_geo = get_geo(path_geo)
    # Сообщения
    df_m_0 = df_events.where("event_type == 'message'") \
        .selectExpr('event.datetime', 'event.message_from as user_id', 'lat', 'lon')
    df_m = get_city(
        df_events=df_m_0,
        df_geo=df_geo
    ).withColumn('month', F.trunc(F.col('datetime'), 'month')). \
        withColumn('week', F.trunc(F.col('datetime'), 'week'))
    # Регистрации (по первым сообщениям)
    window_u = Window().partitionBy('user_id').orderBy(F.col('datetime'))
    df_u = df_m \
        .withColumn('row_number', F.row_number().over(window_u)) \
        .filter(F.col('row_number') == 1) \
        .drop('row_number')
    # Реакции
    df_r_0 = df_events.where("event_type == 'reaction'") \
        .selectExpr('event.datetime', 'event.reaction_from as user_id', 'event.reaction_type', 'lat', 'lon')
    df_r = get_city(
        df_events=df_r_0,
        df_geo=df_geo
    ).withColumn('month', F.trunc(F.col('datetime'), 'month')). \
        withColumn('week', F.trunc(F.col('datetime'), 'week'))
    # Подписки
    df_s_0 = df_events.where("event_type == 'subscription'") \
        .selectExpr('event.datetime', 'event.user as user_id', 'event.subscription_channel', 'lat', 'lon')
    df_s = get_city(
        df_events=df_s_0,
        df_geo=df_geo
    ).withColumn('month', F.trunc(F.col('datetime'), 'month')). \
        withColumn('week', F.trunc(F.col('datetime'), 'week'))
    # Расчет итогового датафрейма
    df_dm_zone = df_m.groupBy('id', 'month', 'week').agg(F.count('id').alias('week_message')) \
        .join(df_r.groupBy('id', 'month', 'week').agg(F.count('id').alias('week_reaction')), ['id', 'month', 'week'],
              'full') \
        .join(df_s.groupBy('id', 'month', 'week').agg(F.count('id').alias('week_subscription')),
              ['id', 'month', 'week'], 'full') \
        .join(df_u.groupBy('id', 'month', 'week').agg(F.count('id').alias('week_user')),
              ['id', 'month', 'week'], 'full') \
        .join(df_m.groupBy('id', 'month').agg(F.count('id').alias('month_message')), ['id', 'month'], 'full') \
        .join(df_r.groupBy('id', 'month').agg(F.count('id').alias('month_reaction')), ['id', 'month'], 'full') \
        .join(df_s.groupBy('id', 'month').agg(F.count('id').alias('month_subscription')), ['id', 'month'], 'full') \
        .join(df_u.groupBy('id', 'month').agg(F.count('id').alias('month_user')), ['id', 'month'], 'full') \
        .withColumnRenamed('id', 'zone_id') \
        .fillna(0, subset=['week_message', 'week_reaction', 'week_subscription', 'week_user',
                           'month_message', 'month_reaction', 'month_subscription', 'month_user'])
    # Сохранение в HDFS
    write_df_dm(df_dm_zone, 'dm_zone', datetime.now().date().strftime('%Y-%m-%d'))

if __name__ == '__main__':
    main()