import findspark
findspark.init()
findspark.find()
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

EARTH_R = 6371

def get_geo(path_geo) -> pyspark.sql.DataFrame:
    df_geo = spark.read.csv(path_geo, sep=';', inferSchema=True, header=True) \
        .withColumn('lat', F.regexp_replace('lat', ',', '.')) \
        .withColumn('lat', F.col('lat').cast(FloatType())) \
        .withColumn('lng', F.regexp_replace('lng', ',', '.')) \
        .withColumn('lng', F.col('lng').cast(FloatType())) \
        .withColumnRenamed('lat', 'city_lat') \
        .withColumnRenamed('lng', 'city_lon')
    return df_geo

def get_city(df_events, df_geo) -> pyspark.sql.DataFrame:

    city_dist = 2 * F.lit(EARTH_R) * F.asin(
        F.sqrt(
            F.pow(F.sin((F.radians(F.col("lat")) - F.radians(F.col("city_lat"))) / 2), 2) +
            F.cos(F.radians(F.col("lat"))) * F.cos(F.radians(F.col("city_lat"))) *
            F.pow(F.sin((F.radians(F.col("lon")) - F.radians(F.col("city_lon"))) / 2), 2)
        )
    )
    window = Window().partitionBy('user_id').orderBy(F.col('dist').asc())
    df_events_city = df_events \
        .crossJoin(df_geo) \
        .withColumn('dist', city_dist)\
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col('row_number')==1) \
        .drop('row_number') \
        .persist()

    return df_events_city

# Расчет расстояния между двумя точками по координатам
dist = 2 * F.lit(EARTH_R) * F.asin(
    F.sqrt(
        F.pow(F.sin((F.radians(F.col('lat_left')) - F.radians(F.col('lat_right'))) / 2), 2) +
        F.cos(F.radians(F.col('lat_left'))) * F.cos(F.radians(F.col('lat_right'))) *
        F.pow(F.sin((F.radians(F.col('lon_left')) - F.radians(F.col('lon_right'))) / 2), 2)
    )
)

def write_df_dm(df: pyspark.sql.DataFrame, dm_name: str, date: str) -> None:
    """Записывает dataframe в дирректорию витрины

    Args:
        df (pyspark.sql.DataFrame): Датафрейм для записи
        dm_name (str): название директории витрины
        date (str): дата
    """
    df.write.mode('overwrite').parquet(f'/user/avzhuravle/analytics/{dm_name}/date={date}')