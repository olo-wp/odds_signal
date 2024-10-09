#def send_to_spark():
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, decode
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
import json

if __name__ == "__main__":
    s_conn = SparkSession.builder \
        .appName("SparkDataStreaming") \
        .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                       "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()

    s_conn.sparkContext.setLogLevel("ERROR")

    raw_data = s_conn.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'airflow-data-stream') \
    .option('startingOffsets', 'earliest') \
    .load()

    df_json = raw_data.selectExpr("CAST(value AS BINARY) as binary_value") \
        .select(decode(col("binary_value"), "UTF-8").alias("json_string"))
    '''

    # Zdefiniuj schemat dla danych JSON z użyciem FloatType dla kolumn numerycznych
    schema = StructType([
        StructField("game", StringType(), True),
        StructField("home", FloatType(), True),
        StructField("draw", FloatType(), True),
        StructField("away", FloatType(), True),
        StructField("game_date", TimestampType(), True)
    ])

    # Parsowanie danych JSON na kolumny z już odpowiednim typem FloatType
    df_parsed = df_json.withColumn("data", from_json(col("json_string"), schema)) \
        .select("data.*")

    # Pokaż schemat danych
    df_parsed.printSchema()
    print(df_parsed.isStreaming)
    '''
    df_json \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start() \
        .awaitTermination()


