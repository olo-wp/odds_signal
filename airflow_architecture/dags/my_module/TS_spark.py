#def send_to_spark():
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, decode, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, DoubleType
from pyspark.sql.functions import window, avg, col, round

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

    df_json = raw_data.selectExpr(
        "CAST(value AS BINARY) as binary_value",
        "timestamp"
    ).select(
        decode(col("binary_value"), "UTF-8").alias("json_string"),
        col("timestamp").alias("message_timestamp")
    )

    schema = (StructType([
        StructField("game", StringType(), True),
        StructField("home", DoubleType(), True),
        StructField("draw", DoubleType(), True),
        StructField("away", DoubleType(), True),
        StructField("date", DoubleType(), True),
    ]))

    df_parsed = df_json.select(
        from_json(col("json_string"), schema).alias("data"),
        col("message_timestamp")
    ).select("data.*", "message_timestamp")

    df_parsed = df_parsed.withColumn(
        "game_date",
        from_unixtime(col("date")).cast("timestamp")
    )
    df_parsed = df_parsed.drop("date")

    df_parsed.printSchema()
    print(df_parsed.isStreaming)

    """
    df_parsed \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start() \
        .awaitTermination()
    """

    df_avg = df_parsed.groupBy(
        col("game"),
        window(col("message_timestamp"), "10 minutes")
    ) \
        .agg(
             round(avg("home"),2).alias("avg_home"),
             round(avg("draw"),2).alias("avg_draw"),
             round(avg("away"),2).alias("avg_away")
    )

    df_avg.writeStream \
        .outputMode("update") \
        .format("console") \
        .start() \
        .awaitTermination()
