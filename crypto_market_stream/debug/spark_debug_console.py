from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, min, max, stddev
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
RAW_TOPIC = "crypto_prices"
CHECKPOINT_LOCATION = "checkpoints/crypto_metrics_console"


spark = SparkSession.builder \
    .appName("CryptoKafkaDebugConsole") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    ) \
    .getOrCreate()


kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", RAW_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()


raw_df = kafka_df.select(
    col("value").cast("string").alias("json_string")
)


schema = StructType([
    StructField("coin", StringType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("market_cap", LongType(), True),
    StructField("volume", LongType(), True),
    StructField("change_24h", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])


parsed_df = raw_df.select(
    from_json(col("json_string"), schema).alias("data")
)

final_df = parsed_df.select("data.*")


windowed_metrics_df = final_df.groupBy(
    window(col("timestamp").cast("timestamp"), "10 minutes"),
    col("coin")
).agg(
    avg("price_usd").alias("avg_price"),
    min("price_usd").alias("min_price"),
    max("price_usd").alias("max_price"),
    stddev("price_usd").alias("volatility")
)


query = windowed_metrics_df.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", False) \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()


query.awaitTermination()
