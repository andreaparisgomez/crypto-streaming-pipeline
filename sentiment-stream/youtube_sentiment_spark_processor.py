import os
import math
from langdetect import detect, LangDetectException
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    udf,
    to_json,
    struct,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

INPUT_TOPIC = "youtube_raw_comments"
OUTPUT_TOPIC = "youtube_sentiment_metrics"


# -----------------------------
# Sentiment helpers
# -----------------------------

def vader_sentiment(text):

    if not text:
        return 0.0

    score = analyzer.polarity_scores(text)

    return score["compound"]

def sentiment_label(score):
    if score >= 0.05:
        return "positive"
    elif score <= -0.05:
        return "negative"
    else:
        return "neutral"


def engagement_score(like_count):
    like_count = like_count or 0
    return math.log1p(like_count)


sentiment_score_udf = udf(vader_sentiment, DoubleType())
sentiment_label_udf = udf(sentiment_label, StringType())
engagement_score_udf = udf(engagement_score, DoubleType())

def detect_language(text):
    try:
        if not text or len(text.strip()) < 10:
            return "unknown"
        return detect(text)
    except LangDetectException:
        return "unknown"


language_udf = udf(detect_language, StringType())

# -----------------------------
# Spark session
# -----------------------------

spark = (
    SparkSession.builder
    .appName("YoutubeSentimentProcessor")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# -----------------------------
# Input schema
# -----------------------------

youtube_schema = StructType([
    StructField("platform", StringType(), True),
    StructField("content_type", StringType(), True),
    StructField("video_id", StringType(), True),
    StructField("video_title", StringType(), True),
    StructField("channel_title", StringType(), True),
    StructField("comment_id", StringType(), True),
    StructField("author", StringType(), True),
    StructField("comment_text", StringType(), True),
    StructField("like_count", IntegerType(), True),
    StructField("published_at", StringType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("source_query", StringType(), True),
])


# -----------------------------
# Read from Kafka
# -----------------------------

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_value")
    .select(from_json(col("json_value"), youtube_schema).alias("data"))
    .select("data.*")
)


# -----------------------------
# Transform sentiment data
# -----------------------------

processed_stream = (
    parsed_stream
    .withColumn("language", language_udf(col("comment_text")))
    .filter(col("language") == "en")
    .withColumn("sentiment_score", sentiment_score_udf(col("comment_text")))
    .withColumn("sentiment_label", sentiment_label_udf(col("sentiment_score")))
    .withColumn("engagement_score", engagement_score_udf(col("like_count")))
    .withColumn(
        "weighted_sentiment_score",
        col("sentiment_score") * col("engagement_score")
    )
    .withColumn("processed_at", current_timestamp())
)

# -----------------------------
# Write back to Kafka
# -----------------------------

kafka_output = (
    processed_stream
    .select(
        col("comment_id").cast("string").alias("key"),
        to_json(struct("*")).alias("value")
    )
)

query = (
    kafka_output.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("topic", OUTPUT_TOPIC)
    .option("checkpointLocation", "./checkpoints/youtube_sentiment_processor")
    .outputMode("append")
    .start()
)

query.awaitTermination()
