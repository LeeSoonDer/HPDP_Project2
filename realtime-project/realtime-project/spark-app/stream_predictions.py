from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, regexp_replace, trim
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import PipelineModel

# --------------------------------------------
# 1. Initialize SparkSession for Streaming
# --------------------------------------------
# Make sure to include the Kafka and Elasticsearch packages
spark = SparkSession.builder \
    .appName("RealtimeSentimentAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,org.elasticsearch:elasticsearch-spark-30_2.13:8.11.4") \
    .getOrCreate()

# Set log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

print("✅ Spark session for streaming created successfully.")


# --------------------------------------------
# 2. Load the Pre-trained Pipeline Model
# --------------------------------------------
# This path is INSIDE the Docker container because of the COPY command in the Dockerfile
model_path = "./models/Logistic_Regression_20250607_010443"
loaded_model = PipelineModel.load(model_path)
print(f"✅ Pre-trained pipeline model loaded from {model_path}")


# --------------------------------------------
# 3. Read from Kafka Stream
# --------------------------------------------
KAFKA_TOPIC = "twitter_topic" # Or whatever your friend named the topic
KAFKA_SERVER = "kafka:9092"   # The Kafka service name from docker-compose.yml

print(f"Subscribing to Kafka topic: {KAFKA_TOPIC}")

# Read the raw data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Define the schema for the incoming JSON data from Kafka
# Assuming your producer sends JSON like: {"text": "some tweet text"}
schema = StructType([
    StructField("text", StringType(), True)
])

# Deserialize the JSON data from the 'value' column of Kafka
kafka_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))
streaming_df = kafka_df.select("data.*")


# --------------------------------------------
# 4. Preprocess and Predict on Streaming Data
# --------------------------------------------
print("🚀 Applying cleaning and prediction pipeline to the stream...")

# Step 4a: Apply the EXACT SAME initial cleaning as your batch script
cleaned_stream_df = (
    streaming_df.withColumn("text", lower(col("text")))
      .withColumn("text", regexp_replace(col("text"), r"http\S+|@\S+|#\S+", ""))
      .withColumn("text", regexp_replace(col("text"), r"[^a-zA-Z\s]", ""))
      .withColumn("text", trim(col("text")))
)

# Step 4b: Use the loaded model to apply the rest of the pipeline
# The model will handle tokenization, stop words, vectorization, and prediction
predictions_df = loaded_model.transform(cleaned_stream_df)

# Let's select the columns we want to save
# We can map the numeric prediction back to a sentiment label
final_df = predictions_df.select(
    col("text"),
    col("prediction").alias("sentiment_id") # e.g., 0.0 or 1.0
)


# --------------------------------------------
# 5. Write Results to Elasticsearch
# --------------------------------------------
ES_INDEX = "sentiment_stream_results" # Name for the Elasticsearch index
ES_SERVER = "es01:9000" # NOTE: This is how Spark connects to Elasticsearch

print(f"Writing streaming results to Elasticsearch index: {ES_INDEX}")

query = final_df.writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", ES_INDEX) \
    .option("es.nodes", "es01") \
    .option("es.port", "9200") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

# Wait for the streaming query to finish
query.awaitTermination()