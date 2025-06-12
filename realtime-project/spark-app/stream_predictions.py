# stream_predictions.py (FINAL PYTHON-NATIVE VERSION)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, lower, regexp_replace, trim
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import PipelineModel
from elasticsearch import Elasticsearch, helpers

# --- This function sends a batch of data to Elasticsearch ---
def send_to_es(df: DataFrame, epoch_id: int):
    # Check if the DataFrame is empty. If so, do nothing.
    if df.count() == 0:
        return

    print(f"--- Batch {epoch_id} ---")
    
    # Convert the Spark DataFrame to a list of dictionaries
    # to_dict('records') creates a list like [{'text': '...', 'sentiment_id': 1.0}, ...]
    records = df.toPandas().to_dict('records')
    
    # The index to write to
    es_index = "sentiment_stream_results"
    
    actions = [
        {
            "_index": es_index,
            "_source": record
        }
        for record in records
    ]

    try:
        # Initialize the client *inside* the function for Spark compatibility
        es = Elasticsearch(hosts=["es01"], port=9200, scheme="http")
        
        # Use the bulk helper to efficiently insert the data
        helpers.bulk(es, actions)
        print(f"✅ Successfully sent {len(records)} records to Elasticsearch index '{es_index}'.")

    except Exception as e:
        print(f"❌ Error sending data to Elasticsearch: {e}")


# --------------------------------------------
# 1. Initialize SparkSession
# --------------------------------------------
spark = SparkSession.builder.appName("RealtimeSentimentAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("✅ Spark session created successfully.")

# --------------------------------------------
# 2. Load the Pre-trained Pipeline Model
# --------------------------------------------
model_path = "./models/Logistic_Regression_20250607_010443"
loaded_model = PipelineModel.load(model_path)
print(f"✅ Pre-trained pipeline model loaded from {model_path}")

# --------------------------------------------
# 3. Read from Kafka Stream
# --------------------------------------------
KAFKA_TOPIC = "twitter_topic"
KAFKA_SERVER = "kafka:9092"
print(f"Subscribing to Kafka topic: {KAFKA_TOPIC}")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

schema = StructType([StructField("text", StringType(), True)])
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# --------------------------------------------
# 4. Preprocess and Predict
# --------------------------------------------
print("🚀 Applying cleaning and prediction pipeline...")
cleaned_df = parsed_df.withColumn("text", lower(col("text"))) \
    .withColumn("text", regexp_replace(col("text"), r"http\S+|@\S+|#\S+", "")) \
    .withColumn("text", regexp_replace(col("text"), r"[^a-zA-Z\s]", "")) \
    .withColumn("text", trim(col("text")))

predictions_df = loaded_model.transform(cleaned_df)
final_df = predictions_df.select(col("text"), col("prediction").alias("sentiment_id"))

# --------------------------------------------
# 5. Write to Elasticsearch using the Python function
# --------------------------------------------
query = final_df.writeStream \
    .outputMode("append") \
    .foreachBatch(send_to_es) \
    .start()

print("✅ Streaming query started. Waiting for data from Kafka...")
query.awaitTermination()