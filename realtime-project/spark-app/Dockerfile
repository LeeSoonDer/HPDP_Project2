# File: spark-app/Dockerfile (CRITICAL FIX FOR SCALA INCOMPATIBILITY)

# 1. Start from the latest official Jupyter image with PySpark.
FROM jupyter/pyspark-notebook:latest

# 2. This image runs as user 'jovyan'. Switch to root for installations.
USER root

# 3. Set the working directory
WORKDIR /home/jovyan/work

# 4. Copy your requirements file. The --chown flag gives the correct permissions.
COPY --chown=jovyan:users requirements.txt .

# 5. Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copy your model and script
COPY --chown=jovyan:users Logistic_Regression_20250607_010443 ./models/Logistic_Regression_20250607_010443
COPY --chown=jovyan:users stream_predictions.py .

# 7. Switch back to the standard, non-root user
USER jovyan

# 8. The command to run the app.
# IMPORTANT FIX: Changed Kafka package to _2.12 AND added Elasticsearch package with _2.12 suffix.
CMD ["/usr/local/spark/bin/spark-submit", \
      "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.5.3", \
      "stream_predictions.py"]