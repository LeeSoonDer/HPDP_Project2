# FINAL, WORKING Dockerfile - Using the Jupyter PySpark Notebook Image

# 1. Start from the official Jupyter image with Spark 3.5.1
# This is a very stable, widely-used image for PySpark.
FROM jupyter/pyspark-notebook:spark-3.5.1

# 2. This image runs as user 'jovyan'. We switch to root to perform installations.
USER root

# 3. Set the working directory
WORKDIR /home/jovyan/work

# 4. Copy your requirements file.
# The --chown flag ensures the standard 'jovyan' user owns the files.
COPY --chown=jovyan:users requirements.txt .

# 5. Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copy your model and script
COPY --chown=jovyan:users Logistic_Regression_20250607_010443 ./models/Logistic_Regression_20250607_010443
COPY --chown=jovyan:users stream_predictions.py .

# 7. Switch back to the standard, non-root 'jovyan' user for security
USER jovyan

# 8. The command to run the app.
# We now use --packages again, as this clean environment handles it correctly.
# The path to spark-submit is also different in this image.
CMD ["/usr/local/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,org.elasticsearch:elasticsearch-spark-30_2.13:8.5.3", \
     "stream_predictions.py"]