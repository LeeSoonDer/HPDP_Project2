# spark-app/Dockerfile (FINAL, STABLE CONFIGURATION)

# 1. Start from the Bitnami Spark image
FROM bitnami/spark:3.5
WORKDIR /app

# 2. Copy and install Python dependencies from your requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy the Spark configuration file into the container.
# This is a more reliable way to provide the packages.
COPY spark-defaults.conf /opt/bitnami/spark/conf/

# 4. Copy your model folder and application script
COPY Logistic_Regression_20250607_010443 ./models/Logistic_Regression_20250607_010443
COPY stream_predictions.py .

# 5. A very simple command. All config is now in the .conf file.
CMD [ "spark-submit", "stream_predictions.py" ]