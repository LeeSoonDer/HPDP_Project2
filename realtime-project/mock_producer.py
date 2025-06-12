# File: mock_producer.py

import json
from kafka import KafkaProducer
import time
import random

# --- Preset Data ---
# A list of sample sentences to send to Kafka.
# Include a mix of positive, negative, and neutral sentiments.
PRESET_DATA = [
    "I love the food in Johor Bahru, it's absolutely amazing!",
    "The traffic today was terrible, I was stuck for over an hour.",
    "Just bought a new phone from Shopee, the delivery was very fast.",
    "This new policy is very confusing, I don't understand it.",
    "Planning a trip to Kuala Lumpur next month, very excited.",
    "My internet connection has been very unstable all day.",
    "The weather is quite nice this afternoon.",
    "I think the service at that cafe has really gone downhill.",
    "Watching the new local movie, it's pretty good so far.",
    "The price of groceries seems to be increasing every week."
]

# --- Kafka Configuration ---
# These must match your other scripts and docker-compose file.
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "twitter_topic"

# --- Initialize Kafka Producer ---
# This is the same setup as your twitter_producer.py
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"✅ Mock Kafka Producer initialized. Sending to topic '{KAFKA_TOPIC}' via {KAFKA_SERVER}")
print("Press Ctrl+C to stop.")

# --- Main Loop ---
# This loop will run forever, sending a new message every 5 seconds.
try:
    while True:
        # 1. Choose a random sentence from our preset data
        chosen_sentence = random.choice(PRESET_DATA)
        
        # 2. Create the message in the same format as the Twitter producer
        message = {'text': chosen_sentence}
        
        print(f"✉  Sending message: {message}")
        
        # 3. Send the message to the Kafka topic
        producer.send(KAFKA_TOPIC, value=message)
        
        # 4. Wait for a few seconds before sending the next one
        time.sleep(10)

except KeyboardInterrupt:
    print("\nStopping the mock producer.")
finally:
    # Close the producer connection
    producer.close()