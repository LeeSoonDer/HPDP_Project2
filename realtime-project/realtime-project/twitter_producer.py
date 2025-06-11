# File: twitter_producer.py (UPDATED for Free Tier)

import tweepy
import json
from kafka import KafkaProducer
import time
import os

# --- IMPORTANT: Use the Bearer Token from your Project's App ---
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAExQ2AEAAAAAdg1pCFe2uMaxj3b18EdUvFwd56k%3Dk9hUGPr96ZiujT893PkwH6o2isFhcB8ME3qJEprSYeudfXnMq5"

# --- Kafka Configuration ---
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "twitter_topic"

# --- Initialize Kafka Producer ---
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
print(f"✅ Kafka Producer initialized. Sending to topic '{KAFKA_TOPIC}' via {KAFKA_SERVER}")

# --- Initialize Tweepy v2 Client ---
# We use tweepy.Client for the recent search endpoint
client = tweepy.Client(BEARER_TOKEN)
print("✅ Tweepy Client initialized.")

# --- The search query ---
# This is the same as your old stream rule
QUERY = "Malaysia lang:en -is:retweet"

# This variable will store the ID of the newest tweet we've seen
since_id = None

print("🚀 Starting to search for recent tweets...")

# --- Main Loop ---
# This loop will run forever, polling Twitter every 15 minutes
while True:
    try:
        # Make the request to the recent search endpoint
        # since_id tells the API to only give us tweets newer than the last one we saw
        response = client.search_recent_tweets(query=QUERY, since_id=since_id)
        
        # Check if the response contains any tweets
        if response.data:
            new_tweets = response.data
            print(f"🔍 Found {len(new_tweets)} new tweet(s).")
            
            # The newest tweet is the first one in the response
            since_id = new_tweets[0].id
            
            # Send each new tweet to Kafka
            for tweet in new_tweets:
                message = {'text': tweet.text}
                print(f"  -> Sending to Kafka: {message}")
                producer.send(KAFKA_TOPIC, value=message)
        else:
            print("No new tweets found in this interval.")

    except Exception as e:
        print(f"An error occurred: {e}")

    # Wait for 15 minutes before searching again
    # Twitter's rate limit for this endpoint is 1 request every 15 minutes on the Free plan
    print("\n🕒 Waiting for 15 minutes before the next search...\n")
    time.sleep(15 * 60)