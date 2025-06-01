import os
import json
from datetime import datetime, timezone
import time
import requests
import boto3
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Read sensitive credentials and config from environment
API_KEY = os.getenv("API_KEY")
SYMBOL = os.getenv("SYMBOL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC = "stock_prices"

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# MarketStack API URL
API_URL = f"http://api.marketstack.com/v2/eod/latest?access_key={API_KEY}&symbols={SYMBOL}"

# AWS S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)


def upload_to_s3(data, symbol):
    """Upload full API response to S3 bucket."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    file_name = f"{symbol}_{timestamp}.json"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"stock_data/{file_name}",
        Body=json.dumps(data),
        ContentType='application/json'
    )
    print(f"📦 Uploaded to S3: stock_data/{file_name}")


def fetch_and_send():
    """Fetch data from API, upload to S3, and send relevant fields to Kafka."""
    while True:
        try:
            response = requests.get(API_URL)
            if response.status_code == 200:
                result = response.json()

                # Store full API response to S3
                upload_to_s3(result, SYMBOL)

                # Send summary to Kafka
                if "data" in result and result["data"]:
                    stock_data = result["data"][0]
                    print(stock_data)
                    producer.send(TOPIC, value=stock_data)
                    producer.flush()
                    print(f"✅ Sent to Kafka: {stock_data}")
                else:
                    print("⚠️ No data found in API response.")
            else:
                print(f"❌ API error: {response.status_code}, {response.text}")
        except Exception as e:
            print(f"❌ Error: {e}")

        time.sleep(10)  # Wait 60 seconds before the next API call


# Start the process
fetch_and_send()
