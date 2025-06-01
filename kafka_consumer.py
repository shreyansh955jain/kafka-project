import os
import json
from datetime import datetime, timezone
import boto3
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Read environment configurations
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "44.201.82.63:9092")
TOPIC = "stock_prices"

# Setup Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='stock_data_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Setup S3 Client
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def upload_message_to_s3(data):
    """Upload Kafka-consumed message to S3."""
    symbol = data.get("symbol", "unknown")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    file_name = f"{symbol}_{timestamp}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"kafka_messages/{file_name}",
        Body=json.dumps(data),
        ContentType='application/json'
    )
    print(f"📥 Kafka message uploaded to S3: kafka_messages/{file_name}")

# Start consuming and uploading
print("🎧 Kafka Consumer started...")
for message in consumer:
    upload_message_to_s3(message.value)
