from kafka import KafkaProducer
import csv
import json
import time

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'ecommerce-transactions'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Path to your e-commerce CSV file
csv_file_path = '../data/raw/E commerce.csv'

# Read and send CSV data
with open(csv_file_path, mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        producer.send(TOPIC_NAME, value=row)
        print(f"Sent: {row}")
        time.sleep(1)  # simulate near real-time ingestion

producer.flush()
producer.close()
