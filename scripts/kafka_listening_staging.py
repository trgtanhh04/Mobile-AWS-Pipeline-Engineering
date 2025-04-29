#! /usr/bin/env python
import json
import os
import time
import sys
import csv
import pandas as pd
import logging
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from kafka.admin import KafkaAdminClient, NewTopic
import threading
import subprocess



# Configure logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "kafka.log"), 'a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

from utils.constants import INPUT_PATH, OUTPUT_PATH
from utils.constants import KAFKA_BOOTSTRAP_SERVERS, TOPIC_PHONE_DATA


def reset_kafka_topic(bootstrap_servers, topic):
    """
    Xóa topic Kafka (nếu tồn tại) và tạo lại topic mới.
    """
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        # Xóa topic nếu tồn tại
        logger.info(f"Checking if topic '{topic}' exists...")
        topics = admin_client.list_topics()
        if topic in topics:
            logger.info(f"Topic '{topic}' already exists. Deleting it...")
            admin_client.delete_topics([topic])
            logger.info(f"Topic '{topic}' deleted successfully.")
        
        # Tạo lại topic
        logger.info(f"Creating topic '{topic}'...")
        admin_client.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
        logger.info(f"Topic '{topic}' created successfully.")
    except Exception as e:
        logger.error(f"Failed to reset topic '{topic}': {e}")
    finally:
        admin_client.close()


def send_to_kafka(data, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=TOPIC_PHONE_DATA):
    """
    Send data to Kafka topic.
    Args:
        data (list[dict]): List of product data to send.
        bootstrap_servers (str): Kafka bootstrap servers.
        topic (str): Kafka topic name.
    """
     # Reset topic trước khi gửi
    reset_kafka_topic(bootstrap_servers, topic)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")  # Serialize data to JSON
    )
    try:
        for record in data:
            producer.send(topic, value=record)
            # logger.info(f"Sent record to Kafka topic '{topic}': {record}")
        producer.flush()
        logger.info(f"All records have been sent to Kafka topic '{topic}'.")
    except KafkaError as e:
        logger.error(f"Error sending data to Kafka: {e}")
    finally:
        producer.close()


def trigger_new_data_event(timestamp=None, data=None):
    """Trigger a new data event to Kafka topic.
    Args:
        timestamp (str): Timestamp of the new data.
        data (any): Data associated with the new event.
    """

    if timestamp is None:
        logger.error("Timestamp is None. Cannot trigger new data event.")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return
    message_key = f"new_data_event_{timestamp}"
    message = {
        'data': data
    }
    # logger.info(f"Triggering new data event with timestamp: {timestamp} and data: {data}")
    logger.info(f"Sending message to Kafka topic: {TOPIC_PHONE_DATA}")
    return send_to_kafka(topic=TOPIC_PHONE_DATA, data=message)

def consume_messages_from_kafka(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=None, group_id=None, callback=None):
    """Consume messages from Kafka topic.
    Args:
        bootstrap_servers (str): Kafka bootstrap servers.
        topic (str): Kafka topic name.
    """
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info(f"Consuming messages from Kafka topic: {topic}")
        for message in consumer:
            try:
                logger.info(f"Received message: {message.value}")
                callback(message)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
            # Process the message here
    except KafkaError as e:
        logger.error(f"Failed to consume message: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

def process_message(message):
    """Process the message received from Kafka.
    Args:
        message (dict): The message received from Kafka.
    """
    logger.info(f"Processing message: {message}")
    print(f"Processed message: {message}")


if __name__ == "__main__":
    # Example usage
    example_data = [
        {
            "product_name": "Điện thoại Xiaomi Redmi 12C (Helio G85)",
            "brand": "Redmi",
            "variants": [
                {"color": "Xanh Đậm", "memory": "4-64GB", "price": "1.650.000 ₫", "old_price": "2.950.000₫"},
                {"color": "Xanh bạc hà", "memory": "4-64GB", "price": "1.650.000 ₫", "old_price": "2.950.000₫"}
            ],
            "warranty": "BH Thường 12 tháng",
            "specifications": {
                "screen": "IPS LCD, 500 nits (typ), 6.71 inches, HD+ (720 x 1650 pixels)",
                "os": "Android 12, MIUI 13",
                "camera_rear": "50 MP (góc rộng), PDAF",
                "camera_front": "5 MP",
                "cpu": "Helio G85",
                "battery": "Li-Po 5000 mAh"
            },
            "rating": "5/5",
            "reviews_count": 11,
            "link": "https://mobilecity.vn/dien-thoai/xiaomi-redmi-12c-helio-g85.html"
        }
    ]

    # Gửi dữ liệu lên Kafka
    send_to_kafka(data=example_data)

    consume_messages_from_kafka(topic=TOPIC_PHONE_DATA, group_id="phone_data_group", callback=process_message)
