from confluent_kafka import Consumer
from config import ConsumerConfig
import json

logger = ConsumerConfig.logger


def get_message():
    message = consumer.poll(1.0)
    if message is None:
        return None
    logger.info(f"message received from topic {message.topic()}")
    return {"topic": message.topic(), "message": json.loads(message.value().decode())}
