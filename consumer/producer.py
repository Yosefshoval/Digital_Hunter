from confluent_kafka import Producer
from config import ConsumerConfig
import json

logger = ConsumerConfig.logger

producer = Producer(ConsumerConfig.producer_config)

def callback(err, msg):
    if err:
        logger.error(err)
        return
    logger.info(f"message produced: {msg.value}")

def produce_message(message: dict, topic: str):
    producer.produce(
        topic=topic,
        value=json.dumps(message).encode(),
        callback=callback
    )
    producer.flush()