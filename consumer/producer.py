from confluent_kafka import Producer
from config import ConsumerConfig
import json
from logger import log_event


producer = Producer(ConsumerConfig.producer_config)

def callback(err, msg):
    if err:
        log_event('ERROR', err)
        return
    log_event('INFO', f"message produced.", msg.value())

def produce_message(message: dict, topic: str):
    producer.produce(
        topic=topic,
        value=json.dumps(message).encode(),
        callback=callback
    )
    producer.flush()