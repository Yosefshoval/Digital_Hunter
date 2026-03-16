from confluent_kafka import Consumer
from config import ConsumerConfig
from logger import log_event

consumer = Consumer(ConsumerConfig.consumer_config)
consumer.subscribe(ConsumerConfig.topics_list)
log_event('INFO', f"kafka consumer created and subscribe to 3 topics.", {"topics": ConsumerConfig.topics_list})


def get_message():
    message = consumer.poll(1.0)
    if message is None:
        return None
    if message.error():
        log_event('ERROR', f"{message.error()}")
        return None
    log_event('INFO', f"message received from topic {message.topic()}")
    value = message.value().decode("utf-8")
    print(value)
    return message.topic(), value


print(get_message())