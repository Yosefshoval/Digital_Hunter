from confluent_kafka import Consumer
from config import ConsumerConfig

logger = ConsumerConfig.logger

consumer = Consumer(ConsumerConfig.consumer_config)
consumer.subscribe(ConsumerConfig.topics_list)
logger.info(f"kafka consumer created and subscribe to topics {ConsumerConfig.topics_list}")


def get_message():
    message = consumer.poll(1.0)
    if message is None:
        return None
    if message.error():
        logger.error(f"{message.error()}")
        return None
    logger.info(f"message received from topic {message.topic()}")
    value = message.value().decode("utf-8")
    print(value)
    return message.topic(), value


print(get_message())