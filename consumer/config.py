import os
import logging
from logger import log_event


class ConsumerConfig:
    logger = logging.getLogger("message handler")
    logging.basicConfig(level=logging.INFO)
    logger.info('logger created')

    KAFKA_URL = os.getenv("KAFKA_URL", "localhost:9092"),

    consumer_config = {
        "bootstrap.servers": KAFKA_URL,
        "group.id": "main_tracker",
        "auto.offset.reset": "earliest"
    }

    producer_config = {"bootstrap.servers": "localhost:9092"}

    topics_list = ["attack", "damage", "intel"]

    mongodb_url = os.getenv("MONGO_URL", "mongodb://localhost:27017")
    mongo_database = os.getenv("MONGO_DATABASE", "digital_hunters")
    mongo_collections = ["attacks", "damages", "intels"]

    lowest_priority_level = 99
