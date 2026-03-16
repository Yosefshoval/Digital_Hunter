import os

class ConsumerConfig:
    KAFKA_URL = os.getenv("KAFKA_URL", "localhost:9092"),

    consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "main_tracker",
        "auto.offset.reset": "earliest"
    }

    topics_list = ["attack", "damage", "intel"]


