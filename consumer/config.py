import os

class ConsumerConfig:
    KAFKA_URL = os.getenv("KAFKA_URL", "localhost:9092"),

    consumer_config = {
        "bootstrap.servers": KAFKA_URL,
        "group.id": "main_tracker",
        "auto.offset.reset": "earliest"
    }

    topics_list = ["attack", "damage", "intel"]

    mongodb_url = os.getenv("MONGO_URL", "mongodb://localhost:27017")
    mongo_database = os.getenv("MONGO_DATABASE", "digital_hunters")
    mongo_collections = ["attack", "damage", "intel"]


