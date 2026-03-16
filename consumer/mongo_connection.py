from typing import Literal
from pymongo import MongoClient
from config import ConsumerConfig
from models import Intel, Attack, Damage

logger = ConsumerConfig.logger
TOPIC_TYPES = Literal["intel", "attack", "damage"]


class MongoDB:
    def __init__(self):
        self.client = MongoClient(ConsumerConfig.mongodb_url)


    def get_collection(self, coll_name: str):
        if coll_name not in ConsumerConfig.mongo_collections:
            return False
        db = self.client[ConsumerConfig.mongo_database]
        coll = db[coll_name]
        return coll


    def update_message(self, message: dict, type: TOPIC_TYPES):
        pass


    def insert_message(self, message: dict, type: TOPIC_TYPES):
        conn = self.get_collection(coll_name=f"{type}s")
        result = conn.insert_one(document=message)
        return result


    def check_message_exists(self, message: dict, type: TOPIC_TYPES):
        conn = self.get_collection(coll_name=f"{type}s")
        message_exists = conn.find_one(message['entity_id'])
        if not message_exists:
            return False
        return message_exists


# message['priority_level'] = lowest_priority_level
# inserted = self.insert_message(message, type)