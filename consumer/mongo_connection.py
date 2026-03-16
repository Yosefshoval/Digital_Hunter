from typing import Literal
from pymongo import MongoClient
from config import ConsumerConfig
from models import Intel, Attack, Damage
from logger import log_event

TOPIC_TYPES = Literal["intel", "attack", "damage"]


class MongoDB:
    def __init__(self):
        self.client = MongoClient(ConsumerConfig.mongodb_url)


    def get_collection(self, coll_name: str):
        if coll_name not in ConsumerConfig.mongo_collections:
            raise TypeError(f"nu such supported collection: {coll_name}")
        db = self.client[ConsumerConfig.mongo_database]
        coll = db[coll_name]
        return coll


    def update_message(self, entity_id, message: dict, type: TOPIC_TYPES):
        collection = self.get_collection(coll_name=f"{type}s")
        updated = collection.update_one(
            filter={"entity_id" : entity_id},
            update={"$set" : message},
        )
        log_event('INFO', f'message updated with entity id {entity_id}', message)
        return updated


    def insert_message(self, message: dict, type: TOPIC_TYPES):
        collection = self.get_collection(coll_name=f"{type}s")
        result = collection.insert_one(document=message)
        log_event('INFO', f'message inserted to mongo in collection {type}', message)
        return result


    def check_message_exists(self, message: dict, type: TOPIC_TYPES):
        conn = self.get_collection(coll_name=f"{type}s")
        message_exists = conn.find_one({"entity_id" : message['entity_id']})
        log_event('INFO', f"message with id {message['entity_id']} {'exists' if message_exists else 'not exists'} in mongo")

        if not message_exists:
            return False
        return message_exists

