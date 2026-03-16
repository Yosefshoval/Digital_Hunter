from pymongo import MongoClient
from config import ConsumerConfig


class MongoDB:
    def __init__(self):
        self.client = MongoClient(ConsumerConfig.mongodb_url)


    def get_collection(self, coll_name: str):
        if coll_name not in ConsumerConfig.mongo_collections:
            return False
        db = self.client[ConsumerConfig.mongo_database]
        coll = db[coll_name]
        return coll

    def upsert_message(self, message: dict):
        conn = self.get_collection("intel")
        message_already_exists = conn.find_one(message['entity_id'])
        if not message_already_exists:
            conn.insert_one(document=message)
            return True
        result = conn.update_one(
            filter={"entity_id" : message["entity_id"]},
            update=message,
            upsert=True
        )
        return result


# mongo = MongoDB()
# print(mongo.client.is_mongos)