from mongo_connection import MongoDB
from kafka_tracker import get_message
from config import ConsumerConfig
from haversine import haversine_km
from models import *

logger = ConsumerConfig.logger


mongo_client = MongoDB()
logger.info("mongodb client connected")

consumer = Consumer(ConsumerConfig.consumer_config)
consumer.subscribe(ConsumerConfig.topics_list)
logger.info(f"kafka consumer created and subscribe to topics {ConsumerConfig.topics_list}")


def main():
    while True:
        message = get_message()
        if message is None:
            continue
        if message.error():
            logger.error(f"{message.error()}")
            continue

        # steps:
        # 1) check the topic.
        # 2) for intel topic: insert / update the message as is in mongo -> client.upsert(message["message"], type=message["topic"])
        # 3) for attack topic: insert new attack in 'attacks' collection. update entity (by 'id'), the 'weapon_type', 'attack_id' and 'attack_timestamp'
        # 4) for damage topic: insert new damage in 'damages' collection. update entity (by 'id'), the 'result', 'attack_id' and 'damage_timestamp'


        topic = message['topic']
        content = message['content']
        try:
            match topic:
                case "intel":
                    validate_intel_message(content)

                    message_exists = mongo_client.check_message_exists(content, topic)
                    message['priority_level'] = ConsumerConfig.lowest_priority_level

                    movement_distance = haversine_km(
                        lat1=message_exists['lat_reported'],
                        lon1=message_exists['lon_reported'],
                        lat2=content['lat_reported'],
                        lon2=content['lon_reported'],
                    ) if message_exists else 0

                    content['movement_distance'] = movement_distance

                    inserted = self.insert_message(content, type)
                    logger.info(f"message content inserted to {topic} collection. new id: {inserted.inserted_id}")

                case "attack":
                    pass

                case "damage":
                    pass

        except:
            pass


if __name__ == "__main__":
    logger.info("service starting...")
    main()
