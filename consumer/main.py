import time
from mongo_connection import MongoDB
from kafka_tracker import get_message
from config import ConsumerConfig
from haversine import haversine_km
from models import *
from producer import produce_message
import json
from pydantic import ValidationError


logger = ConsumerConfig.logger


mongo_client = MongoDB()
logger.info("mongodb client connected")



def main():
    while True:
        message = get_message()
        if message is None:
            continue
        topic, message = message[0], message[1]

        try:
            content = json.loads(message)
            validate_message(content, topic)

        except (json.decoder.JSONDecodeError, ValidationError) as e:
            logger.error(f'{e.__class__.__name__}: {e}')
            produce_message(
                {"error": str(e), "problematic_message": message},
                'dlq_signals_intel'
            )
            continue

        try:
            match topic:
                case "intel":
                    message_exists = mongo_client.check_message_exists(content, topic)
                    if not message_exists: content['priority_level'] = ConsumerConfig.lowest_priority_level
                    content['attacked'] = False

                    if message_exists:
                        movement_distance = haversine_km(
                            lat1=message_exists['reported_lat'],
                            lon1=message_exists['reported_lon'],
                            lat2=content['reported_lat'],
                            lon2=content['reported_lon'],
                        )
                    else:
                        movement_distance = 0

                    content['movement_distance'] = movement_distance

                    inserted = mongo_client.insert_message(content, topic)
                    logger.info(f"message content inserted to {topic}s collection. new id: {inserted.inserted_id}")

                case "attack":
                    if not mongo_client.check_message_exists(content, topic):
                        raise ValueError("entity not found in the targets bank")

                    inserted = mongo_client.insert_message(content, topic)
                    logger.info(f"message content inserted to {topic}s collection. new id: {inserted.inserted_id}")
                    entity_id = content["entity_id"]
                    attack = {
                        "attacked": True,
                        "weapon_type": content["weapon_type"],
                        "attack_id": content['attack_id']
                    }
                    mongo_client.update_message(entity_id, attack, topic)

                case "damage":
                    if not mongo_client.check_message_exists(content, topic):
                        raise ValueError("entity not found in the targets bank")

                    inserted = mongo_client.insert_message(content, topic)
                    logger.info(f"message content inserted to {topic}s collection. new id: {inserted.inserted_id}")
                    entity_id = content["entity_id"]
                    damage = {
                        "result": content["result"],
                    }
                    mongo_client.update_message(entity_id, damage, topic)

        except Exception as e:
            logger.error(f'{e.__class__.__name__}: {e}')


if __name__ == "__main__":
    logger.info("service starting...")
    main()