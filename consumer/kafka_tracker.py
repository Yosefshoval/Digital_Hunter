from confluent_kafka import Consumer
from config import ConsumerConfig
import json


consumer = Consumer(ConsumerConfig.consumer_config)
consumer.subscribe(ConsumerConfig.topics_list)


def get_message():
    message = consumer.poll(1.0)
    if message is None:
        return None
    print(message.topic())
    if message.topic():
        print(message.value())

import time
while True:
    time.sleep(1.0)
    print(get_message())

"""
intel:
b'{
"timestamp": "2026-03-16T09:51:11.968121+00:00", 
"signal_id": "987e95b9-800f-478a-8de7-6a94cb79cdd1", 
"entity_id": "TGT-006", 
"reported_lat": 31.668593, 
"reported_lon": 34.33561, 
"signal_type": "VISINT", 
"priority_level": 2
}'
"""