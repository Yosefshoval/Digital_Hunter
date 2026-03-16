from datetime import datetime
from pydantic import BaseModel

class Intel(BaseModel):
    timestamp: datetime
    signal_id: str
    entity_id: str
    reported_lat: float
    reported_lon: float
    signal_type: str
    priority_level: int


class Attack(BaseModel):
    timestamp: datetime
    attack_id: str
    entity_id: str
    weapon_type: str


class Damage(BaseModel):
    timestamp: datetime
    attack_id: str
    entity_id: str
    result: str


def validate_intel_message(message_content: dict):
    Intel(**message_content)

def validate_attack_message(message_content: dict):
    Attack(**message_content)

def validate_damage_message(message_content: dict):
    Damage(**message_content)
