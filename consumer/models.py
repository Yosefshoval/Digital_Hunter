from datetime import datetime
from pydantic import BaseModel

class Intel(BaseModel):
    timestamp: str
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


def validate_message(message_content: dict, type: str):
    if type == "intel":
        Intel(**message_content)
    if type == "attack":
        Attack(**message_content)
    if type == "damage":
        Damage(**message_content)
    return True
