import json

from kafka import KafkaConsumer
from src.kafka.tbank.config import SERVER
from src.kafka.tbank.topics import Topic

consumer = KafkaConsumer(
    Topic.CANDLESHOUR.value,
    bootstrap_servers=[SERVER],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='python-consumer',
)
