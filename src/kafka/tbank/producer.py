import json

from kafka import KafkaProducer

from .config import SERVER
from .json_encoder import json_encoder

producer = KafkaProducer(
    bootstrap_servers=[SERVER],
    value_serializer=lambda m: json.dumps(m, default=json_encoder).encode('utf-8'),
    max_block_ms=5000
)
