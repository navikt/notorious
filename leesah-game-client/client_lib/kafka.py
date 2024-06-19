import os
import uuid
from typing import Dict


def base_config(bootstrap_servers: str) -> Dict:
    config = {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SSL",
        "ssl.ca.location": os.getenv("KAFKA_CA_PATH"),
        "ssl.certificate.location": os.getenv("KAFKA_CERTIFICATE_PATH"),
        "ssl.key.location": os.getenv("KAFKA_PRIVATE_KEY_PATH"),
    }
    return config


def consumer_config(bootstrap_servers: str, auto_commit: bool):
    return base_config(bootstrap_servers) | {
        "group.id": str(uuid.uuid4()),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": str(auto_commit),
    }


def producer_config(bootstrap_servers: str):
    return base_config(bootstrap_servers) | {"broker.address.family": "v4"}
