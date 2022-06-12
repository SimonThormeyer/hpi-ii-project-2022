import requests
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer
from build.gen.bakdata.organization.v1.organization_pb2 import Organization  # type: ignore
from rb_crawler.rb_producer import RbProducer


class TrOrgConsumer:
    BOOTSTRAP_SERVER: str = "localhost:29092"
    SCHEMA_REGISTRY_URL: str = "http://localhost:8081"
    CORPORATE_TOPIC = RbProducer.TOPIC
    SCHEMA = Organization

    def __init__(self):
        protobuf_deserializer = ProtobufDeserializer(
            TrOrgConsumer.SCHEMA, {"use.deprecated.format": True}
        )

        conf = {
            "bootstrap.servers": TrOrgConsumer.BOOTSTRAP_SERVER,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": protobuf_deserializer,
            "group.id": "test"
        }

        self.consumer = DeserializingConsumer(conf)
        self.consumer.subscribe(["transparency-organization-events"])
