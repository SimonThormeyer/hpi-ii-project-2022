from re import findall

import requests
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer
from build.gen.bakdata.organization.v1.organization_pb2 import Organization  # type: ignore
from rb_crawler.rb_producer import RbProducer
from build.gen.bakdata.tr_rb_integration.v1.tr_rb_integration_pb2 import IntegratedOrganization  # type: ignore
from tr_rb_integration.tr_rb_producer import TrRbProducer
from build.gen.bakdata.corporate.v1.corporate_pb2 import Corporate  # type: ignore


class TrRbConsumer:
    BOOTSTRAP_SERVER: str = "localhost:29092"
    SCHEMA_REGISTRY_URL: str = "http://localhost:8081"
    CORPORATE_TOPIC = RbProducer.TOPIC
    SCHEMA = Corporate

    def __init__(self):
        protobuf_deserializer = ProtobufDeserializer(
            TrRbConsumer.SCHEMA, {"use.deprecated.format": True}
        )

        conf = {
            "bootstrap.servers": TrRbConsumer.BOOTSTRAP_SERVER,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": protobuf_deserializer,
            "group.id": "test"
        }

        self.consumer = DeserializingConsumer(conf)
        self.consumer.subscribe(["corporate-events"])
        self.producer = TrRbProducer()


if __name__ == '__main__':
    consumer = TrRbConsumer()
    while True:
        message = consumer.consumer.poll(timeout=-1)
        corporate: Corporate = message.value()
        if corporate.event_type == "Neueintragungen":
            corporate_name = findall(pattern=r".*:\s(.*)\s\(.*\)?,", string=corporate.information)[0]
            payload = {
                "min_score": 10,
                "query": {
                    "query_string": {
                        "query": corporate_name,
                        "default_field": "name.originalName"
                    }
                }
            }
            elastic_search_url = "http://localhost:9200/transparency-organization-events/_search"
            organization = requests.get()
