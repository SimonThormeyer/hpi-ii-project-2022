from build.gen.bakdata.tr_rb_integration.v1.tr_rb_integration_pb2 import IntegratedOrganization  # type: ignore
from abstract.producer import Producer
from build.gen.bakdata.tr_rb_integration.v1.tr_rb_integration_pb2 import IntegratedOrganization  # type:ignore


class DedupProducer(Producer):
    TOPIC = "dedup-organization-events-2"

    def __init__(self):
        super().__init__(DedupProducer.TOPIC, IntegratedOrganization)

    def get_key(self, message: IntegratedOrganization):
        return message.id