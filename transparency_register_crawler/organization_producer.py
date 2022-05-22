from abstract.producer import Producer
from build.gen.bakdata.corporate.v1 import corporate_pb2
from build.gen.bakdata.corporate.v1.corporate_pb2 import Organization  # type: ignore


class TransRegOrganizationProducer(Producer):
    TOPIC = "transparency-organization-events"

    def __init__(self):
        super().__init__(TransRegOrganizationProducer.TOPIC, corporate_pb2.Organization)

    def get_key(self, message: Organization):
        return message.identificationCode
