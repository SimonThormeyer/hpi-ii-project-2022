from abstract.producer import Producer
from build.gen.bakdata.organization.v1 import organization_pb2
from build.gen.bakdata.organization.v1.organization_pb2 import Organization  # type: ignore


class TransRegOrganizationProducer(Producer):
    TOPIC = "transparency-organization-events"

    def __init__(self):
        super().__init__(TransRegOrganizationProducer.TOPIC, organization_pb2.Organization)

    def get_key(self, message: Organization):
        return message.identificationCode
