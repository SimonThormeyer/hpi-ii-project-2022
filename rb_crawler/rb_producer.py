from abstract.producer import Producer
from build.gen.bakdata.corporate.v1 import corporate_pb2
from build.gen.bakdata.corporate.v1.corporate_pb2 import Corporate  # type: ignore


class RbProducer(Producer):
    TOPIC = "corporate-events"

    def __init__(self):
        super().__init__(RbProducer.TOPIC, corporate_pb2.Corporate)

    def get_key(self, corporate: Corporate):
        return corporate.id
