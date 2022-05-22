from abstract.producer import Producer
from build.gen.bakdata.person.v1 import person_pb2
from build.gen.bakdata.person.v1.person_pb2 import Person  # type: ignore


class TransRegPersonProducer(Producer):
    TOPIC = "transparency-person-events"

    def __init__(self):
        super().__init__(TransRegPersonProducer.TOPIC, person_pb2.Person)

    def get_key(self, message: Person):
        return f"{message.orgIdentificationCode}#{message.firstName}#{message.lastName}"
