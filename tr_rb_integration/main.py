import hashlib
import logging
import os
import re
from build.gen.bakdata.corporate.v1.corporate_pb2 import Corporate  # type: ignore
from build.gen.bakdata.organization.v1.organization_pb2 import Organization  # type: ignore
from build.gen.bakdata.tr_rb_integration.v1.tr_rb_integration_pb2 import Grant, IntegratedOrganization  # type: ignore
from tr_rb_integration.tr_organizations_consumer import TrOrgConsumer
from tr_rb_integration.tr_rb_producer import TrRbProducer
from elasticsearch import Elasticsearch

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


def integrate_rb_corporates():
    producer = TrRbProducer()
    es = Elasticsearch("http://localhost:9200")
    # load corporate pages
    rb_body = {"query": {
        "query_string": {
            "query": "create",
            "default_field": "event_type"
        }
    },
        "size": 10000
    }
    result = es.search(index='corporate-events', body=rb_body, scroll="200m")
    rb_hits = result['hits']['hits']
    scroll_id = result["_scroll_id"]

    while rb_hits:
        for rb_hit in rb_hits:
            corporate_data = rb_hit['_source']
            # parse corporate
            corporate = Corporate(**corporate_data)
            regex_result = re.findall(r"(.*?:\s)?(.*?)(,|(\s\())", string=corporate.information)
            company_name = regex_result[0][1]

            i_org = IntegratedOrganization()
            i_org.rb_reference_id = str(corporate.rb_id)
            i_org.rb_registrationDate = corporate.event_date
            i_org.name = company_name
            i_org.rb_information = corporate.information
            i_org.id = hashlib.sha1(
                f"{corporate.rb_id}{corporate.event_date}{company_name}{corporate.information}".encode(
                    'utf-8')).hexdigest()
            producer.produce_to_topic(i_org)

        rb_hits = es.scroll(scroll_id=scroll_id, scroll='1s')['hits']['hits']


def integrate_tr_organizations():
    consumer = TrOrgConsumer()
    producer = TrRbProducer()
    while True:
        message = consumer.consumer.poll(timeout=60)
        if message is None:
            break
        organization: Organization = message.value()

        i_org = IntegratedOrganization()
        i_org.tr_identificationCode = organization.identificationCode
        i_org.tr_registrationDate = organization.registrationDate
        i_org.name = organization.name.originalName
        for interest in organization.interests:
            i_org.interests.append(interest.name)
        for grant in organization.financialData.closedYear.grants:
            i_org.grants.append(Grant(amount=grant.amount.absoluteCost, source=grant.source))
        i_org.id = hashlib.sha1(
            f"{i_org.tr_identificationCode}{i_org.tr_registrationDate}{i_org.name}{i_org.interests}{i_org.grants}".encode(
                'utf-8')).hexdigest()

        producer.produce_to_topic(i_org)


def run():
    integrate_tr_organizations()
    integrate_rb_corporates()


if __name__ == '__main__':
    run()
