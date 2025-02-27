import re

from build.gen.bakdata.tr_rb_integration.v1.tr_rb_integration_pb2 import Grant, IntegratedOrganization  # type: ignore
import logging
import os

from elasticsearch import Elasticsearch

from cleansing.dedup_producer import DedupProducer

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


def run():
    producer = DedupProducer()
    es = Elasticsearch("http://localhost:9200")
    # load corporate pages
    rb_body = {"query": {
        "match_all": {}
    },
        "size": 10000
    }
    result = es.search(index="integrated-new", body=rb_body, scroll="200m")
    hits = result['hits']['hits']
    scroll_id = result["_scroll_id"]
    no_id_counter = 0
    while hits:
        for hit in hits:
            corporate_data = hit['_source']
            # parse corporate
            dedup_org = IntegratedOrganization(**corporate_data)
            dedup_org_id = re.sub(r'\W+', '', dedup_org.name).lower()
            # duplicates will be removed via elastic search UPSERT

            if not dedup_org_id:
                dedup_org.id = f"NoId{no_id_counter}"
                no_id_counter += 1
            else:
                dedup_org.id = dedup_org_id
            producer.produce_to_topic(dedup_org)

        hits = es.scroll(scroll_id=scroll_id, scroll='1s')['hits']['hits']

    producer.finish()


if __name__ == '__main__':
    run()
