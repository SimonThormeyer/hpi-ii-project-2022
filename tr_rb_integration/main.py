import re
from build.gen.bakdata.corporate.v1.corporate_pb2 import Corporate  # type: ignore
from tr_rb_integration.tr_rb_producer import TrRbProducer
from elasticsearch import Elasticsearch


def run():
    producer = TrRbProducer()
    es = Elasticsearch("http://localhost:9200")
    # load corporate pages
    rb_body = {"query": {
        "query_string": {
            "query": "create",
            "default_field": "event_type"
        }
    },
    "size": 10000}
    rb_hits = es.search(index='corporate-events', body=rb_body)['hits']['hits']

    for rb_hit in rb_hits:
        corporate_data = rb_hit['_source']
        # parse corporate
        corporate = Corporate(**corporate_data)
        company_name = re.findall(r"(.*?:\s)??(.*?),", string=corporate.information)[0][1]
        tr_body = {"query": {
            "query_string": {
                "query": company_name,
                "default_field": "name.originalName"
            }
        },
            "min_score": 10
        }

        tr_body = {
            "query": {
                "fuzzy": {
                    "name.originalName": {
                        "value": "SAP",
                        "fuzziness": "AUTO",
                        "max_expansions": 50,
                        "prefix_length": 0,
                        "transpositions": True,
                        "rewrite": "constant_score"
                    }
                }
            }
        }

        tr_hits = es.search(index='transparency-organization-events', body=tr_body, )['hits']['hits']
        if tr_hits:
            hit = tr_hits[0]

    # for each message in page
    #   find integration from elastic
    #   if found create IntegrationOrganization and produce
    # producer.produce_to_topic()


if __name__ == '__main__':
    run()
