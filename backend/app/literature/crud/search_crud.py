from elasticsearch import Elasticsearch
from literature.config import config


def search_references(query):
    es_host = config.ELASTICSEARCH_HOST
    es = Elasticsearch(hosts=es_host + ":" + config.ELASTICSEARCH_PORT)
    res = es.search(index="references_index",
                    body={
                        "query": {
                            "match": {
                                "title": query
                            }
                        }
                    })
    return [{"curie": ref["_source"]["curie"], "title": ref["_source"]["title"]} for ref in res["hits"]["hits"]]
