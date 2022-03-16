from elasticsearch import Elasticsearch
from literature.config import config


def show(query):
    es_host = config.ELASTICSEARCH_HOST
    if not (es_host.startswith('http://') or es_host.startswith('https://')):
        es_host = ('http://' if config.ELASTICSEARCH_PORT != "443" else 'https://') + es_host
    es = Elasticsearch(hosts=es_host + ':' + config.ELASTICSEARCH_PORT)
    res = es.search(index="references_index", q=query)
    return [{'curie': ref['_source']['curie'], 'title': ref['_source']['title']} for ref in res['hits']['hits']]
