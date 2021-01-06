# AGR Literature Serivice

# ElasticSearch

Resource: https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html

## Pull image
```bash
docker pull docker.elastic.co/elasticsearch/elasticsearch:7.10.1
```


## Run Single Node

```bash
docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.10.1
```

## RESTful

https://flask-restful.readthedocs.io/en/latest/quickstart.html
