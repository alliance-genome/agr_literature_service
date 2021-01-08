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

# RESTful

https://flask-restful.readthedocs.io/en/latest/quickstart.html

# Development

Setup development environment

```bash
python3 -m venv venv
```

Install Pipenv

```bash
pip3 install pipenv
```

## Running Server

```bash
pipenv run python src/main.py --port=<port>
```

## Curling endpoint

Add a reference
```bash
curl http://localhost:5003/reference/PMID:4040 -d 'data={"title": "Worms abi-1"}' -X POST
```

Get a Reference
```bash
curl http://localhost:5004/reference/PMID:4040
```

# Production

use --prod argument to use the WSGI server in production
