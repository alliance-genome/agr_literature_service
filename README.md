# AGR Literature Service

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
curl http://localhost:5003/references/add/ -d 'data={"title": "Worms abi-1", "pubmed_id": "PMID:53e565", "mod": "WB", "pubmod_id": "WBPub:0e0000003"}' -X POST
```

Get a Reference
```bash
curl http://localhost:5004/reference/PMID:4040
```

# Production

use --prod argument to use the WSGI server in production


# XML Processing

## Downloading PubMed XML

### Download xml from an input file in the file system
python src/xml_processing/get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set

### Download xml from an input file at a URL
python src/xml_processing/get_pubmed_xml.py -u http://tazendra.caltech.edu/~azurebrd/var/work/pmid_sample

### Download xml from commandline flags
python src/xml_processing/get_pubmed_xml.py -c 12345678 4576890 18280033

### Download xml from a database-based job (stub)
python src/xml_processing/get_pubmed_xml.py -d

### Download xml from a hardcoded sample
python src/xml_processing/get_pubmed_xml.py -s

