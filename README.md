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
```bash
python src/xml_processing/get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set
```

### Download xml from an input file at a URL
```bash
python src/xml_processing/get_pubmed_xml.py -u http://tazendra.caltech.edu/~azurebrd/var/work/pmid_sample
```

### Download xml from commandline flags
```bash
python src/xml_processing/get_pubmed_xml.py -c 12345678 4576890 18280033
```

### Download xml from a database-based job (stub)
```bash
python src/xml_processing/get_pubmed_xml.py -d
```

### Download xml from a REST API-based job (stub)
```bash
python src/xml_processing/get_pubmed_xml.py -r
```

### Download xml from a hardcoded sample
```bash
python src/xml_processing/get_pubmed_xml.py -s
```

## Convert PubMed XML to JSON

### Has same flags as get_pubmed_xml.py, e.g.
```bash
python src/xml_processing/xml_to_json.py  -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set
```

## Generate resource JSON from J_Medline.txt
To use pubmed url file -u flag<br/>
To use local file use -l flag<br/>
To upload to aws s3 agr-literature/develop/resource/metadata/resource_pubmed_all.json use -s flag<br/>

### Generate from URL at https://ftp.ncbi.nih.gov/pubmed/J_Medline.txt (default if no flag), and upload to s3 bucket
```bash
pipenv run python src/xml_processing/generate_pubmed_nlm_resource.py -u -s
```

### Generate from local J_Medline.txt file and no upload
```bash
pipenv run python src/xml_processing/generate_pubmed_nlm_resource.py -l
```

## Download DQM MOD data from FMS
get_dqm_data.py downloads DQM MOD JSON from FMS and uncompresses.  compares md5sum to current file to prevent downloading if it's the same
```bash
python src/xml_processing/get_dqm_data.py
```

## Generating login credentials (must repeat every 12 hours to access base linux image and neo4j env image)
- make sure you have AWS-CLI installed locally
- make sure you have AWS login credentials for the agr_aws account, with the permission group - AWS group for ECR access.
- create a ~/.aws/config file with the following content:
`[default]
region=us-east-1`
- create a ~/.aws/credentials file with the following content (swap aws_access_key_id and aws_secret_access_key with your appropriate values.  You may need to regenerate your aws_secret_access_key to obtain these values).
`[default]
aws_access_key_id =
aws_secret_access_key = `
-  To test that your credentials are working correctly, run `aws ecr get-login-password should spit out a token` and verify a token is produced.
- Touch .docker/config.json
- Run this command to push the credentials generated into config.json:
`aws ecr get-login-password | docker login --username AWS --password-stdin 100225593120.dkr.ecr.us-east-1.amazonaws.com`
- Verify that you can pull the neo4j env image:
`docker pull 100225593120.dkr.ecr.us-east-1.amazonaws.com/agr_neo4j_env:4.0.0`
- Proceed with the appropriate make commands as usual.
- Reminder: this process needs to be repeated every time you get an error like this (usually ~ every 12 hours):
`Error response from daemon: pull access denied for 100225593120.dkr.ecr.us-east-1.amazonaws.com/agr_neo4j_env, repository does not exist or may require 'docker login': denied: Your authorization token has expired. Reauthenticate and try again.`
