# AGR Literature Service

## Spin up stack

Create an application container with your local changes
```bash
Make build
```

To spin up all the components of the stack
```bash
docker-compose up
```

## Dependencies

- Postgres
- ElasticSearch
- Redis

# RESTful

https://flask-restful.readthedocs.io/en/latest/quickstart.html

## Building Images

Create a base image with the necessary requirments to run the app

```bash
make build-env
```

Create base image that can be use to run pytest and flake8 etc...

```bash
make build-dev
```

Creating application image

```bash
make build-app
```

## Testing

Run flake8

```bash
make run-flake8
```
### Coverage

This will

```bash
make run-dev-bash
pytest --cov --cov-fail-under=100
```

## Backup and Restore

### Backup

Run the following command to create a backup of the database inside the development container (by running "make run-dev-bash". The compressed file will show up in the root of the project.

```bash
export PGPASSWORD=postgres
pg_dump -h postgres -U postgres -p 5432 -Fc <database-name> > <database-name>.dump
```

### Restore

Also inside the development container run the following command. After creating a blank database (through the pgadmin if you prefer)

```bash
pg_restore -d <newdb> -h postgres -U postgres -p 5432 <database-name>.dump
```


# Development

First creat application image

Use Docker Compose to spin up all the containers

spin up development environment

```bash
docker-compose up
```

## Develop and run applicatoin server in Docker

```bash
make run-dev-bash
python src/server.py --port=<port>
```

## Curling endpoint

Add a reference
```bash
curl http://localhost:<app port>/references/add/ -d 'data={"title": "Worms abi-1", "pubmed_id": "PMID:53e565", "mod": "WB", "pubmod_id": "WBPub:0e0000003"}' -X POST
```

Get a Reference
```bash
curl http://localhost:<app-port>/reference/PMID:4040
```

# Production

use --prod argument to use the WSGI server in production when running the application


# XML Processing

## Scripts and the order to run them in

### get dqm data from fms

- Get dqm data
  - python3 src/xml_processing/get_dqm_data.py
- input
  - mods = ['SGD', 'RGD', 'FB', 'WB', 'MGI', 'ZFIN']
  - datatypes = ['REFERENCE', 'REF-EXCHANGE', 'RESOURCE']
  - 'https://fms.alliancegenome.org/api/datafile/by/' + release + '/' + datatype + '/' + mod + '?latest=true'
- output
  - dqm_data/&lt;datatype&gt;_&lt;mod&gt;.json

### OPTIONAL generate smaller sample set from dqm data
 
- optionally generate sample sets, then  parse_dqm_json_reference.py -f can use dqm_sample/ to test changes on smaller set
- 2 minutes, 13 seconds
  - python3 generate_dqm_json_test_set.py
- input
  - dqm_data/REFERENCE_&lt;mod&gt;.json
- output
  - dqm_sample/REFERENCE_&lt;mod&gt;.json

### Extract pmid data from dqm data

- create directories for sanitize reference json output, report_files, pubmed_json files converted from xml, resource_xml files, input files
  - NOT mkdir src/xml_processing/inputs/ it is part of repo
- get pmid data from dqm data, create inputs/alliance_pmids listing all pmids among all MODs, and pmids_by_mods sorting PMIDs into which mods have them
- 41 seconds
  - python3 parse_dqm_json_reference.py -p
- input
  - dqm_data/REFERENCE_&lt;mod&gt;.json
- output
  - inputs/alliance_pmids
  - pmids_by_mods

### Recursively download pubmed xml, convert to json, get CommentsCorrections with PMIDs, recurse
- Does the job of the next two parts, and recurses to get all possible CommentsCorrections that have non-Comment PMIDs. Logging does not work though.
- 2 hours
  - python3 process_many_pmids_to_json.py -f inputs/alliance_pmids
- output
  - inputs/pubmed_only_pmids
  - inputs/all_pmids
  - pubmed_xml/&lt;files&gt;.xml
  - pubmed_xml/md5sum
  - pmids_not_found
  - pubmed_json/&lt;pmid&gt;.json
  - pubmed_json/md5sum

### Download pubmed xml (if manual instead of recursive above)

- get pubmed xml and store into pubmed_xml/ with list of files not found at pmids_not_found.  skips files already existing in output directory.  downloads in batches of 5000 pmids
- 2 hours
  - python3 get_pubmed_xml.py -f inputs/alliance_pmids
- output
  - pubmed_xml/&lt;files&gt;.xml
  - pubmed_xml/md5sum
  - pmids_not_found

### Convert pubmed xml to json (if manual instead of recursive above)

- take input list of pmids, read .xml and convert to .json
- 2 hours on agr-lit-dev, but only 22 minutes on dev.wormbase
  - python3 xml_to_json.py -f inputs/alliance_pmids
- input
  - inputs/alliance_pmids
  - pubmed_xml/&lt;pmid&gt;.xml
- output
  - pubmed_json/&lt;pmid&gt;.json
  - pubmed_json/md5sum

### Download pubmed resources

- take input medline resources and create .json (optionally upload to s3)
- 1 second
  - python3 generate_pubmed_nlm_resource.py -u
- input
  - https://ftp.ncbi.nih.gov/pubmed/J_Medline.txt
- output
  - pubmed_resource_json/resource_pubmed_all.json
  - optionally upload to s3://agr-literature/develop/resource/metadata/resource_pubmed_all.json

### Process mod+pubmed references+resources to generate sanitized reference json files

- take input mod resources, mod references, pubmed resources, pubmed json, pmids to mod mappings, agr_schemas reference file ; then generate report files for each mod and affecting multiple mods, list of unmatched resourceAbbreviations, sliced json files of mod references / pubmed single-mod references / pubmed multiple-mod reference, mapping of FB resourceAbbreviations to NLM.
- 1 hour 32 minutes on agr-lit-dev, but only 33 minutes on dev.wormbase
  - python3 parse_dqm_json_reference.py -f dqm_data/ -m all
- input
  - dqm_data/RESOURCE_&lt;mod&gt;.json
  - pubmed_resource_json/resource_pubmed_all.json
  - pmids_by_mods
  - https://raw.githubusercontent.com/alliance-genome/agr_schemas/master/ingest/resourcesAndReferences/reference.json
  - dqm_data/REFERENCE_&lt;mod&gt;.json
  - pubmed_json/&lt;pmid&gt;.json
- output
  - report_files/&lt;mod&gt;
  - report_files/multi_mod
  - resource_xml/resource_abbreviation_not_matched
  - sanitized_reference_json/REFERENCE_PUBMOD_&lt;mod&gt;_&lt;counter&gt;.json
  - sanitized_reference_json/REFERENCE_PUBMED_&lt;mod&gt;_&lt;counter&gt;.json
  - sanitized_reference_json/REFERENCE_PUBMED_MULTI_&lt;counter&gt;.json
  - FB_resourceAbbreviation_to_NLM.json

### Process pubmed references from recursive pubmed CommentsCorrections

- take input from list of pmids generated by process_many_pmids_to_json.py
- 3 seconds
  - python3 parse_pubmed_json_reference.py -f inputs/pubmed_only_pmids
- input
  - inputs/pubmed_only_pmids
  - pubmed_json/&lt;pmid&gt;.json
- output
  - sanitized_reference_json/REFERENCE_PUBMED_PMID.json

### Process mod+pubmed resources to generate sanitized resource json files

- take input from pubmed resource, mod resource, fb to nlm mappings ;  produce sanitized files for ingest
- 2 seconds
  - python3 parse_dqm_json_resource.py
- input
  - FB_resourceAbbreviation_to_NLM.json
  - dqm_data/RESOURCE_FB.json
  - dqm_data/RESOURCE_ZFIN.json
  - pubmed_resource_json/resource_pubmed_all.json
- output
  - sanitized_resource_json/RESOURCE_FB.json
  - sanitized_resource_json/RESOURCE_ZFIN.json
  - sanitized_resource_json/RESOURCE_NLM.json

### OPTIONAL sort pmids not found by mod for curators to look into

- optionally  sort_not_found_pmids_by_mod.py takes pmids_not_found from get_pubmed_xml.py, and pmids_by_mods from parse_dqm_json_reference.py, and generates a set sorted by MODs of pmids that were not found in pubmed in file pmids_not_found_by_mod.
- 1 second
  - python3 sort_not_found_pmids_by_mod.py
- input
  - pmids_by_mods
  - pmids_not_found
- output
  - pmids_not_found_by_mod

### OPTIONAL find DOIs in multiple reference curies

- optionally  find_duplicate_doi.py takes sanitized reference .json files, and generates DOIs that have multiple separate curies for curators to clean up.
- 1 second
  - python3 find_duplicate_doi.py
- input
  - sanitized_reference_json/REFERENCE_*.json
- output
  - duplicate_dois

### OPTIONAL try to find out nlms of resourceAbbreviations that do not match, will need analysis

- optionally  try to find nlm of resource_abbreviation_not_matched, by querying ncbi with 5 second delay, output xml to resource_xml/ for future analysis (some do not match, others give multiple nlms)
- 5 seconds * amount of resoureAbbreviations in resource_abbreviation_not_matched (64 minutes on agr-literature-dev with 759 entries)
  - python3 get_pubmed_nlm_resource_unmatched.py
- input
  - resource_xml/resource_abbreviation_not_matched
- output
  - resource_xml/&lt;resourceAbbreviation&gt;.xml

### Post resources to API / DB

- take sanitized resource files, convert them to api format, and post to localhost api.  skip resources that have already been posted before.  if necessary generate an okta token or read from file.
- 12 minutes
  - python3 post_resource_to_api.py
- input
  - sanitized_resource_json/RESOURCE_FB.json
  - sanitized_resource_json/RESOURCE_ZFIN.json
  - sanitized_resource_json/RESOURCE_NLM.json
  - resource_primary_id_to_curie
  - okta_token
- output
  - resource_primary_id_to_curie
  - errors_in_posting_resource
  - okta_token

### Post references to API / DB

- take sanitized reference files, convert them to api format, and post to localhost api.  map resource names to resource curies.  skip references that have already been posted before.  if necessary generate an okta token or read from file.
- 1 day 14 hours 12 minutes
  - python3 post_reference_to_api.py
- input
  - sanitized_reference_json/REFERENCE_*.json
  - resource_primary_id_to_curie
  - reference_primary_id_to_curie
  - okta_token
- output
  - reference_primary_id_to_curie
  - errors_in_posting_reference
  - okta_token

### Post comments-corrections connections to API / DB

- take list of all pmids, process to extract comments-corrections connections, convert to reference curies, and post to localhost api.  map pmids to reference curies.  if necessary generate an okta token or read from file.
- 1 hour 19 minutes
  - python3 post_comments_corrections_to_api.py -f inputs/all_pmids
- input
  - inputs/all_pmids
  - reference_primary_id_to_curie
  - pubmed_json/&lt;pmid&gt;.json
- output
  - okta_token

### Compress and upload to s3

#### pubmed xml

- compress all pubmed xml from directory pubmed_xml/ into 1.3G .tar.gz for faster upload/download (8.5 minutes on dev.wormbase, 1 hour 37 minutes on agr-literature-dev)
- % tar zvcf alliance_pubmed_xml.tar.gz pubmed_xml/
- copy tar.gz of all alliance pubmed xml from local computer to s3 (20 seconds to upload 1.3G file)
- % aws s3 cp alliance_pubmed_xml.tar.gz s3://agr-literature/develop/reference/metadata/pubmed/xml_compressed/

#### pubmed json

- compress all pubmed json from directory pubmed_json/ into 629M .tar.gz for faster upload/download (~5 minutes)
- % tar zvcf alliance_pubmed_json.tar.gz pubmed_json/
- upload to s3 (8 seconds)
- % aws s3 cp alliance_pubmed_json.tar.gz s3://agr-literature/develop/reference/metadata/pubmed/json_from_xml_compressed/

#### sanitized references json

- compress sanitized reference json then cp to s3
- % tar zvcf alliance_sanitized_reference_json.tar.gz sanitized_reference_json/
- % aws s3 cp  alliance_sanitized_reference_json.tar.gz s3://agr-literature/develop/reference/metadata/sanitized_for_ingest/

#### sanitized resources json

- compress sanitized resource json then cp to s3
- % tar cvfz alliance_sanitized_resource_json.tar.gz sanitized_resource_json/
- % aws s3 cp  alliance_sanitized_resource_json.tar.gz s3://agr-literature/develop/resource/metadata/sanitized_for_ingest/


## Short description of scripts

- `generate_pubmed_nlm_resource.py` download pubmed resouce data, generate json and upload to s3
- `get_dqm_data.py` download dqm data if it's got a new md5sum
- `parse_dqm_json_reference.py` process dqm data to generate list of PMIDs to download
- `generate_dqm_json_test_set.py` optionally generate a small sample set from DQM MOD data
- `get_pubmed_xml.py` download pubmed xml from list to local directory
- `sort_not_found_pmids_by_mod.py` optionally take list of pmids not found and sort by mods
- `xml_to_json.py` process pubmed xml into json to local directory

## Downloading PubMed XML

### Download xml from an input file in the file system
```bash
pipenv run python src/xml_processing/get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set
```

### Download xml from an input file at a URL
```bash
pipenv run python src/xml_processing/get_pubmed_xml.py -u http://tazendra.caltech.edu/~azurebrd/var/work/pmid_sample
```

### Download xml from commandline flags
```bash
pipenv run python src/xml_processing/get_pubmed_xml.py -c 12345678 4576890 18280033
```

### Download xml from a database-based job (stub)
```bash
pipenv run python src/xml_processing/get_pubmed_xml.py -d
```

### Download xml from a REST API-based job (stub)
```bash
pipenv run python src/xml_processing/get_pubmed_xml.py -r
```

### Download xml from a hardcoded sample
```bash
pipenv run python src/xml_processing/get_pubmed_xml.py -s
```

## Convert PubMed XML to JSON

### Has same flags as get_pubmed_xml.py, e.g.
```bash
pipenv run python src/xml_processing/xml_to_json.py  -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set
```

## Generate resource JSON from J_Medline.txt
- To use pubmed url file -u flag<br/>
- To use local file use -l flag<br/>
- To upload to aws s3 agr-literature/develop/resource/metadata/resource_pubmed_all.json use -s flag<br/>

### Generate from URL at https://ftp.ncbi.nih.gov/pubmed/J_Medline.txt (default if no flag), and upload to s3 bucket
```bash
pipenv run python src/xml_processing/generate_pubmed_nlm_resource.py -u -s
```

### Generate from local J_Medline.txt file and no upload
```bash
pipenv run python src/xml_processing/generate_pubmed_nlm_resource.py -l
```

## Download DQM MOD data from FMS
- get_dqm_data.py downloads DQM MOD JSON from FMS and uncompresses.  compares md5sum to current file to prevent downloading if it's the same
```bash
pipenv run python src/xml_processing/get_dqm_data.py
```

## Generate testing sample of DQM MOD data
- generate_dqm_json_test_set.py generates a small sample set from DQM MOD JSON in dqm_data/ and outputs to dqm_sample/
```bash
pipenv run python src/xml_processing/generate_dqm_json_test_set.py
```

## Parse DQM data to generate lists of PMIDs
- Parse DQM data to generate list of PMIDs that will need XML downloaded, and generate mapping of PMIDs and MODs that have them
```bash
pipenv run python parse_dqm_json_reference.py -p
```

## Parse DQM and PubMed reference data, generate validated JSON for REST API ingest
- Aggregate and validate DQM data against agr_schemas's reference.json, resolve if PubMed id or PubMod id, create sanitized JSON for REST API
- Using sample set from generate_dqm_json_test_set.py
```bash
pipenv run python parse_dqm_json_reference.py -f dqm_sample -m WB
```
- Using full dqm data from get_dqm_data.py
```bash
pipenv run python parse_dqm_json_reference.py -f dqm_data -m all
```

## Parse DQM and PubMed resource data, generate validated JSON for REST API ingest
- Aggregate and validate DQM resource data against processed J_Medline.txt values, and create sanitized JSON for REST API
```bash
pipenv run python parse_dqm_json_resource.py
```

## Sort PMIDs without XML by MOD
- sort_not_found_pmids_by_mod.py takes pmids_not_found from get_pubmed_xml.py, and pmids_by_mods from parse_dqm_json_reference.py, and generates a set sorted by MODs of pmids that were not found in pubmed.
```bash
pipenv run python src/xml_processing/sort_not_found_pmids_by_mod.py
```

## (Optional) Query PubMed for DQM resourceAbbreviations not matched in J_Medline.txt
- get_pubmed_nlm_resource_unmatched.py takes resourceAbbreviations not matched in J_Medline.txt from resource_xml/resource_abbreviation_not_matched, queries pubmed for their xml and stores in a simplified filename at resource_xml/
```bash
pipenv run python src/xml_processing/get_pubmed_nlm_resource_unmatched.py
```

# Downloading tar.gz from Pubmed Processing

## Download tar.gz to pubmed_tgz/
- take list of pmids as input
- download from pubmed oa_file_list.txt with mapping of pmids to their ftp location
- download if it is in desired pmid list, download to pubmed_tgz/
```bash
pipenv run python get_pubmed_tgz.py -f inputs/alliance_pmids
```

## (Optional) Chunk into directories with 10000 files and upload to s3
- see chunking_pmids/README
- upload files to s3://agr-literature/develop/reference/documents/pubmed/tarball_chunks/
```bash
pipenv run python generate_chunk_files.py
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
