#!/bin/bash
set -e

REG=100225593120.dkr.ecr.us-east-1.amazonaws.com
TAG=latest

echo "Building environment and dev..."
make build-env build-dev
docker volume rm agr_literature_service_agr-literature-test-pg-data
docker-compose -f docker-compose-test.yml up -d
sleep  5
echo "Done"

# Unset exit on error so we guarantee to shutdown docker-compose
set +e
test_cmd="export ENV_STATE=test; pytest --cov --cov-fail-under=80 -vv --cov-report html $@"
echo "Running \`$test_cmd\`"
docker run -it --rm \
		--network=agr_literature_service_agr-literature-test \
	    -p 8080:8080 \
	    -v $PWD:/workdir \
		$REG/agr_literature_dev:$TAG \
		/bin/bash -c "export ENV_STATE=test; pytest $@"

echo "Stopping test env"
docker-compose -f docker-compose-test.yml down
