REG=100225593120.dkr.ecr.us-east-1.amazonaws.com
TAG=latest

ifdef ENV_FILE
	include ${ENV_FILE}
else
	ENV_FILE=.env
endif

ifndef ALEMBIC_COMMENT
	ALEMBIC_COMMENT=""
endif


login-ecr:
	docker run -v ~/.aws/credentials:/root/.aws/credentials --rm -it amazon/aws-cli ecr get-login-password | docker login --username AWS --password-stdin ${REG}

build:
	docker-compose build

run-flake8:
	docker-compose --env-file .env.test down -v
	docker-compose --env-file .env.test run -v ${PWD}:/workdir test_runner /bin/bash -c "python3 -m flake8 agr_literature_service"
	docker-compose --env-file .env.test down -v

run-local-flake8:
	python3 -m flake8 .

run-mypy:
	docker-compose --env-file .env.test down -v
	docker-compose --env-file .env.test run -v ${PWD}:/workdir test_runner /bin/bash -c "mypy --config-file mypy.config agr_literature_service"
	docker-compose --env-file .env.test down -v

run-local-mypy:
	mypy --config-file mypy.config .

run-dev-bash:
	docker-compose --env-file .env.test up -d postgres
	docker-compose --env-file .env.test run --service-ports --rm dev_app /bin/bash

run-dev-zsh:
	docker-compose --env-file .env.test up -d postgres
	docker-compose --env-file .env.test run --service-ports --rm -v "${HOME}/.vimrc:/root/.vimrc:rw" -v "${HOME}/.zshrc:/root/.zshrc:rw" -v ${PWD}:/workdir dev_app /bin/zsh

docker-compose-up:
	docker run -itd --env-file=.env -v /var/run/docker.sock:/var/run/docker.sock -v /home/core/.docker:/root/.docker -v ${PWD}:/var/tmp/ docker/compose:1.24.1  -f /var/tmp/docker-compose.yaml up -d

docker-compose-down:
	docker run -itd --env-file=.env -v /var/run/docker.sock:/var/run/docker.sock -v /home/core/.docker:/root/.docker -v ${PWD}:/var/tmp/ docker/compose:1.24.1  -f /var/tmp/docker-compose.yaml down 

run-test-bash:
	docker-compose --env-file .env.test down -v
	docker-compose --env-file .env.test up -d postgres
	sleep 5
	docker-compose --env-file .env.test up -d elasticsearch
	docker-compose --env-file .env.test up -d --build api
	sleep 10
	docker-compose --env-file .env.test build test_runner
	-docker-compose --env-file .env.test run -v ${PWD}:/workdir test_runner ./run_tests.sh > pytest.out
    #doing here after shutdown of database
	python3 check_tests.py
	docker-compose --env-file .env.test down -v

run-functest:
	docker-compose --env-file .env.test down -v
	docker-compose --env-file .env.test up -d postgres
	sleep 5
	docker-compose --env-file .env.test up -d api
	docker logs -f agr-literature-test_api_1 > outfile.log &
	sleep 5

	# load the mods
	docker-compose --env-file .env.test run test_runner python3 agr_literature_service/lit_processing/mod_populate_load.py

	# load the data
	docker-compose --env-file .env.test run test_runner /bin/bash agr_literature_service/lit_processing/sample_reference_populate_load.sh

	# load the update
	docker-compose --env-file .env.test run test_runner /bin/bash agr_literature_service/lit_processing/sample_reference_populate_update.sh

	docker-compose --env-file .env.test run test_runner python3 agr_literature_service/lit_processing/tests/functional_tests.py
	docker-compose --env-file .env.test down

start-debezium-local:
	docker-compose --env-file ${ENV_FILE} up -d postgres
	sleep 5
	docker-compose --env-file ${ENV_FILE} up -d elasticsearch
	sleep 10
	docker-compose --env-file ${ENV_FILE} up -d dbz_zookeeper dbz_kafka dbz_connector
	sleep 10
	docker-compose --env-file ${ENV_FILE} up -d dbz_ksql_server
	sleep 20
	docker-compose --env-file ${ENV_FILE} up -d dbz_setup

start-debezium-aws:
	docker-compose --env-file ${ENV_FILE} up -d dbz_zookeeper dbz_kafka dbz_connector
	sleep 10
	docker-compose --env-file ${ENV_FILE} up -d dbz_ksql_server
	sleep 20
	docker-compose --env-file ${ENV_FILE} up -d dbz_setup

restart-api:
	docker-compose --env-file ${ENV_FILE} build --no-cache api
	docker-compose --env-file ${ENV_FILE} rm -s -f api
	docker-compose --env-file ${ENV_FILE} up -d api

alembic-create-migration:
	docker-compose --env-file ${ENV_FILE} run --service-ports --rm dev_app alembic revision --autogenerate -m "${ALEMBIC_COMMENT}"
	docker-compose --env-file ${ENV_FILE} run --service-ports --rm dev_app bash -c "chmod -R o+w alembic/versions"

alembic-apply-latest-migration:
	docker-compose --env-file ${ENV_FILE} run --service-ports --rm dev_app alembic upgrade head

