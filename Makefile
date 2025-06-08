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
	docker compose build

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
	docker-compose --env-file .env.test down
	docker-compose --env-file .env.test rm -svf elasticsearch
	docker-compose --env-file .env.test up -d postgres
	sleep 5
	docker compose --env-file .env.test build test_runner
	docker compose --env-file .env.test build dev_app
	docker compose --env-file .env.test run --rm dev_app sh tests/init_test_db.sh
	docker compose --env-file .env.test up -d elasticsearch
	sleep 10
	-docker-compose --env-file .env.test run -v ${PWD}:/workdir test_runner ./run_tests.sh > pytest.out
    #doing here after shutdown of database
	python3 check_tests.py
	docker compose --env-file .env.test down

run-functest:
	docker compose --env-file .env.test down
	docker compose --env-file .env.test up -d postgres
	sleep 5
	docker compose --env-file .env.test build test_runner
	docker compose --env-file .env.test build dev_app
	docker compose --env-file .env.test run --rm dev_app sh tests/init_test_db.sh
	docker compose --env-file .env.test run test_runner python3 agr_literature_service/lit_processing/tests/functional_tests.py
	docker compose --env-file .env.test down

restart-debezium-local:
	docker-compose --env-file ${ENV_FILE} rm -svf dbz_connector dbz_kafka dbz_zookeeper dbz_ksql_server
	docker-compose --env-file ${ENV_FILE} up -d postgres
	sleep 5
	docker-compose --env-file ${ENV_FILE} up -d elasticsearch
	sleep 10
	docker-compose --env-file ${ENV_FILE} up -d dbz_zookeeper dbz_kafka dbz_connector
	sleep 10
	docker-compose --env-file ${ENV_FILE} exec dbz_connector bash -c "/kafka/bin/kafka-topics.sh --create --topic 'abc.public.obsolete_reference_curie' --partitions 1 --replication-factor 1 --bootstrap-server dbz_kafka:9092"
	sleep 5
	docker-compose --env-file ${ENV_FILE} up -d dbz_ksql_server
	sleep 20
	docker-compose --env-file ${ENV_FILE} up -d --build dbz_setup

restart-debezium-aws:
	docker-compose --env-file ${ENV_FILE} rm -svf dbz_connector dbz_kafka dbz_zookeeper dbz_ksql_server dbz_setup
	docker-compose --env-file ${ENV_FILE} up -d dbz_zookeeper dbz_kafka dbz_connector
	sleep 10
	docker-compose --env-file ${ENV_FILE} exec dbz_connector bash -c "/kafka/bin/kafka-topics.sh --create --topic 'abc.public.obsolete_reference_curie' --partitions 1 --replication-factor 1 --bootstrap-server dbz_kafka:9092"
	sleep 5
	docker-compose --env-file ${ENV_FILE} up -d dbz_ksql_server
	sleep 20
	docker-compose --env-file ${ENV_FILE} up -d --build dbz_setup

stop-debezium:
	docker-compose --env-file ${ENV_FILE} rm -svf dbz_connector dbz_kafka dbz_zookeeper dbz_ksql_server dbz_setup

restart-api:
	docker-compose --env-file ${ENV_FILE} build --no-cache api
	docker-compose --env-file ${ENV_FILE} rm -s -f api
	docker-compose --env-file ${ENV_FILE} up -d api

restart-api-and-automated-scripts:
	docker-compose --env-file ${ENV_FILE} build --no-cache api
	docker-compose --env-file ${ENV_FILE} rm -s -f api
	docker-compose --env-file ${ENV_FILE} up -d api
	docker-compose --env-file ${ENV_FILE} build --no-cache automated_scripts
	docker-compose --env-file ${ENV_FILE} rm -s -f automated_scripts
	docker-compose --env-file ${ENV_FILE} up --build -d automated_scripts

alembic-create-migration:
	docker-compose --env-file ${ENV_FILE} run --service-ports --rm dev_app alembic revision --autogenerate -m "${ALEMBIC_COMMENT}"
	docker-compose --env-file ${ENV_FILE} run --service-ports --rm dev_app bash -c "chmod -R o+w alembic/versions"

alembic-apply-latest-migration:
	docker-compose --env-file ${ENV_FILE} rm -svf dbz_connector dbz_kafka dbz_zookeeper dbz_ksql_server dbz_setup
	docker-compose --env-file ${ENV_FILE} rm -s -f api
	docker-compose --env-file ${ENV_FILE} build dev_app
	docker-compose --env-file ${ENV_FILE} run --service-ports --rm dev_app alembic upgrade head
	$(MAKE) ENV_FILE=${ENV_FILE} restart-api-and-automated-scripts
	$(MAKE) ENV_FILE=${ENV_FILE} restart-debezium-aws

restart-automated-scripts:
	docker-compose --env-file ${ENV_FILE} rm -s -f automated_scripts
	docker-compose --env-file ${ENV_FILE} up --build -d automated_scripts

bulk_upload_reference_files:
	docker-compose --env-file ${ENV_FILE} run --rm -v $(local_folder):/usr/files_to_upload/ file_uploader upload_files $(mod_abbreviation)

dump_prod_locally:
	docker-compose --env-file ${ENV_FILE} run --service-ports --rm dev_app python3 agr_literature_service/lit_processing/data_export/dump_database.py  -t  ondemand

run-debezium-integration-tests:
	@echo "Starting Debezium integration tests..."
	# Clean up any existing containers
	docker-compose --env-file .env.test down -v
	docker-compose --env-file .env.test rm -svf elasticsearch dbz_connector dbz_kafka dbz_zookeeper dbz_ksql_server dbz_setup
	# Start required services
	docker-compose --env-file .env.test up -d postgres
	sleep 10
	docker-compose --env-file .env.test build test_runner
	docker-compose --env-file .env.test build dev_app
	# Initialize test database with mock data
	docker-compose --env-file .env.test run --rm dev_app sh tests/init_test_db.sh
	docker-compose --env-file .env.test run --rm dev_app python3 tests/populate_test_db.py
	# Start Elasticsearch
	docker-compose --env-file .env.test up -d elasticsearch
	sleep 15
	# Start Debezium stack
	docker-compose --env-file .env.test up -d dbz_zookeeper dbz_kafka dbz_connector
	sleep 15
	# Create required Kafka topics
	docker-compose --env-file .env.test exec dbz_connector bash -c "/kafka/bin/kafka-topics.sh --create --topic 'abc.public.obsolete_reference_curie' --partitions 1 --replication-factor 1 --bootstrap-server dbz_kafka:9092" || true
	sleep 5
	# Start KSQL server
	docker-compose --env-file .env.test up -d dbz_ksql_server
	sleep 20
	# Run Debezium setup
	docker-compose --env-file .env.test up -d --build dbz_setup
	sleep 30
	# Wait for data to be indexed
	@echo "Waiting for Debezium pipeline to process data..."
	sleep 60
	# Run the actual integration tests
	#docker-compose --env-file .env.test run --rm test_runner python3 -m pytest tests/test_debezium_integration.py -v -m "debezium"
	# Cleanup
	#docker-compose --env-file .env.test down
