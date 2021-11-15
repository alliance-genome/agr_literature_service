REG=100225593120.dkr.ecr.us-east-1.amazonaws.com
TAG=latest

login-ecr:
	docker run -v ~/.aws/credentials:/root/.aws/credentials --rm -it amazon/aws-cli ecr get-login-password | docker login --username AWS --password-stdin ${REG}

build-env:
	docker build . \
		--build-arg REG=${REG} \
		--build-arg aws_secret_access_key=${AWS_SECRET_ACCESS_KEY} \
		--build-arg aws_access_key_id=${AWS_ACCESS_KEY_ID} \
		--build-arg okta_client_id=${OKTA_CLIENT_ID} \
		--build-arg okta_client_secret=${OKTA_CLIENT_SECRET} \
		-t ${REG}/agr_literature_env:${TAG} \
		-f ./docker/Dockerfile.env

build-dev:
	docker build . --build-arg REG=${REG} -t ${REG}/agr_literature_dev:${TAG} -f ./docker/Dockerfile.dev.env --progress=plain

build-app:
	docker build . --build-arg REG=${REG} -t ${REG}/agr_literature_app:${TAG} -f ./docker/Dockerfile.app.env

run-flake8:
	docker run --rm -v ${PWD}:/workdir -i ${REG}/agr_literature_dev:${TAG} /bin/bash -c "python3 -m flake8 ."

run-local-flake8:
	python3 -m flake8 .

run-mypy:
	docker run --rm -v ${PWD}:/workdir -i ${REG}/agr_literature_dev:${TAG} /bin/bash -c "mypy --config-file mypy.config ."

run-local-mypy:
	mypy --config-file mypy.config .

run-dev-bash:
	docker run --rm \
	    --network=agr_literature_service_agr-literature \
	    -p ${API_PORT}:8080 \
	    -v ${PWD}:/workdir \
		-t -i ${REG}/agr_literature_dev:${TAG} \
		/bin/bash

run-dev-zsh:
	docker run --rm -v "${HOME}/.vimrc:/root/.vimrc:rw" -v "${HOME}/.zshrc:/root/.zshrc:rw" -v ${PWD}:/workdir -t -i ${REG}/agr_literature_dev:${TAG} /bin/zsh

docker-compose-up:
	docker run -itd --env-file=.env -v /var/run/docker.sock:/var/run/docker.sock -v /home/core/.docker:/root/.docker -v ${PWD}:/var/tmp/ docker/compose:1.24.1  -f /var/tmp/docker-compose.yaml up -d

docker-compose-down:
	docker run -itd --env-file=.env -v /var/run/docker.sock:/var/run/docker.sock -v /home/core/.docker:/root/.docker -v ${PWD}:/var/tmp/ docker/compose:1.24.1  -f /var/tmp/docker-compose.yaml down 

run-test-bash: build-env build-dev
	-docker volume rm agr_literature_service_agr-literature-test-pg-data    
	docker-compose -f docker-compose-test.yml up -d
	sleep 5
	# Minus at start means ignore exit code for that line
	-docker run -it --rm \
		--network=agr_literature_service_agr-literature-test \
	    -p 8080:8080 \
	    -v ${PWD}:/workdir \
		${REG}/agr_literature_dev:${TAG} \
		./run_tests.sh
	docker-compose -f docker-compose-test.yml down

