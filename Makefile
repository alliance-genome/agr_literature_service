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
	docker build . --build-arg REG=${REG} -t ${REG}/agr_literature_dev:${TAG} -f ./docker/Dockerfile.dev.env

build-app:
	docker build . --build-arg REG=${REG} -t ${REG}/agr_literature_app:${TAG} -f ./docker/Dockerfile.app.env

run-flake8:
	docker run --rm -v ${PWD}:/workdir -i ${REG}/agr_literature_dev:${TAG} /bin/bash -c "python3 -m flake8 ."

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

run-test-bash:
	docker run -it --rm \
		--network=agr_literature_service_agr-literature \
	    -p ${API_PORT}:8080 \
	    -v ${PWD}:/workdir \
		${REG}/agr_literature_dev:${TAG} \
		./run_tests.sh