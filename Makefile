REG=100225593120.dkr.ecr.us-east-1.amazonaws.com
TAG=latest

login-ecr:
	docker run --rm -it amazon/aws-cli ecr get-login-password | docker login --username AWS --password-stdin ${REG}

build-env:
	docker build . --build-arg REG=${REG} -t ${REG}/agr_literature_env:${TAG} -f ./docker/Dockerfile.env

build-dev:
	docker build . --build-arg REG=${REG} -t ${REG}/agr_literature_dev:${TAG} -f ./docker/Dockerfile.dev.env

build-app:
	docker build . --build-arg REG=${REG} -t ${REG}/agr_literature_app:${TAG} -f ./docker/Dockerfile.app.env

run-flake8:
	docker run -v ${PWD}:/workdir -i ${REG}/agr_literature_dev:${TAG} /bin/bash -c "python3 -m flake8 ."

run-dev-bash:
	docker run -v ${PWD}:/workdir -t -i ${REG}/agr_literature_dev:${TAG} /bin/bash

start-docker-compose:
	docker run -itd -v /var/run/docker.sock:/var/run/docker.sock -v ${PWD}:/var/tmp/ docker/compose:1.24.1  -f /var/tmp/docker-compose.yaml up -d
