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
	docker run --rm -v ${PWD}:/workdir -i ${REG}/agr_literature_dev:${TAG} /bin/bash -c "python3 -m flake8 ."

run-dev-bash:
	docker run --network=agr_literature_service_demo_agr-literature -v ${PWD}:/workdir -t -i ${REG}/agr_literature_dev:${TAG} /bin/bash

run-dev-zsh:
	docker run --rm -v "${HOME}/.vimrc:/root/.vimrc:rw" -v "${HOME}/.zshrc:/root/.zshrc:rw" -v ${PWD}:/workdir -t -i ${REG}/agr_literature_dev:${TAG} /bin/zsh

start-docker-compose:
	docker run -itd -v /var/run/docker.sock:/var/run/docker.sock -v ${PWD}:/var/tmp/ docker/compose:1.24.1  -f /var/tmp/docker-compose.yaml up -d
