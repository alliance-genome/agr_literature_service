REG=100225593120.dkr.ecr.us-east-1.amazonaws.com
TAG=latest

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
