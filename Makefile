REG=100225593120.dkr.ecr.us-east-1.amazonaws.com
TAG=latest

build-env:
	docker build . --build-arg REG=${REG} -t ${REG}/agr_literature_env:${TAG} -f ./docker/Dockerfile.env

build-dev:
	docker build . --build-arg REG=${REG} -t ${REG}/agr_literature_dev:${TAG} -f ./docker/Dockfile.dev
