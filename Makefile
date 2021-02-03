REG=100225593120.dkr.ecr.us-east-1.amazonaws.com
TAG=latest

build:
	docker build --build-arg REG=${REG} -t ${REG}/agr_literature_run:${TAG} .
