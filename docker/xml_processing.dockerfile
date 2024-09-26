ARG REG=agrdocker
ARG ALLIANCE_RELEASE=latest

FROM python:3.11-alpine3.20

RUN apk update && apk add --no-cache git gcc musl-dev postgresql-dev libffi-dev

WORKDIR /usr/local/bin/src/literature
RUN mkdir -p /usr/local/bin/src/literature/agr_literature_service
ADD ./agr_literature_service /usr/local/bin/src/literature/agr_literature_service
COPY ./requirements.txt .
COPY ./logging.conf .
RUN apk update && apk add bash
RUN pip3 install -r requirements.txt
CMD ["bash"]