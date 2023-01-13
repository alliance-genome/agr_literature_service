ARG REG=agrdocker
ARG ALLIANCE_RELEASE=latest

FROM python:3.8-alpine3.16

RUN apk update && apk add --no-cache git gcc musl-dev postgresql-dev libffi-dev

WORKDIR /usr/local/bin/src/literature

ADD . .

RUN pip3 install -r requirements.dev.txt
RUN pip3 install -r requirements.txt

CMD ["python3", "agr_literature_service/api/main.py", "--port=8080"]

