ARG REG
ARG ALLIANCE_RELEASE

FROM python:3.8-alpine3.16

RUN apk update && apk add --no-cache git gcc musl-dev postgresql-dev libffi-dev

WORKDIR /usr/local/bin/src/literature

ADD . .

RUN pip3 install --upgrade pip
RUN pip3 install wheel
RUN pip3 install "Cython<3.0" pyyaml --no-build-isolation

RUN pip3 install -r requirements.txt

CMD ["python3", "agr_literature_service/api/main.py", "--port=8080"]