ARG REG
ARG ALLIANCE_RELEASE

FROM python:3.11-alpine3.20

RUN apk update && apk add --no-cache git gcc musl-dev postgresql-dev libffi-dev

WORKDIR /usr/local/bin/src/literature
RUN mkdir -p /usr/local/bin/src/literature/agr_literature_service
ADD ./agr_literature_service /usr/local/bin/src/literature/agr_literature_service
COPY ./requirements.txt .
COPY ./logging.conf .
RUN pip3 install --upgrade pip
RUN pip3 install wheel
RUN pip3 install "Cython<3.0" pyyaml --no-build-isolation

RUN pip3 install -r requirements.txt

CMD ["python3", "agr_literature_service/api/main.py", "--port=8080"]