ARG REG
ARG ALLIANCE_RELEASE

FROM python:3.11-alpine3.20

RUN apk update && apk add --no-cache git gcc musl-dev postgresql-dev libffi-dev

WORKDIR /usr/local/bin/src/literature
RUN mkdir -p /usr/local/bin/src/literature/agr_literature_service
ADD ./agr_literature_service /usr/local/bin/src/literature/agr_literature_service
COPY ./requirements.txt .
COPY ./logging.conf .
COPY ./gunicorn.conf.py .
COPY ./uvicorn_worker.py .
RUN pip3 install --upgrade pip
RUN pip3 install wheel
RUN pip3 install "Cython<3.0" pyyaml --no-build-isolation

RUN pip3 install -r requirements.txt
RUN pip3 install .

# Use gunicorn with uvicorn workers for multi-process handling
CMD ["gunicorn", "agr_literature_service.api.main:app", "-c", "gunicorn.conf.py"]