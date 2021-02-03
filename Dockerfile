ARG ALLIANCE_RELEASE=latest
ARG REG=agrdocker
FROM ${REG}/agr_base_linux_env:${ALLIANCE_RELEASE}

RUN python3 -m pip install -U pip
RUN python3 -m pip install -U setuptools

RUN pip install pipenv

ENV PROJECT_DIR /usr/local/bin/src/literature

WORKDIR ${PROJECT_DIR}

COPY Pipfile ${PROJECT_DIR}/

RUN pipenv install --deploy

ADD . .

CMD ["pipenv", "run", "python", "src/main.py", "--port=5000"]
