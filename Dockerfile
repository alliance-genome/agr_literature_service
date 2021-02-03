ARG ALLIANCE_RELEASE=latest
ARG REG=agrdocker
FROM ${REG}/agr_base_linux_env:${ALLIANCE_RELEASE}

RUN python3 -m pip install -U pip
RUN python3 -m pip install -U setuptools

RUN pip install pipenv

ENV PROJECT_DIR /usr/local/bin/src/literature

WORKDIR ${PROJECT_DIR}

COPY Pipfile ${PROJECT_DIR}/

#COPY Pipfile ${PROJECT_DIR}
RUN cd ${PROJECT_DIR} && pipenv lock --keep-outdated --requirements > requirements.txt
RUN pip install -r ${PROJECT_DIR}/requirements.txt


#RUN pipenv install

ADD . .

CMD ["python3", "src/main.py", "--port=8080", "--prod"]
