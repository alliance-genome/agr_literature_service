ARG ALLIANCE_RELEASE=latest
ARG REG=agrdocker
FROM ${REG}/agr_literature_env:${ALLIANCE_RELEASE}

ENV PROJECT_DIR /usr/local/bin/src/literature

WORKDIR ${PROJECT_DIR}

ADD . .

CMD ["python3", "src/main.py", "--port=8080"]
