ARG REG=agrdocker
ARG ALLIANCE_RELEASE=latest

FROM ${REG}/agr_literature_env:${ALLIANCE_RELEASE}

WORKDIR /usr/local/bin/src/literature

ADD . .

RUN pip3 install -r backend/app/requirements.dev.txt

CMD ["bash"]

