ARG REG=agrdocker
ARG ALLIANCE_RELEASE=latest

FROM ${REG}/agr_base_linux_env:${ALLIANCE_RELEASE}

WORKDIR /usr/local/bin/src/literature

ADD . .

RUN pip3 install -r backend/app/requirements.dev.txt
RUN pip3 install -r backend/app/requirements.txt

CMD ["python3", "backend/app/main.py", "--port=8080"]

