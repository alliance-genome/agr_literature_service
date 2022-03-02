ARG REG=agrdocker
ARG ALLIANCE_RELEASE=latest

FROM ${REG}/agr_base_linux_env:${ALLIANCE_RELEASE}

WORKDIR /usr/local/bin/src/literature

ADD . .
RUN apt update
RUN apt install graphviz -y

RUN pip3 install -r backend/app/requirements.txt

CMD ["bash"]