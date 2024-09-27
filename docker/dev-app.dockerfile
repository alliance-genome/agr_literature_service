ARG REG=agrdocker
ARG ALLIANCE_RELEASE=latest

FROM python:3.11-bookworm

WORKDIR /usr/local/bin/src/literature
RUN mkdir -p /usr/local/bin/src/literature/agr_literature_service
ADD ./agr_literature_service /usr/local/bin/src/literature/agr_literature_service
COPY ./requirements.txt .
COPY ./requirements.dev.txt .

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y python3-pip git postgresql-client wget && \
    pip install -r requirements.txt &&  \
    pip install -r requirements.dev.txt &&  \
    apt-get -y install zsh && \
    apt-get update -y && \
    apt-get -y install gnupg2 vim emacs-nox && \
    wget -O ~/libreadline7.deb 'http://mirrors.kernel.org/ubuntu/pool/main/r/readline/libreadline7_7.0-3_amd64.deb' && \
    apt-get -y install ~/libreadline7.deb && \
    rm -f ~/libreadline7.deb && \
    apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install lsb-release -y && \
    apt-get clean all  && \
    sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/postgres.list'  && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc |  apt-key add -  && \
    apt update  && \
    apt install bash -y   && \
    apt install postgresql-client-13 -y
    # TODO: cleanup apt-cache

CMD ["bash"]