ARG REG=agrdocker
ARG ALLIANCE_RELEASE=latest

FROM ubuntu:20.04

WORKDIR /usr/local/bin/src/literature

ADD . .

RUN pip3 install -r requirements.txt &&  \
    pip3 install -r requirements.dev.txt &&  \
    apt-get -y install zsh && \
    apt-get update -y && \
    apt-get -y install gnupg2 vim emacs-nox && \
    wget -O ~/libreadline7.deb 'http://mirrors.kernel.org/ubuntu/pool/main/r/readline/libreadline7_7.0-3_amd64.deb' && \
    apt-get -y install ~/libreadline7.deb && \
    rm -f ~/libreadline7.deb &&  \
    apt-get -y install postgresql-client &&  \
    # TODO: cleanup apt-cache

CMD ["bash"]