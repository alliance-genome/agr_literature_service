ARG REG=agrdocker
ARG ALLIANCE_RELEASE=latest

FROM ${REG}/agr_base_linux_env:${ALLIANCE_RELEASE}

WORKDIR /usr/local/bin/src/literature

ADD requirements.txt .
ADD requirements.dev.txt .

ADD . .

RUN pip3 install -r requirements.txt
RUN pip3 install -r requirements.dev.txt

RUN apt-get -y install zsh


#install posgres client
RUN apt-get update -y
RUN apt-get -y install gnupg2 vim emacs-nox
RUN wget -O ~/libreadline7.deb 'http://mirrors.kernel.org/ubuntu/pool/main/r/readline/libreadline7_7.0-3_amd64.deb'
RUN apt-get -y install ~/libreadline7.deb
RUN rm -f ~/libreadline7.deb
RUN apt-get -y install postgresql-client

CMD ["bash"]