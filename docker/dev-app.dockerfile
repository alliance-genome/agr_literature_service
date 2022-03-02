FROM ${REG}/agr_literature_env:${ALLIANCE_RELEASE}

ADD backend/app/requirements.dev.txt .

RUN pip3 install -r requirements.dev.txt

RUN apt-get -y install zsh


#install posgres client
RUN apt-get update -y
RUN apt-get -y install gnupg2
RUN wget -O ~/libreadline7.deb 'http://mirrors.kernel.org/ubuntu/pool/main/r/readline/libreadline7_7.0-3_amd64.deb'
RUN apt-get -y install ~/libreadline7.deb
RUN rm -f ~/libreadline7.deb
ENV APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=DontWarn
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ bionic-pgdg main" | tee /etc/apt/sources.list.d/pgdg.list
RUN apt-get update
RUN apt-get -y install postgresql-client-13

CMD ["bash"]