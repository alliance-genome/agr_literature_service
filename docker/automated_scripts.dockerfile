FROM python:3.11-bookworm
# Set timezone:
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get update && apt-get install -y cron git
WORKDIR /usr/src/app/
RUN mkdir -p /usr/app/agr_literature_service
ADD ./agr_literature_service /usr/src/app/agr_literature_service
COPY ./requirements.txt .
COPY ./logging.conf .

ADD crontab /etc/cron.d/automate_scripts_crontab

RUN pip3 install -r requirements.txt  && \
    apt-get update -y  && \
    apt-get upgrade -y  && \
    apt-get install lsb-release -y  && \
    apt-get install wget -y  && \
    apt-get clean all  && \
    sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/postgres.list'  && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc |  apt-key add -  && \
    apt update  && \
    apt install postgresql-client-13 -y && \
    apt install curl -y && \
    apt install gettext -y   && \
    apt install bash -y   && \
    apt install jq -y
COPY debezium/* /
RUN chmod 0644 /etc/cron.d/automate_scripts_crontab
RUN crontab /etc/cron.d/automate_scripts_crontab

CMD /bin/bash -c 'declare -p' | grep -Ev 'BASHOPTS|BASH_VERSINFO|EUID|PPID|SHELLOPTS|UID' > /container.env && cron && tail -f /dev/null