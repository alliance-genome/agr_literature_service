#FROM python:3.11-bookworm
FROM python:3.11-slim
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

# Install system dependencies and tools
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    lsb-release \
    wget \
    curl \
    gettext \
    bash \
    jq \
    gnupg \
    postgresql-client && \
    apt-get clean

# Set up PostgreSQL repository
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/postgres.list' && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | tee /etc/apt/trusted.gpg.d/postgresql.asc > /dev/null && \
    apt-get update -y

# Copy requirements and install Python dependencies
RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt --no-cache-dir

COPY debezium/* /
RUN chmod 0644 /etc/cron.d/automate_scripts_crontab
RUN crontab /etc/cron.d/automate_scripts_crontab

CMD /bin/bash -c 'declare -p' | grep -Ev 'BASHOPTS|BASH_VERSINFO|EUID|PPID|SHELLOPTS|UID' > /container.env && cron && tail -f /dev/null