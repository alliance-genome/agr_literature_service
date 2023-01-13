ARG REG
ARG ALLIANCE_RELEASE

FROM python:3.8-alpine3.16

RUN apk update && apk add --no-cache git gcc musl-dev postgresql-dev libffi-dev

WORKDIR /usr/src/app/

ADD . .
ADD crontab /etc/cron.d/automate_scripts_crontab

RUN pip3 install -r requirements.txt

RUN chmod 0644 /etc/cron.d/automate_scripts_crontab
RUN crontab /etc/cron.d/automate_scripts_crontab

CMD /bin/bash -c 'declare -p' | grep -Ev 'BASHOPTS|BASH_VERSINFO|EUID|PPID|SHELLOPTS|UID' > /container.env && cron && tail -f /dev/null