FROM ubuntu:20.04

RUN apt-get update && apt-get install -y cron git

WORKDIR /usr/src/app/

ADD . .
ADD crontab /etc/cron.d/automate_scripts_crontab

RUN pip3 install -r requirements.txt

RUN chmod 0644 /etc/cron.d/automate_scripts_crontab
RUN crontab /etc/cron.d/automate_scripts_crontab

CMD /bin/bash -c 'declare -p' | grep -Ev 'BASHOPTS|BASH_VERSINFO|EUID|PPID|SHELLOPTS|UID' > /container.env && cron && tail -f /dev/null