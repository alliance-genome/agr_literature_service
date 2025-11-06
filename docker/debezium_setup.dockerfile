FROM alpine:3.14

RUN apk add --no-cache bash curl gettext postgresql-client jq

COPY debezium/setup.sh /setup.sh
COPY debezium/status_manager.sh /status_manager.sh
COPY debezium/*.json /
COPY debezium/*.ksql /

RUN chmod +x /setup.sh /status_manager.sh

CMD ["bash", "/setup.sh"]

