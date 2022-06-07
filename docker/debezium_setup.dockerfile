FROM alpine:3.14

RUN apk add --no-cache bash curl gettext

COPY debezium/* /

CMD ["bash", "/setup.sh"]

