#!/usr/bin/env sh

export PGPASSWORD=${PSQL_PASSWORD}
psql -U ${PSQL_USERNAME} -p ${PSQL_PORT} -h ${PSQL_HOST} -c "\set AUTOCOMMIT on\nDROP DATABASE \"${PSQL_DATABASE}\"; CREATE DATABASE \"${PSQL_DATABASE}\";"
# A-team tables the ateam_merge branch has foreign keys to (SCRUM-6720), before the app creates its own
python3 -m tests.init_ateam_fixture_db
