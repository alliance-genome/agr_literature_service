#!/bin/bash
# Apply the latest alembic migration against the database defined in an env file.
#
# Usage:
#   ./apply_alembic_migration.sh <env_file> <devdb|rds>
#
#   devdb - stops the API, applies the migration, restarts the API
#   rds   - stops API/Debezium/automated scripts, applies the migration,
#           restarts API/Debezium/automated scripts
#
# Before doing anything the script prints the database connection info from
# the env file and the list of containers that will be stopped/restarted,
# then asks for confirmation. alembic.ini is temporarily updated with the
# connection URL from the env file and restored to its original content
# when the script exits (so credentials never stay in the git-tracked file).

set -euo pipefail

usage() {
    echo "Usage: $0 <env_file> <devdb|rds>"
    exit 1
}

[ $# -eq 2 ] || usage

ENV_FILE=$1
MODE=$2

if [ "${MODE}" != "devdb" ] && [ "${MODE}" != "rds" ]; then
    echo "ERROR: mode must be 'devdb' or 'rds', got '${MODE}'"
    usage
fi

if [ ! -f "${ENV_FILE}" ]; then
    echo "ERROR: env file '${ENV_FILE}' does not exist"
    exit 1
fi

get_var() {
    grep -E "^$1=" "${ENV_FILE}" | tail -1 | cut -d= -f2- | tr -d '"' | tr -d "'"
}

PSQL_USERNAME=$(get_var PSQL_USERNAME)
PSQL_PASSWORD=$(get_var PSQL_PASSWORD)
PSQL_HOST=$(get_var PSQL_HOST)
PSQL_PORT=$(get_var PSQL_PORT)
PSQL_DATABASE=$(get_var PSQL_DATABASE)

MISSING=""
for var in PSQL_USERNAME PSQL_PASSWORD PSQL_HOST PSQL_PORT PSQL_DATABASE; do
    [ -n "${!var}" ] || MISSING="${MISSING} ${var}"
done
if [ -n "${MISSING}" ]; then
    echo "ERROR: missing variable(s) in ${ENV_FILE}:${MISSING}"
    exit 1
fi

echo "================================================================"
echo " Alembic migration plan"
echo "================================================================"
echo ""
echo " Env file:      ${ENV_FILE}"
echo ""
echo " Database connection (from env file):"
echo "   PSQL_USERNAME = ${PSQL_USERNAME}"
echo "   PSQL_PASSWORD = ********"
echo "   PSQL_HOST     = ${PSQL_HOST}"
echo "   PSQL_PORT     = ${PSQL_PORT}"
echo "   PSQL_DATABASE = ${PSQL_DATABASE}"
echo ""
echo " alembic.ini will be temporarily updated with:"
echo "   sqlalchemy.url = postgresql://${PSQL_USERNAME}:********@${PSQL_HOST}:${PSQL_PORT}/${PSQL_DATABASE}"
echo " and restored to its original content when this script exits."
echo ""
if [ "${MODE}" = "devdb" ]; then
    # Dev databases are distinguished by hostname (e.g. literature-4006);
    # strip any domain suffix so just the name is shown
    echo " Mode: ${PSQL_HOST%%.*}"
    echo "   1. Stop container:    api"
    echo "   2. Run:               alembic upgrade head (in dev_app)"
    echo "   3. Restart container: api"
else
    # Identify the target from PSQL_HOST so stage vs prod is obvious at the prompt
    case "${PSQL_HOST}" in
        *literature-prod*rds*|*rds*literature-prod*) MODE_LABEL="PROD ABC database" ;;
        *literature-dev*rds*|*rds*literature-dev*)   MODE_LABEL="STAGE ABC database" ;;
        *) MODE_LABEL="rds (WARNING: unrecognized host '${PSQL_HOST}')" ;;
    esac
    echo " Mode: ${MODE_LABEL}"
    echo "   1. Stop containers:   dbz_connector dbz_kafka dbz_zookeeper dbz_ksql_server dbz_setup"
    echo "                         api, automated_scripts"
    echo "   2. Run:               alembic upgrade head (in dev_app)"
    echo "   3. Restart:           api, automated_scripts, Debezium stack"
fi
echo ""
echo "================================================================"

read -r -p "Proceed? [y/N] " ANSWER < /dev/tty
case "${ANSWER}" in
    y|Y|yes|YES) ;;
    *) echo "Aborted, no changes made."; exit 1 ;;
esac

# Temporarily point alembic.ini at the database from the env file;
# restore the original on any exit so credentials are never left behind.
cp alembic.ini alembic.ini.bak
restore_alembic_ini() {
    mv alembic.ini.bak alembic.ini
    echo "Restored original alembic.ini"
}
trap restore_alembic_ini EXIT

export NEW_URL="postgresql://${PSQL_USERNAME}:${PSQL_PASSWORD}@${PSQL_HOST}:${PSQL_PORT}/${PSQL_DATABASE}"
awk '/^sqlalchemy\.url[ ]*=/ { print "sqlalchemy.url = " ENVIRON["NEW_URL"]; next } { print }' \
    alembic.ini.bak > alembic.ini
echo "Updated alembic.ini sqlalchemy.url from ${ENV_FILE}"

if [ "${MODE}" = "rds" ]; then
    docker-compose --env-file "${ENV_FILE}" rm -svf dbz_connector dbz_kafka dbz_zookeeper dbz_ksql_server dbz_setup
    docker-compose --env-file "${ENV_FILE}" rm -s -f api
    docker-compose --env-file "${ENV_FILE}" rm -s -f automated_scripts
else
    docker-compose --env-file "${ENV_FILE}" rm -s -f api
fi

docker-compose --env-file "${ENV_FILE}" build dev_app
docker-compose --env-file "${ENV_FILE}" run --service-ports --rm dev_app alembic upgrade head

# Put the original alembic.ini back before rebuilding/restarting services
trap - EXIT
restore_alembic_ini

if [ "${MODE}" = "rds" ]; then
    make ENV_FILE="${ENV_FILE}" restart-api-and-automated-scripts
    make ENV_FILE="${ENV_FILE}" restart-debezium-aws
else
    make ENV_FILE="${ENV_FILE}" restart-api
fi

echo "Done: migration applied and services restarted."
