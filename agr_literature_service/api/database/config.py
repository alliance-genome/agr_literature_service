from agr_literature_service.api.config import config

SQLALCHEMY_DATABASE_URL = "postgresql://" \
    + config.PSQL_USERNAME + ":" + config.PSQL_PASSWORD \
    + "@" + config.PSQL_HOST + ":" + config.PSQL_PORT \
    + "/" + config.PSQL_DATABASE

SQLALCHEMY_DATABASE_NOPASS = "postgresql://" \
    + config.PSQL_USERNAME + ":XXXXXX"  \
    + "@" + config.PSQL_HOST + ":" + config.PSQL_PORT \
    + "/" + config.PSQL_DATABASE
