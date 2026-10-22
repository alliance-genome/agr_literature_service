from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context
from sqlalchemy_continuum.plugins import PropertyModTrackerPlugin
from sqlalchemy_continuum import make_versioned
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.triggers.curie_mirror_sql_func_triggers import mirror_columns
from sqlalchemy.orm import configure_mappers

# from literature.database.versioning import enable_versioning
# enable_versioning()

make_versioned(user_cls=None,
               plugins=[PropertyModTrackerPlugin()])
# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# `alembic -x db_url=<sqlalchemy url> ...` targets another database than the
# sqlalchemy.url of alembic.ini (e.g. the merged ABC + A-team database for the ateam_merge branch)
_db_url = context.get_x_argument(as_dictionary=True).get('db_url')
if _db_url:
    config.set_main_option('sqlalchemy.url', _db_url.replace('%', '%%'))

# Interpret the config file for Python logging.
# This line sets up loggers basically.
# fileConfig(config.config_file_name)
config_file_name = config.config_file_name or 'default_logging.ini'
fileConfig(config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
# target_metadata = literature.database.base.Base.metadata
## target_metadata = None

target_metadata = Base.metadata

import agr_literature_service.api.models  # noqa
configure_mappers()
# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def include_object(obj, name, type_, reflected, compare_to):
    """Autogenerate only compares ABC's own tables (SCRUM-6720). A-team tables are left alone:
    the ones ABC maps (ontologyterm, flagged info['ateam']), and in the merged database every table
    that ABC's metadata does not have (the rest of A-team's, ateam_person, ateam_reference)."""
    if type_ == 'table':
        if obj.info.get('ateam'):
            return False
        if reflected and compare_to is None:
            return False
    # the trigger-maintained curie mirrors (SCRUM-6720 Phase 4) are deliberately not in the models
    if type_ == 'column' and reflected and compare_to is None and name in mirror_columns(obj.table.name):
        return False
    return True


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            include_object=include_object
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
