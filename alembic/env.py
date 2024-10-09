from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context
from sqlalchemy_continuum.plugins import PropertyModTrackerPlugin
from sqlalchemy_continuum import make_versioned
from agr_literature_service.api.database.base import Base
from sqlalchemy.orm import configure_mappers

# from literature.database.versioning import enable_versioning
# enable_versioning()

make_versioned(user_cls=None,
               plugins=[PropertyModTrackerPlugin()])
# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

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
    """
    def include_name(name, type_, parent_names):
        if type_ == "schema":
            # note this will not include the default schema
            return name in ["lit"]
        else:
            return True
    """

    def include_object(object, name, type_, reflected, compare_to):
        #if type_ == "table" and object.schema != "lit":
        #    return False
        #else:
        #    return True
        print("schema:" + object.schema + "target schema:" + target_metadata.schema)
        if (type_ == 'table' and object.schema == target_metadata.schema):
            return True
        if (type_ == 'column' and object.table.schema == target_metadata.schema):
            return True

    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        version_table_schema=target_metadata.schema,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        include_object=include_object,
        include_name = include_name
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    def include_object(object, name, type_, reflected, compare_to):
        #if type_ == "table" and object.schema != "lit":
        #    return False
        #else:
        #    return True
        #print("schema:" + object.schema + "target schema:" + target_metadata.schema)
        if (type_ == 'table' and object.schema == target_metadata.schema):
            return True
        if (type_ == 'column' and object.table.schema == target_metadata.schema):
            return True

    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table_schema=target_metadata.schema,
            compare_type=True,
            include_schemas=True,
            include_object=include_object,
            include_name=include_name
        )

        with context.begin_transaction():
            context.run_migrations()


def include_name(name, type_, parent_names):
    if type_ == "schema":
        return name in ["lit"]
    else:
        return True

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
