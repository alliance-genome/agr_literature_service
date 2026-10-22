"""Models follow the A-team conventions of the main-chain revisions (SCRUM-6719).

App startup runs Base.metadata.create_all() and re-creates every trigger function from
api/triggers/*.py, so a model or trigger source that disagrees with the revisions would put
the old names back on the first restart. These tests read the revision files rather than
copying their tables.
"""
import importlib.util
import re
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import BigInteger, Integer, text

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import ReferenceModel, ResourceModel
from agr_literature_service.api.triggers import (author_sql_func_triggers, citation_sql_func_triggers,
                                                 person_email_sql_func_triggers, reference_image_sql_func_triggers,
                                                 reference_sql_func_triggers, resource_sql_func_triggers)

from ..fixtures import db  # noqa: F401

VERSIONS = Path(__file__).resolve().parents[2] / 'alembic' / 'versions'
TRIGGER_MODULES = (author_sql_func_triggers, citation_sql_func_triggers, person_email_sql_func_triggers,
                   reference_image_sql_func_triggers, reference_sql_func_triggers, resource_sql_func_triggers)


def _revision(filename):
    spec = importlib.util.spec_from_file_location(filename, VERSIONS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PRIMARY_KEYS = _revision('20261016_80595e1e0d4d_primary_keys_to_id.py')
BIGINT_IDS = _revision('20261022_039dbc7de1ef_id_columns_to_bigint.py')


def _function_source(name):
    """The CREATE OR REPLACE FUNCTION / PROCEDURE statement for name, as written in api/triggers/*.py."""
    sql = '\n'.join(value for module in TRIGGER_MODULES for value in vars(module).values() if isinstance(value, str))
    start = re.search(rf'(FUNCTION|PROCEDURE) {name}\(', sql).start()
    end = re.compile(r'CREATE OR REPLACE (FUNCTION|PROCEDURE)').search(sql, start + 1)
    return sql[start:end.start() if end else len(sql)]


def test_every_renamed_table_has_id_primary_key():
    wrong = {table: [column.name for column in Base.metadata.tables[table].primary_key]
             for table in PRIMARY_KEYS.PK_RENAMES if table in Base.metadata.tables}
    assert {table: columns for table, columns in wrong.items() if columns != ['id']} == {}


def test_every_renamed_table_is_mapped():
    assert [table for table in PRIMARY_KEYS.PK_RENAMES if table not in Base.metadata.tables] == []


def test_no_foreign_key_targets_an_old_primary_key():
    stale = [f'{table.name}.{fk.parent.name} -> {fk.target_fullname}'
             for table in Base.metadata.tables.values() for fk in table.foreign_keys
             if fk.target_fullname.split('.')[0] in PRIMARY_KEYS.PK_RENAMES and not fk.target_fullname.endswith('.id')]
    assert stale == []


def test_trigger_sources_match_revision():
    for function, edits in PRIMARY_KEYS.FUNCTION_REWRITES.items():
        source = _function_source(function)
        for old, new in edits:
            assert old not in source, (function, old)
            assert new in source, (function, new)


def test_every_listed_id_column_is_bigint():
    wrong = [f'{table}.{name}' for table, names in BIGINT_IDS.BIGINT_COLUMNS.items() if table in Base.metadata.tables
             for name in names if not isinstance(Base.metadata.tables[table].columns[name].type, BigInteger)]
    assert wrong == []


def test_no_integer_id_column_left():
    """A new integer primary key, foreign key or *_id column would come back as integer from create_all."""
    left = [f'{table.name}.{column.name}' for table in Base.metadata.tables.values() for column in table.columns
            if isinstance(column.type, Integer) and not isinstance(column.type, BigInteger)
            and (column.primary_key or column.foreign_keys or re.search(r'(^id$|_id$)', column.name))]
    assert left == []


def test_unlisted_integer_ids_skips_ateam_tables(db):  # noqa: F811
    """In the database merged with the A-team (SCRUM-6720) Flyway's integer key is not an ABC id."""
    connection = db.connection()
    connection.execute(text("CREATE TABLE flyway_schema_history (installed_rank integer PRIMARY KEY)"))
    connection.execute(text("CREATE TABLE not_listed_abc_table (id integer PRIMARY KEY)"))
    try:
        with Operations.context(MigrationContext.configure(connection)):
            assert BIGINT_IDS._unlisted_integer_ids() == ['not_listed_abc_table.id']
    finally:
        db.rollback()


def test_trigger_sources_match_bigint_revision():
    for function, edits in BIGINT_IDS.FUNCTION_TYPE_EDITS.items():
        source = _function_source(function).lower()
        for old, new in edits:
            # update_citations takes reference.id%type, which follows the column by itself
            if not new.startswith('(IN '):
                assert old.lower() not in source, (function, old)
                assert new.lower() in source, (function, new)


def test_citation_created_on_reference_insert(db):  # noqa
    """The citation trigger finds its sequence whether it is citation_citation_id_seq (migrated) or
    citation_id_seq (create_all)."""
    resource = ResourceModel(curie='AGRKB:102999999999901', title='Conventions Test Journal',
                             title_abbreviation='Conv Test J')
    db.add(resource)
    db.flush()
    reference = ReferenceModel(curie='AGRKB:101999999999901', title='Conventions test',
                               category='research_article', resource_id=resource.resource_id)
    db.add(reference)
    db.commit()
    citation_id = db.execute(text('SELECT citation_id FROM reference WHERE id = :i'),
                             {'i': reference.reference_id}).scalar()
    assert citation_id is not None
