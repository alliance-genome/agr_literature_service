"""users -> agent, with an integer id primary key

Revision ID: 1e8243e92015
Revises: 47d74bfbbe87
Create Date: 2026-10-16 12:10:00.000000

SCRUM-6719: A-team conventions before the merge (see linkml_audited_object).

users carried two identifiers: the legacy string users.id (the primary key since 2021: an Okta
uid, then a person curie or an automation name) and the integer users.user_id added in Oct
2025, which SQLAlchemy-Continuum's transaction.user_id already references. Once the audit
columns point at user_id (previous revision) nothing references the string any more, and it is
redundant: it always equals automation_username, or else the person's curie. The upgrade checks
that for every row before dropping it.

  users.id (varchar, PK)         dropped
  users.user_id (integer, UK)    renamed to id, the primary key
  users                          renamed to agent (sequence, constraints, indexes follow)
  agent.automation_username      its index becomes UNIQUE (agents are looked up by label)
  transaction.user_id            FK repointed to agent(id); Continuum keeps the column name
  <audited>.created_by_id /      FK to agent(id) on every live audited table (history tables
  updated_by_id                  keep no FK)

Continuum derives transaction.user_id from the mapped primary key of user_cls ('UserModel'),
whatever its table and column are called, so the model only needs __tablename__ = 'agent' and
id as its primary key. users is not versioned.

The new foreign keys are added NOT VALID under the migration's lock and validated afterwards in
an autocommit block (SHARE UPDATE EXCLUSIVE only).
"""
import logging
from typing import List, Tuple

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '1e8243e92015'
down_revision = '47d74bfbbe87'
branch_labels = None
depends_on = None

logger = logging.getLogger('alembic.runtime.migration')

AUDIT_ID_COLUMNS = ('created_by_id', 'updated_by_id')
# names before this revision, for the downgrade
LEGACY_NAMES = {
    'agent_pkey': 'users_pkey',
    'ck_agent_exactly_one_of_person_or_automation': 'ck_users_exactly_one_of_person_or_automation',
    'agent_person_id_fkey': 'users_person_id_fkey',
    'ix_agent_automation_username': 'ix_users_automation_username',
    'ix_agent_person_id': 'ix_users_person_id',
}


def _rows(statement: str, **params) -> List[tuple]:
    return [tuple(row) for row in op.get_bind().execute(text(statement), params)]


def _scalar(statement: str, **params):
    return op.get_bind().execute(text(statement), params).scalar()


def _inbound_foreign_keys(table: str) -> List[Tuple[str, str, str, List[str]]]:
    return [(referencing, name, definition, list(columns)) for referencing, name, definition, columns in _rows(
        "SELECT c.conrelid::regclass::text, c.conname, pg_get_constraintdef(c.oid), "
        "ARRAY(SELECT a.attname FROM unnest(c.confkey) k "
        "      JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k) "
        "FROM pg_constraint c WHERE c.contype = 'f' AND c.confrelid = CAST(:t AS regclass) ORDER BY 1, 2", t=table)]


def _live_audited_tables() -> List[str]:
    return [table for (table,) in _rows(
        "SELECT DISTINCT table_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND column_name = 'created_by_id' ORDER BY 1")
        if not table.endswith('_version') and not table.startswith('ateam_')]


def _person_pk() -> str:
    return _scalar("SELECT a.attname FROM pg_index i JOIN pg_attribute a "
                   "ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0] "
                   "WHERE i.indrelid = CAST('person' AS regclass) AND i.indisprimary")


def _rename_names(table: str, mapping) -> None:
    for (name,) in _rows("SELECT conname FROM pg_constraint WHERE conrelid = CAST(:t AS regclass)", t=table):
        if mapping(name) != name:
            op.execute(f'ALTER TABLE {table} RENAME CONSTRAINT {name} TO {mapping(name)}')
    for (name,) in _rows("SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename = :t", t=table):
        if mapping(name) != name:
            op.execute(f'ALTER INDEX {name} RENAME TO {mapping(name)}')


def upgrade():
    foreign_keys = _inbound_foreign_keys('users')
    on_legacy = [fk for fk in foreign_keys if fk[3] == ['id']]
    if on_legacy:
        raise RuntimeError(f'{len(on_legacy)} foreign keys still reference users.id, e.g. {on_legacy[0][:2]}')
    unexpected = [fk for fk in foreign_keys if fk[3] != ['user_id']]
    if unexpected:
        raise RuntimeError(f'unexpected foreign keys into users: {unexpected}')
    if _scalar("SELECT to_regclass('public.users_version') IS NOT NULL"):
        raise RuntimeError('users_version exists: users is versioned again; rename it together with users')
    mismatches = _rows(
        f"SELECT u.user_id, u.id, u.automation_username, p.curie FROM users u "
        f"LEFT JOIN person p ON p.{_person_pk()} = u.person_id "
        "WHERE u.id IS DISTINCT FROM COALESCE(u.automation_username, p.curie) LIMIT 20")
    if mismatches:
        raise RuntimeError(f'users.id is not automation_username / person curie for (user_id, id, automation, '
                           f'curie): {mismatches}; it cannot be dropped')

    for referencing, name, _definition, _columns in foreign_keys:
        op.execute(f'ALTER TABLE {referencing} DROP CONSTRAINT {name}')
    op.execute('ALTER TABLE users DROP CONSTRAINT users_pkey')
    op.execute('ALTER TABLE users DROP CONSTRAINT uq_users_user_id')
    op.execute('ALTER TABLE users DROP COLUMN id')  # and ix_users_id with it
    op.execute('ALTER TABLE users RENAME COLUMN user_id TO id')
    op.execute('ALTER TABLE users RENAME TO agent')
    op.execute('ALTER TABLE agent ADD CONSTRAINT agent_pkey PRIMARY KEY (id)')
    sequence = _scalar("SELECT pg_get_serial_sequence('agent', 'id')")
    if sequence:
        op.execute(f'ALTER SEQUENCE {sequence} RENAME TO agent_id_seq')
    _rename_names('agent', lambda name: name.replace('users', 'agent'))
    # agent ids are now looked up by label; an automation name was unique as users.id, so this
    # cannot fail, and it lets resolve_agent_id insert with ON CONFLICT (automation_username)
    op.execute('DROP INDEX ix_agent_automation_username')
    op.execute('CREATE UNIQUE INDEX ix_agent_automation_username ON agent (automation_username)')

    to_validate = []
    for referencing, name, definition, _columns in foreign_keys:
        new_definition = definition.replace(' NOT VALID', '').replace('REFERENCES users(user_id)', 'REFERENCES agent(id)')
        op.execute(f'ALTER TABLE {referencing} ADD CONSTRAINT {name} {new_definition} NOT VALID')
        to_validate.append((referencing, name))
    for table in _live_audited_tables():
        for column in AUDIT_ID_COLUMNS:
            name = f'fk_{table}_{column}'[:63]
            op.execute(f'ALTER TABLE {table} ADD CONSTRAINT {name} FOREIGN KEY ({column}) REFERENCES agent (id) NOT VALID')
            to_validate.append((table, name))
    with op.get_context().autocommit_block():
        for table, name in to_validate:
            op.execute(f'ALTER TABLE {table} VALIDATE CONSTRAINT {name}')
    logger.info("users renamed to agent; %d foreign keys validated", len(to_validate))


def downgrade():
    for table in _live_audited_tables():
        for column in AUDIT_ID_COLUMNS:
            name = f'fk_{table}_{column}'[:63]
            op.execute(f'ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}')
    foreign_keys = _inbound_foreign_keys('agent')
    for referencing, name, _definition, _columns in foreign_keys:
        op.execute(f'ALTER TABLE {referencing} DROP CONSTRAINT {name}')
    op.execute('DROP INDEX ix_agent_automation_username')
    op.execute('CREATE INDEX ix_agent_automation_username ON agent (automation_username)')
    _rename_names('agent', lambda name: LEGACY_NAMES.get(name, name))
    op.execute('ALTER TABLE agent DROP CONSTRAINT users_pkey')
    op.execute('ALTER TABLE agent RENAME TO users')
    op.execute('ALTER TABLE users RENAME COLUMN id TO user_id')
    sequence = _scalar("SELECT pg_get_serial_sequence('users', 'user_id')")
    if sequence:
        op.execute(f'ALTER SEQUENCE {sequence} RENAME TO users_user_id_seq')
    op.execute('ALTER TABLE users ADD COLUMN id VARCHAR')
    op.execute(f"UPDATE users u SET id = COALESCE(u.automation_username, "
               f"(SELECT p.curie FROM person p WHERE p.{_person_pk()} = u.person_id))")
    op.execute('ALTER TABLE users ALTER COLUMN id SET NOT NULL')
    op.execute('ALTER TABLE users ADD CONSTRAINT users_pkey PRIMARY KEY (id)')
    op.execute('ALTER TABLE users ADD CONSTRAINT uq_users_user_id UNIQUE (user_id)')
    op.execute('CREATE INDEX ix_users_id ON users (id)')
    for referencing, name, definition, _columns in foreign_keys:
        if not name.startswith('fk_'):
            new_definition = definition.replace(' NOT VALID', '').replace('REFERENCES agent(id)', 'REFERENCES users(user_id)')
            op.execute(f'ALTER TABLE {referencing} ADD CONSTRAINT {name} {new_definition}')
