"""agent / person / automation as class-table inheritance

Revision ID: 8a73f0666768
Revises: 9c40d77181c9
Create Date: 2026-11-24

SCRUM-6995. Before this revision an agent pointed at what it was (agent.person_id, or
agent.automation_username), and a person had its own id sequence. After it, agent is the parent
table and person / automation are child tables that share the agent's id:

  agent        id, agent_type ('person' | 'automation'), display_name, the audit columns
  person       id = agent.id, agent_type fixed to 'person', FK (id, agent_type) -> agent
  automation   id = agent.id, agent_type fixed to 'automation', FK (id, agent_type) -> agent

  person.display_name and the audit columns move to agent (person_version's to agent_version,
  which is seeded from person_version so a person's name history survives the move).
  agent.person_id, agent.automation_username and their check constraint are dropped.
  agent.id becomes a plain sequence (agent_id_seq), which is also where person ids now come from;
  person_person_id_seq is dropped.

Which agent id a person gets: the id of its existing agent (the lowest when it has several, so a
login agent wins over a later WBPerson placeholder claimed by SCRUM-6729), else a new one. Every
other agent of the same person is merged into that one: created_by_id / updated_by_id on every
audited table and history table, and transaction.user_id, are rewritten to the kept agent, and the
extra agent is deleted. Person ids are then rewritten to agent ids in person, person_version and
every column (live and history) that references a person.

Columns that take part in a unique index are rewritten in two passes (to the negated new id, then
the sign flipped), since PostgreSQL checks those indexes row by row and an old id may equal another
person's new id.

Downgrade restores the composition model but is lossy: persons keep their new ids, merged agents
are not split again, and persons that had no agent keep the agent this revision gave them.
"""
import logging
import time
from typing import List, Tuple

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '8a73f0666768'
down_revision = '9c40d77181c9'
branch_labels = None
depends_on = None

logger = logging.getLogger('alembic.runtime.migration')

TIMESTAMP = 'TIMESTAMP WITHOUT TIME ZONE'
AUDIT_ID_COLUMNS = ('created_by_id', 'updated_by_id')
# the AuditedModel columns, with the types they have on every audited table
AUDIT_COLUMNS = (
    ('date_created', TIMESTAMP),
    ('date_updated', TIMESTAMP),
    ('created_by_id', 'BIGINT'),
    ('updated_by_id', 'BIGINT'),
    ('db_date_created', TIMESTAMP),
    ('db_date_updated', TIMESTAMP),
    ('internal', 'BOOLEAN'),
    ('obsolete', 'BOOLEAN'),
)
# what moves from person (and person_version) to agent (and agent_version)
MOVED_COLUMNS = (('display_name', 'VARCHAR'),) + AUDIT_COLUMNS
MOVED_NAMES = tuple(name for name, _type in MOVED_COLUMNS)
# the versioned agent columns besides the primary key, each with a Continuum <name>_mod flag
AGENT_VERSIONED = ('agent_type',) + MOVED_NAMES
UTC_NOW = "(now() AT TIME ZONE 'UTC')"


def _rows(statement: str, **params) -> List[tuple]:
    return [tuple(row) for row in op.get_bind().execute(text(statement), params)]


def _scalar(statement: str, **params):
    return op.get_bind().execute(text(statement), params).scalar()


def _execute(label: str, statement: str, **params) -> int:
    started = time.monotonic()
    count = op.get_bind().execute(text(statement), params).rowcount
    logger.info('%s: %s rows in %.1fs', label, count, time.monotonic() - started)
    return count


def _table_exists(table: str) -> bool:
    return bool(_scalar("SELECT to_regclass(:t) IS NOT NULL", t=f'public.{table}'))


def _columns(table: str) -> List[str]:
    return [name for (name,) in _rows(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = :t", t=table)]


def _inbound_foreign_keys(table: str) -> List[Tuple[str, str, str, List[str]]]:
    """(referencing table, constraint name, definition, referencing columns) of each FK into table."""
    return [(referencing, name, definition, list(columns)) for referencing, name, definition, columns in _rows(
        "SELECT c.conrelid::regclass::text, c.conname, pg_get_constraintdef(c.oid), "
        "ARRAY(SELECT a.attname FROM unnest(c.conkey) k "
        "      JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k) "
        "FROM pg_constraint c WHERE c.contype = 'f' AND c.confrelid = CAST(:t AS regclass) ORDER BY 1, 2", t=table)]


def _audited_tables() -> List[str]:
    """Every table with an audit agent column, history tables included (not the A-team copies)."""
    return [table for (table,) in _rows(
        "SELECT DISTINCT table_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND column_name IN ('created_by_id', 'updated_by_id') ORDER BY 1")
        if not table.startswith('ateam_')]


def _unique_index_columns(table: str) -> List[str]:
    return [name for (name,) in _rows(
        "SELECT DISTINCT a.attname FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
        "WHERE i.indrelid = CAST(:t AS regclass) AND i.indisunique", t=table)]


def _person_id_columns(person_fks) -> List[Tuple[str, str]]:
    """Every (table, column) holding a person id: person and person_version themselves, each
    column with an FK into person, and the same column of that table's history table."""
    targets = [('person', 'id'), ('person_version', 'id')]
    for referencing, _name, _definition, columns in person_fks:
        version_table = f'{referencing}_version'
        version_columns = _columns(version_table) if _table_exists(version_table) else []
        for column in columns:
            targets.append((referencing, column))
            if column in version_columns:
                targets.append((version_table, column))
    return targets


def _check_preconditions() -> None:
    for table in ('automation', 'agent_version'):
        if _table_exists(table):
            raise RuntimeError(f'{table} already exists')
    with_colon = _rows("SELECT id, automation_username FROM agent WHERE automation_username LIKE '%:%' LIMIT 20")
    if with_colon:
        raise RuntimeError(f'automation names look like curies, so labels would be ambiguous: {with_colon}')
    malformed = _rows("SELECT id FROM agent WHERE (person_id IS NULL) = (automation_username IS NULL) LIMIT 20")
    if malformed:
        raise RuntimeError(f'agents that are not exactly one of person / automation: {malformed}')


def _agent_id_sequence() -> None:
    """agent.id from a plain sequence named agent_id_seq (it is an identity column on the RDS
    databases), set past the highest id."""
    if _scalar("SELECT attidentity FROM pg_attribute "
               "WHERE attrelid = CAST('agent' AS regclass) AND attname = 'id'") in ('a', 'd'):
        op.execute('ALTER TABLE agent ALTER COLUMN id DROP IDENTITY')  # drops its sequence
        op.execute('CREATE SEQUENCE agent_id_seq AS BIGINT OWNED BY agent.id')
        op.execute("ALTER TABLE agent ALTER COLUMN id SET DEFAULT nextval('agent_id_seq')")
    else:
        sequence = _scalar("SELECT pg_get_serial_sequence('agent', 'id')")
        if sequence and sequence != 'public.agent_id_seq':
            op.execute(f'ALTER SEQUENCE {sequence} RENAME TO agent_id_seq')
    op.execute("SELECT setval('agent_id_seq', GREATEST((SELECT max(id) FROM agent), 1))")


def _add_agent_columns() -> None:
    columns = ['ADD COLUMN agent_type VARCHAR', 'ADD COLUMN display_name VARCHAR']
    for name, column_type in AUDIT_COLUMNS:
        default = ' NOT NULL DEFAULT false' if column_type == 'BOOLEAN' else ''
        columns.append(f'ADD COLUMN {name} {column_type}{default}')
    op.execute(f"ALTER TABLE agent {', '.join(columns)}")


def _build_person_agent_map(person_id_columns: List[Tuple[str, str]]) -> None:
    """person_agent_map: old person id -> agent id, for every person id found anywhere (history of
    deleted persons included); agent_merge_map: every other agent of a person -> the kept one."""
    op.execute('CREATE TEMP TABLE person_agent_map (person_id BIGINT PRIMARY KEY, agent_id BIGINT NOT NULL UNIQUE)')
    _execute('person_agent_map from existing agents',
             'INSERT INTO person_agent_map (person_id, agent_id) '
             'SELECT person_id, min(id) FROM agent WHERE person_id IS NOT NULL GROUP BY person_id')
    known_ids = ' UNION '.join(f'SELECT {column} FROM {table}' for table, column in person_id_columns
                               if table.endswith('_version') or table == 'person')
    _execute('person_agent_map new agent ids',
             'INSERT INTO person_agent_map (person_id, agent_id) '
             f"SELECT ids.id, nextval('agent_id_seq') FROM ({known_ids}) AS ids (id) "
             'WHERE ids.id IS NOT NULL '
             'AND NOT EXISTS (SELECT 1 FROM person_agent_map m WHERE m.person_id = ids.id)')
    op.execute('CREATE TEMP TABLE agent_merge_map (extra_id BIGINT PRIMARY KEY, keeper_id BIGINT NOT NULL)')
    _execute('agent_merge_map',
             'INSERT INTO agent_merge_map (extra_id, keeper_id) '
             'SELECT a.id, m.agent_id FROM agent a JOIN person_agent_map m ON m.person_id = a.person_id '
             'WHERE a.id <> m.agent_id')


def _fill_agent_rows() -> None:
    _execute('agents for persons without one',
             'INSERT INTO agent (id, person_id) '
             'SELECT m.agent_id, m.person_id FROM person_agent_map m JOIN person p ON p.id = m.person_id '
             'WHERE NOT EXISTS (SELECT 1 FROM agent a WHERE a.person_id = m.person_id)')
    moved = ', '.join(f'{name} = p.{name}' for name in MOVED_NAMES)
    _execute('person agents', f"UPDATE agent a SET agent_type = 'person', {moved} FROM person p WHERE p.id = a.person_id")
    _execute('automation agents',
             f"UPDATE agent SET agent_type = 'automation', display_name = automation_username, "
             f"date_created = {UTC_NOW}, date_updated = {UTC_NOW}, "
             f"db_date_created = {UTC_NOW}, db_date_updated = {UTC_NOW} "
             'WHERE automation_username IS NOT NULL')


def _merge_extra_agents() -> None:
    if not _scalar('SELECT count(*) FROM agent_merge_map'):
        logger.info('no person has more than one agent; nothing to merge')
        return
    for extra_id, keeper_id in _rows('SELECT extra_id, keeper_id FROM agent_merge_map ORDER BY 1'):
        logger.info('merging agent %s into agent %s', extra_id, keeper_id)
    extras = 'SELECT extra_id FROM agent_merge_map'
    for table in _audited_tables():
        columns = [column for column in AUDIT_ID_COLUMNS if column in _columns(table)]
        assignments = ', '.join(
            f'{column} = COALESCE((SELECT keeper_id FROM agent_merge_map WHERE extra_id = {table}.{column}), {column})'
            for column in columns)
        condition = ' OR '.join(f'{column} IN ({extras})' for column in columns)
        _execute(f'merge {table}', f'UPDATE {table} SET {assignments} WHERE {condition}')
    _execute('merge transaction',
             'UPDATE "transaction" t SET user_id = m.keeper_id FROM agent_merge_map m WHERE t.user_id = m.extra_id')
    _execute('delete merged agents', f'DELETE FROM agent WHERE id IN ({extras})')


def _remap_person_ids(person_fks, person_id_columns: List[Tuple[str, str]]) -> None:
    for referencing, name, _definition, _columns_ in person_fks:
        op.execute(f'ALTER TABLE {referencing} DROP CONSTRAINT {name}')
    op.execute('ALTER TABLE agent DROP CONSTRAINT IF EXISTS agent_person_id_fkey')
    for table, column in person_id_columns:
        if column in _unique_index_columns(table):
            _execute(f'remap {table}.{column} (pass 1)',
                     f'UPDATE {table} t SET {column} = -m.agent_id FROM person_agent_map m '
                     f'WHERE t.{column} = m.person_id')
            _execute(f'remap {table}.{column} (pass 2)', f'UPDATE {table} SET {column} = -{column} WHERE {column} < 0')
        else:
            _execute(f'remap {table}.{column}',
                     f'UPDATE {table} t SET {column} = m.agent_id FROM person_agent_map m '
                     f'WHERE t.{column} = m.person_id')
    sequence = _scalar("SELECT pg_get_serial_sequence('person', 'id')")
    op.execute('ALTER TABLE person ALTER COLUMN id DROP DEFAULT')
    if sequence:
        op.execute(f'DROP SEQUENCE {sequence}')


def _create_children() -> None:
    op.execute('ALTER TABLE agent '
               'ALTER COLUMN agent_type SET NOT NULL, '
               'ALTER COLUMN display_name SET NOT NULL, '
               'ALTER COLUMN date_created SET NOT NULL, '
               "ADD CONSTRAINT ck_agent_agent_type CHECK (agent_type IN ('person', 'automation')), "
               'ADD CONSTRAINT uq_agent_id_agent_type UNIQUE (id, agent_type)')
    op.execute("ALTER TABLE person ADD COLUMN agent_type VARCHAR NOT NULL DEFAULT 'person' "
               "CONSTRAINT ck_person_agent_type CHECK (agent_type = 'person')")
    op.execute('ALTER TABLE person ADD CONSTRAINT fk_person_agent FOREIGN KEY (id, agent_type) '
               'REFERENCES agent (id, agent_type) ON DELETE CASCADE NOT VALID')
    op.execute("CREATE TABLE automation ("
               "id BIGINT NOT NULL, "
               "agent_type VARCHAR NOT NULL DEFAULT 'automation', "
               "CONSTRAINT automation_pkey PRIMARY KEY (id), "
               "CONSTRAINT ck_automation_agent_type CHECK (agent_type = 'automation'), "
               "CONSTRAINT fk_automation_agent FOREIGN KEY (id, agent_type) "
               "REFERENCES agent (id, agent_type) ON DELETE CASCADE)")
    _execute('automation rows', "INSERT INTO automation (id) SELECT id FROM agent WHERE agent_type = 'automation'")
    for column in AUDIT_ID_COLUMNS:
        op.execute(f'ALTER TABLE agent ADD CONSTRAINT fk_agent_{column} FOREIGN KEY ({column}) '
                   'REFERENCES agent (id) NOT VALID')


def _create_agent_version() -> None:
    """agent_version in the layout SQLAlchemy-Continuum builds (validity strategy plus the
    PropertyModTrackerPlugin _mod flags), seeded from person_version."""
    columns = ['id BIGINT NOT NULL', 'agent_type VARCHAR']
    for name, column_type in MOVED_COLUMNS:
        default = ' DEFAULT false' if column_type == 'BOOLEAN' else ''
        columns.append(f'{name} {column_type}{default}')
    columns += ['transaction_id BIGINT NOT NULL', 'end_transaction_id BIGINT', 'operation_type SMALLINT NOT NULL']
    columns += [f'{name}_mod BOOLEAN NOT NULL DEFAULT false' for name in AGENT_VERSIONED]
    columns.append('CONSTRAINT agent_version_pkey PRIMARY KEY (id, transaction_id)')
    op.execute(f"CREATE TABLE agent_version ({', '.join(columns)})")
    for column in ('transaction_id', 'end_transaction_id', 'operation_type', 'agent_type',
                   'date_created', 'date_updated'):
        op.execute(f'CREATE INDEX ix_agent_version_{column} ON agent_version ({column})')
    copied = MOVED_NAMES + ('transaction_id', 'end_transaction_id', 'operation_type') + tuple(
        f'{name}_mod' for name in MOVED_NAMES)
    _execute('agent_version from person_version',
             f"INSERT INTO agent_version (id, agent_type, agent_type_mod, {', '.join(copied)}) "
             f"SELECT id, 'person', operation_type = 0, {', '.join(copied)} FROM person_version")


def _move_columns_off_person() -> None:
    # their indexes (ix_person_display_name_trgm, ix_person_date_*) and FKs go with them
    op.execute(f"ALTER TABLE person {', '.join(f'DROP COLUMN {name}' for name in MOVED_NAMES)}")
    dropped = MOVED_NAMES + tuple(f'{name}_mod' for name in MOVED_NAMES)
    op.execute(f"ALTER TABLE person_version {', '.join(f'DROP COLUMN {name}' for name in dropped)}")
    op.execute('CREATE INDEX ix_agent_display_name_trgm ON agent USING gin (display_name gin_trgm_ops)')
    op.execute("CREATE UNIQUE INDEX uq_agent_automation_display_name ON agent (display_name) "
               "WHERE agent_type = 'automation'")
    op.execute('CREATE INDEX ix_agent_agent_type ON agent (agent_type)')
    op.execute('CREATE INDEX ix_agent_date_created ON agent (date_created)')
    op.execute('CREATE INDEX ix_agent_date_updated ON agent (date_updated)')


def upgrade():
    _check_preconditions()
    person_fks = [fk for fk in _inbound_foreign_keys('person') if fk[0] != 'agent']
    person_id_columns = _person_id_columns(person_fks)

    _agent_id_sequence()
    _add_agent_columns()
    _build_person_agent_map(person_id_columns)
    _fill_agent_rows()
    _merge_extra_agents()
    _remap_person_ids(person_fks, person_id_columns)
    _create_children()
    _create_agent_version()
    _move_columns_off_person()
    op.execute('ALTER TABLE agent DROP CONSTRAINT ck_agent_exactly_one_of_person_or_automation, '
               'DROP COLUMN person_id, DROP COLUMN automation_username')  # ix_agent_* go with them
    op.execute('DROP TABLE person_agent_map')
    op.execute('DROP TABLE agent_merge_map')

    to_validate = [('person', 'fk_person_agent')] + [('agent', f'fk_agent_{column}') for column in AUDIT_ID_COLUMNS]
    for referencing, name, definition, _columns_ in person_fks:
        op.execute(f"ALTER TABLE {referencing} ADD CONSTRAINT {name} {definition.replace(' NOT VALID', '')} NOT VALID")
        to_validate.append((referencing, name))
    with op.get_context().autocommit_block():
        for table, name in to_validate:
            op.execute(f'ALTER TABLE {table} VALIDATE CONSTRAINT {name}')
    logger.info('agent / person / automation inheritance in place; %d foreign keys validated', len(to_validate))


def downgrade():
    # composition columns back on agent
    op.execute('ALTER TABLE agent ADD COLUMN person_id BIGINT, ADD COLUMN automation_username VARCHAR')
    op.execute("UPDATE agent SET person_id = id WHERE agent_type = 'person'")
    op.execute("UPDATE agent SET automation_username = display_name WHERE agent_type = 'automation'")

    # display_name and the audit columns back on person / person_version
    person_columns = []
    for name, column_type in MOVED_COLUMNS:
        default = ' NOT NULL DEFAULT false' if column_type == 'BOOLEAN' else ''
        person_columns.append(f'ADD COLUMN {name} {column_type}{default}')
    op.execute(f"ALTER TABLE person {', '.join(person_columns)}")
    op.execute(f"UPDATE person p SET {', '.join(f'{name} = a.{name}' for name in MOVED_NAMES)} "
               'FROM agent a WHERE a.id = p.id')
    op.execute('ALTER TABLE person ALTER COLUMN display_name SET NOT NULL, ALTER COLUMN date_created SET NOT NULL')
    version_columns = []
    for name, column_type in MOVED_COLUMNS:
        default = ' DEFAULT false' if column_type == 'BOOLEAN' else ''
        version_columns.append(f'ADD COLUMN {name} {column_type}{default}')
    version_columns += [f'ADD COLUMN {name}_mod BOOLEAN NOT NULL DEFAULT false' for name in MOVED_NAMES]
    op.execute(f"ALTER TABLE person_version {', '.join(version_columns)}")
    restored = MOVED_NAMES + tuple(f'{name}_mod' for name in MOVED_NAMES)
    op.execute(f"UPDATE person_version p SET {', '.join(f'{name} = v.{name}' for name in restored)} "
               'FROM agent_version v WHERE v.id = p.id AND v.transaction_id = p.transaction_id')
    op.execute('CREATE INDEX ix_person_display_name_trgm ON person USING gin (display_name gin_trgm_ops)')
    op.execute('CREATE INDEX ix_person_date_created ON person (date_created)')
    op.execute('CREATE INDEX ix_person_date_updated ON person (date_updated)')
    op.execute('CREATE INDEX ix_person_version_date_created ON person_version (date_created)')
    op.execute('CREATE INDEX ix_person_version_date_updated ON person_version (date_updated)')
    for column in AUDIT_ID_COLUMNS:
        op.execute(f'ALTER TABLE person ADD CONSTRAINT fk_person_{column} FOREIGN KEY ({column}) '
                   'REFERENCES agent (id)')

    # person ids from their own sequence again (persons keep the ids this revision gave them)
    op.execute('CREATE SEQUENCE person_person_id_seq AS BIGINT OWNED BY person.id')
    op.execute("SELECT setval('person_person_id_seq', GREATEST((SELECT max(id) FROM person), 1))")
    op.execute("ALTER TABLE person ALTER COLUMN id SET DEFAULT nextval('person_person_id_seq')")

    # children and history go
    op.execute('ALTER TABLE person DROP CONSTRAINT fk_person_agent')
    op.execute('ALTER TABLE person DROP COLUMN agent_type')  # and ck_person_agent_type
    op.execute('DROP TABLE automation')
    op.execute('DROP TABLE agent_version')
    for column in AUDIT_ID_COLUMNS:
        op.execute(f'ALTER TABLE agent DROP CONSTRAINT fk_agent_{column}')
    op.execute('ALTER TABLE agent DROP CONSTRAINT uq_agent_id_agent_type, DROP CONSTRAINT ck_agent_agent_type')
    agent_dropped = ('agent_type',) + MOVED_NAMES  # ix_agent_* and uq_agent_automation_display_name go with them
    op.execute(f"ALTER TABLE agent {', '.join(f'DROP COLUMN {name}' for name in agent_dropped)}")

    # the composition model's constraints and indexes
    op.execute('ALTER TABLE agent ADD CONSTRAINT agent_person_id_fkey FOREIGN KEY (person_id) '
               'REFERENCES person (id) ON DELETE SET NULL')
    op.execute('CREATE INDEX ix_agent_person_id ON agent (person_id)')
    op.execute('CREATE UNIQUE INDEX ix_agent_automation_username ON agent (automation_username)')
    op.execute('ALTER TABLE agent ADD CONSTRAINT ck_agent_exactly_one_of_person_or_automation '
               'CHECK ((person_id IS NULL) <> (automation_username IS NULL)) NOT VALID')

    # agent.id back to an identity column
    op.execute('ALTER TABLE agent ALTER COLUMN id DROP DEFAULT')
    op.execute('DROP SEQUENCE agent_id_seq')
    op.execute('ALTER TABLE agent ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY')
    op.execute("SELECT setval(pg_get_serial_sequence('agent', 'id'), GREATEST((SELECT max(id) FROM agent), 1))")
