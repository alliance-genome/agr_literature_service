"""LinkML AuditedObject columns

Revision ID: 47d74bfbbe87
Revises: b5e2d8a41c73
Create Date: 2026-10-16 12:00:00.000000

SCRUM-6719: bring the ABC database to the A-team conventions before it is merged with the
A-team persistent store (SCRUM-6720). This revision, users_to_agent and primary_keys_to_id need
no A-team table and run on every ABC database.

Every ABC audited table, and its Continuum history twin, gets the columns of the LinkML
AuditedObject that the A-team persistent store uses (agr_curation_schema, core.yaml):

  created_by / updated_by  (varchar, FK users.id)  ->  created_by_id / updated_by_id (integer,
                                                       the users.user_id; their FK to agent.id
                                                       comes with the agent revision)
  db_date_created / db_date_updated                    new, filled from date_created /
                                                       date_updated: every ABC row was
                                                       created in this database
  internal                                             new, NOT NULL DEFAULT false (LinkML:
                                                       required)
  obsolete                                             new, NOT NULL DEFAULT false; the five
                                                       tables that have is_obsolete rename it

History tables get the same columns (nullable, as Continuum makes them) plus a <column>_mod
flag for each, filled from the flags of the columns they replace.

Audit strings resolve through users.id. History rows that still carry an Okta uid the Oct 2025
users migration replaced resolve through user_id_migration_backup (old_id -> new_id). The
upgrade stops before dropping the strings if any value has no user (measured 2026-10-16 on a
copy of prod: 75 tables, none).

Locking and time: ADD / RENAME COLUMN are metadata-only. The backfill rewrites every audited
row (~87M live + history rows on dev, 75 tables) in primary-key batches, each committed on its own in an
autocommit block, so stop the API and the automated scripts for the duration: a row written
during the backfill keeps NULL ids and makes the drop refuse. Every step is idempotent, so a
failed upgrade can simply be re-run. A final VACUUM FULL of the same tables takes back the space
of the dead row versions and dropped strings. Measured 2026-10-20 on a PG 18.4 copy of dev
(gp3, 3,000 IOPS): backfill 3h09m, VACUUM FULL 13 min, database 47 GB -> 77 GB -> 46 GB.

Downgrade restores the strings from users.id. A history row that carried a replaced Okta uid
gets the person curie that replaced it, not the uid.
"""
import logging
from typing import Dict, List, Optional, Set, Tuple

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '47d74bfbbe87'
down_revision = 'b5e2d8a41c73'
branch_labels = None
depends_on = None

logger = logging.getLogger('alembic.runtime.migration')

BATCH_SIZE = 50000
USER_COLUMNS = (('created_by', 'created_by_id'), ('updated_by', 'updated_by_id'))
DATE_COLUMNS = (('date_created', 'db_date_created'), ('date_updated', 'db_date_updated'))
FLAGS = ('internal', 'obsolete')
NEW_COLUMNS = tuple(new for _old, new in USER_COLUMNS + DATE_COLUMNS) + FLAGS
USER_ID_BACKUP_TABLE = 'user_id_migration_backup'
# is_obsolete before this revision, for the downgrade: table -> (nullable, default)
IS_OBSOLETE: Dict[str, Tuple[bool, Optional[str]]] = {
    'cross_reference': (True, None),
    'laboratory_allele_designation': (False, 'false'),
    'laboratory_cross_reference': (False, 'false'),
    'person_cross_reference': (False, 'false'),
    'vocabulary_term_abc': (False, None),
}
# audit FK names that do not follow <table>_<column>_fkey (person_email was created as email)
LEGACY_FK_PREFIX = {'person_email': 'email'}


def _execute(statement: str, **params) -> int:
    return op.get_bind().execute(text(statement), params).rowcount


def _scalar(statement: str, **params):
    return op.get_bind().execute(text(statement), params).scalar()


def _columns(table: str) -> Set[str]:
    rows = op.get_bind().execute(text(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = :t"),
        {'t': table})
    return {name for (name,) in rows}


def _tables_with(column: str) -> List[str]:
    rows = op.get_bind().execute(text(
        "SELECT DISTINCT table_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND column_name = :c ORDER BY 1"), {'c': column})
    return [table for (table,) in rows if not table.startswith('ateam_')]


def _history(table: str) -> bool:
    return table.endswith('_version')


def _batch_column(table: str) -> str:
    return _scalar(
        "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0] "
        "WHERE i.indrelid = CAST(:t AS regclass) AND i.indisprimary", t=table)


def _batched_update(table: str, assignments: List[str], pending: List[str]) -> None:
    """UPDATE <table> s SET ... WHERE (<pending> OR ...), one primary-key range per transaction."""
    if not assignments:
        return
    pk = _batch_column(table)
    where = ' OR '.join(pending)
    low, high = op.get_bind().execute(text(f'SELECT min(s.{pk}), max(s.{pk}) FROM {table} s WHERE {where}')).one()
    if low is None:
        return
    update = text(f"UPDATE {table} s SET {', '.join(assignments)} "
                  f"WHERE ({where}) AND s.{pk} >= :low AND s.{pk} < :high")
    updated = 0
    for start in range(low, high + 1, BATCH_SIZE):
        updated += op.get_bind().execute(update, {'low': start, 'high': start + BATCH_SIZE}).rowcount
    logger.info("%s: %d rows backfilled", table, updated)


def _add_columns(table: str) -> None:
    columns = _columns(table)
    history = _history(table)
    for _old, new in USER_COLUMNS:
        op.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {new} INTEGER')
    for _old, new in DATE_COLUMNS:
        op.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {new} TIMESTAMP WITHOUT TIME ZONE')
    if 'is_obsolete' in columns and 'obsolete' not in columns:
        op.execute(f'ALTER TABLE {table} RENAME COLUMN is_obsolete TO obsolete')
        if 'is_obsolete_mod' in columns:
            op.execute(f'ALTER TABLE {table} RENAME COLUMN is_obsolete_mod TO obsolete_mod')
    for flag in FLAGS:
        definition = 'BOOLEAN DEFAULT false' if history else 'BOOLEAN NOT NULL DEFAULT false'
        op.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {flag} {definition}')
    if history:
        for column in NEW_COLUMNS:
            op.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column}_mod BOOLEAN NOT NULL DEFAULT false')
    else:
        op.execute(f'ALTER TABLE {table} ALTER COLUMN obsolete SET DEFAULT false')


def _build_user_map() -> None:
    """Session temp table audit_user_map: every audit string in use -> users.user_id."""
    _execute("CREATE TEMP TABLE IF NOT EXISTS audit_user_map (legacy text PRIMARY KEY, user_id integer NOT NULL)")
    _execute("TRUNCATE audit_user_map")
    current = _execute("INSERT INTO audit_user_map SELECT id, user_id FROM users")
    recovered = 0
    if _scalar("SELECT to_regclass(:t) IS NOT NULL", t=f'public.{USER_ID_BACKUP_TABLE}'):
        ambiguous = op.get_bind().execute(text(
            f"SELECT old_id FROM {USER_ID_BACKUP_TABLE} WHERE old_id IS NOT NULL "
            "GROUP BY old_id HAVING count(DISTINCT new_id) > 1")).scalars().all()
        if ambiguous:
            raise RuntimeError(f'{USER_ID_BACKUP_TABLE} maps these ids to more than one user: {ambiguous}')
        recovered = _execute(
            f"INSERT INTO audit_user_map SELECT DISTINCT b.old_id, u.user_id FROM {USER_ID_BACKUP_TABLE} b "
            "JOIN users u ON u.id = b.new_id WHERE b.old_id IS NOT NULL ON CONFLICT (legacy) DO NOTHING")
    _execute("ANALYZE audit_user_map")
    logger.info("audit user map: %d users.id values, %d replaced ids from %s", current, recovered, USER_ID_BACKUP_TABLE)


def _backfill(table: str) -> None:
    columns = _columns(table)
    assignments: List[str] = []
    pending: List[str] = []
    for old, new in USER_COLUMNS + DATE_COLUMNS:
        if old not in columns:
            continue
        if old in ('created_by', 'updated_by'):
            assignments.append(f"{new} = (SELECT m.user_id FROM audit_user_map m WHERE m.legacy = s.{old})")
        else:
            assignments.append(f"{new} = s.{old}")
        pending.append(f"(s.{old} IS NOT NULL AND s.{new} IS NULL)")
        if _history(table) and f'{old}_mod' in columns:
            assignments.append(f"{new}_mod = s.{old}_mod")
    _batched_update(table, assignments, pending)


def _unresolved(table: str) -> int:
    columns = _columns(table)
    return sum(_scalar(f"SELECT count(*) FROM {table} WHERE {old} IS NOT NULL AND {new} IS NULL")
               for old, new in USER_COLUMNS if old in columns)


def upgrade():
    tables = _tables_with('created_by')
    for table in tables:
        _add_columns(table)
    with op.get_context().autocommit_block():
        _build_user_map()
        for table in tables:
            _backfill(table)
        unresolved = {table: count for table in tables if (count := _unresolved(table))}
        if unresolved:
            raise RuntimeError(f'audit values without a user (table: rows): {unresolved}; fix them and re-run')
        for table in tables:
            # an adopted is_obsolete that allowed NULL (cross_reference)
            if not _history(table) and _execute(f"UPDATE {table} SET obsolete = false WHERE obsolete IS NULL"):
                logger.info("%s: NULL obsolete set to false", table)
    for table in tables:
        if not _history(table):
            op.execute(f'ALTER TABLE {table} ALTER COLUMN obsolete SET NOT NULL')
        for old, _new in USER_COLUMNS:
            # the FK to users.id goes with the column
            op.execute(f'ALTER TABLE {table} DROP COLUMN IF EXISTS {old}')
            if _history(table):
                op.execute(f'ALTER TABLE {table} DROP COLUMN IF EXISTS {old}_mod')
    # the backfill left a dead version of every row and the dropped strings still sit in every live
    # row: VACUUM FULL rewrites each table and its indexes without them (it cannot run in a transaction)
    with op.get_context().autocommit_block():
        for table in tables:
            op.execute(f'VACUUM (FULL, ANALYZE) {table}')


def downgrade():
    tables = _tables_with('created_by_id')
    for table in tables:
        for old, _new in USER_COLUMNS:
            op.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {old} VARCHAR')
            if _history(table):
                op.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {old}_mod BOOLEAN NOT NULL DEFAULT false')
    with op.get_context().autocommit_block():
        for table in tables:
            assignments = [f"{old} = (SELECT u.id FROM users u WHERE u.user_id = s.{new})" for old, new in USER_COLUMNS]
            if _history(table):
                assignments += [f"{old}_mod = s.{new}_mod" for old, new in USER_COLUMNS]
            _batched_update(table, assignments,
                            [f"(s.{new} IS NOT NULL AND s.{old} IS NULL)" for old, new in USER_COLUMNS])
            if not _history(table):
                for old, _new in USER_COLUMNS:
                    name = f'{LEGACY_FK_PREFIX.get(table, table)}_{old}_fkey'
                    if not _scalar("SELECT count(*) FROM pg_constraint WHERE conname = :n", n=name):
                        _execute(f"ALTER TABLE {table} ADD CONSTRAINT {name} FOREIGN KEY ({old}) "
                                 "REFERENCES users (id) NOT VALID")
                    _execute(f"ALTER TABLE {table} VALIDATE CONSTRAINT {name}")
    for table in tables:
        history = _history(table)
        base = table[:-len('_version')] if history else table
        for column in NEW_COLUMNS:
            if column == 'obsolete' and base in IS_OBSOLETE:
                continue
            op.execute(f'ALTER TABLE {table} DROP COLUMN IF EXISTS {column}')
            if history:
                op.execute(f'ALTER TABLE {table} DROP COLUMN IF EXISTS {column}_mod')
        if base in IS_OBSOLETE:
            op.execute(f'ALTER TABLE {table} RENAME COLUMN obsolete TO is_obsolete')
            if history:
                op.execute(f'ALTER TABLE {table} RENAME COLUMN obsolete_mod TO is_obsolete_mod')
            else:
                nullable, default = IS_OBSOLETE[base]
                if nullable:
                    op.execute(f'ALTER TABLE {table} ALTER COLUMN is_obsolete DROP NOT NULL')
                if default is None:
                    op.execute(f'ALTER TABLE {table} ALTER COLUMN is_obsolete DROP DEFAULT')
