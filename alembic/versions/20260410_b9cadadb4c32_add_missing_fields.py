"""fix model-db drift: add missing indexes, FKs, and rename misnamed indexes

Revision ID: b9cadadb4c32
Revises: e7695936840b
Create Date: 2026-04-10 19:04:27.228941

Fixes accumulated drift between SQLAlchemy models and the production DB:
- person_setting: missing individual column indexes and AuditedModel FKs
- reference_email: missing AuditedModel FKs on created_by/updated_by
- topic_entity_tag: rename idx_ prefixed indexes to ix_ (Alembic convention)
- workflow_tag: rename idx_ prefixed index to ix_
- workflow_tag_version: add missing workflow_tag_id index
- transaction: add missing user_id index

NOTE: users table drift (id nullable, user_id PK swap) is intentionally
excluded — changing it would break the users_pkey primary key, the
transaction_user_id_fkey FK (sqlalchemy-continuum), and all created_by/
updated_by FKs across the schema. That requires its own dedicated migration.
"""
from alembic import op
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision = 'b9cadadb4c32'
down_revision = 'e7695936840b'
branch_labels = None
depends_on = None


def _replace_index(conn, old_name, new_name):
    """Replace an idx_-prefixed index with ix_-prefixed name.

    If ix_ already exists (e.g. create_all_tables ran), drop the old idx_.
    If only idx_ exists (production), rename it.
    """
    ix_exists = conn.execute(text(
        "SELECT 1 FROM pg_indexes "
        "WHERE schemaname = current_schema() AND indexname = :name"
    ), {"name": new_name}).scalar()

    idx_exists = conn.execute(text(
        "SELECT 1 FROM pg_indexes "
        "WHERE schemaname = current_schema() AND indexname = :name"
    ), {"name": old_name}).scalar()

    if idx_exists and ix_exists:
        conn.execute(text(f'DROP INDEX {old_name}'))
    elif idx_exists:
        conn.execute(text(f'ALTER INDEX {old_name} RENAME TO {new_name}'))


def _create_index_if_not_exists(conn, index_name, table, column):
    """Create an index only if it doesn't already exist."""
    exists = conn.execute(text(
        "SELECT 1 FROM pg_indexes "
        "WHERE schemaname = current_schema() AND indexname = :name"
    ), {"name": index_name}).scalar()
    if not exists:
        conn.execute(text(
            f'CREATE INDEX {index_name} ON {table} ({column})'
        ))


def _create_fk_if_not_exists(conn, fk_name, table, column, ref_table,
                             ref_column):
    """Create a foreign key only if it doesn't already exist."""
    exists = conn.execute(text(
        "SELECT 1 FROM pg_constraint c "
        "JOIN pg_class r ON r.oid = c.conrelid "
        "JOIN pg_namespace n ON n.oid = r.relnamespace "
        "WHERE n.nspname = current_schema() "
        "AND r.relname = :table AND c.conname = :name"
    ), {"table": table, "name": fk_name}).scalar()
    if not exists:
        conn.execute(text(
            f'ALTER TABLE {table} ADD CONSTRAINT {fk_name} '
            f'FOREIGN KEY ({column}) REFERENCES {ref_table}({ref_column})'
        ))


def upgrade():
    conn = op.get_bind()

    # --- person_setting: add missing individual indexes from index=True ---
    _create_index_if_not_exists(
        conn, 'ix_person_setting_component_name',
        'person_setting', 'component_name')
    _create_index_if_not_exists(
        conn, 'ix_person_setting_date_created',
        'person_setting', 'date_created')
    _create_index_if_not_exists(
        conn, 'ix_person_setting_date_updated',
        'person_setting', 'date_updated')
    _create_index_if_not_exists(
        conn, 'ix_person_setting_person_id',
        'person_setting', 'person_id')

    # --- person_setting: add missing AuditedModel FKs ---
    _create_fk_if_not_exists(
        conn, 'person_setting_created_by_fkey',
        'person_setting', 'created_by', 'users', 'id')
    _create_fk_if_not_exists(
        conn, 'person_setting_updated_by_fkey',
        'person_setting', 'updated_by', 'users', 'id')

    # --- reference_email: add missing AuditedModel FKs ---
    _create_fk_if_not_exists(
        conn, 'reference_email_created_by_fkey',
        'reference_email', 'created_by', 'users', 'id')
    _create_fk_if_not_exists(
        conn, 'reference_email_updated_by_fkey',
        'reference_email', 'updated_by', 'users', 'id')

    # --- topic_entity_tag: normalize idx_ indexes to ix_ ---
    for col in ('data_novelty', 'entity', 'entity_type',
                'negated', 'species', 'topic'):
        _replace_index(
            conn,
            f'idx_topic_entity_tag_{col}',
            f'ix_topic_entity_tag_{col}')

    # --- workflow_tag: normalize idx_ index to ix_ ---
    _replace_index(
        conn,
        'idx_workflow_tag_workflow_tag_id',
        'ix_workflow_tag_workflow_tag_id')

    # --- workflow_tag_version: add missing index ---
    _create_index_if_not_exists(
        conn, 'ix_workflow_tag_version_workflow_tag_id',
        'workflow_tag_version', 'workflow_tag_id')

    # --- transaction: add missing user_id index (sqlalchemy-continuum) ---
    _create_index_if_not_exists(
        conn, 'ix_transaction_user_id',
        'transaction', 'user_id')


def downgrade():
    # --- transaction ---
    op.drop_index(op.f('ix_transaction_user_id'), table_name='transaction')

    # --- workflow_tag_version ---
    op.drop_index(
        op.f('ix_workflow_tag_version_workflow_tag_id'),
        table_name='workflow_tag_version')

    # --- workflow_tag: restore idx_ name ---
    op.execute(text(
        'ALTER INDEX ix_workflow_tag_workflow_tag_id '
        'RENAME TO idx_workflow_tag_workflow_tag_id'))

    # --- topic_entity_tag: restore idx_ names ---
    op.execute(text(
        'ALTER INDEX ix_topic_entity_tag_topic '
        'RENAME TO idx_topic_entity_tag_topic'))
    op.execute(text(
        'ALTER INDEX ix_topic_entity_tag_species '
        'RENAME TO idx_topic_entity_tag_species'))
    op.execute(text(
        'ALTER INDEX ix_topic_entity_tag_negated '
        'RENAME TO idx_topic_entity_tag_negated'))
    op.execute(text(
        'ALTER INDEX ix_topic_entity_tag_entity_type '
        'RENAME TO idx_topic_entity_tag_entity_type'))
    op.execute(text(
        'ALTER INDEX ix_topic_entity_tag_entity '
        'RENAME TO idx_topic_entity_tag_entity'))
    op.execute(text(
        'ALTER INDEX ix_topic_entity_tag_data_novelty '
        'RENAME TO idx_topic_entity_tag_data_novelty'))

    # --- reference_email: drop FKs ---
    op.drop_constraint(
        'reference_email_updated_by_fkey', 'reference_email',
        type_='foreignkey')
    op.drop_constraint(
        'reference_email_created_by_fkey', 'reference_email',
        type_='foreignkey')

    # --- person_setting: drop FKs ---
    op.drop_constraint(
        'person_setting_updated_by_fkey', 'person_setting',
        type_='foreignkey')
    op.drop_constraint(
        'person_setting_created_by_fkey', 'person_setting',
        type_='foreignkey')

    # --- person_setting: drop indexes ---
    op.drop_index(
        op.f('ix_person_setting_person_id'), table_name='person_setting')
    op.drop_index(
        op.f('ix_person_setting_date_updated'), table_name='person_setting')
    op.drop_index(
        op.f('ix_person_setting_date_created'), table_name='person_setting')
    op.drop_index(
        op.f('ix_person_setting_component_name'),
        table_name='person_setting')
