"""add data_context to topic_entity_tag and ml_model

Revision ID: d7b3e1c95a24
Revises: 9a1c7f2e4d10
Create Date: 2026-08-31

SCRUM-5697. ``data_context`` records what kind of data a topic entity tag
represents, using four disjoint ATP terms:

    ATP:0000325  experimentally studied data
    ATP:0000360  background information
    ATP:0000328  expression marker
    ATP:0000327  genetic marker

The column is added **nullable** here on purpose. Every TET-creating client
(the extraction pipelines, the MOD loaders in this repo, WB's own scripts,
FB's export scripts) has to start sending a value, and every existing row has
to be backfilled, before the NOT NULL constraint can land.

That flip lives on its own branch/PR (revision e4f9a2c81b57) rather than here,
because ``apply_alembic_migration.sh`` runs ``alembic upgrade head``: shipping
both revisions together would apply this one, then abort on the NOT NULL guard
while NULLs remain -- failing the deploy after it has already stopped the api,
automated_scripts and Debezium containers. Keeping them separate means
``upgrade head`` is safe at every point. Sequence:

    this revision  ->  API deploy  ->  SCRUM-5697_backfill_data_context.py
                   ->  e4f9a2c81b57 (separate PR)

``ml_model.data_context`` mirrors ``ml_model.data_novelty``: the classifier and
entity-extraction pipelines read the value off the model they are running and
stamp it onto every tag they create, so the curation policy lives on the model
row rather than in pipeline code.
"""
from alembic import op
import sqlalchemy as sa


revision = 'd7b3e1c95a24'
down_revision = '9a1c7f2e4d10'
branch_labels = None
depends_on = None


def _col_exists(conn, table: str, column: str) -> bool:
    return bool(
        conn.execute(
            sa.text(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = :t AND column_name = :c
                """
            ),
            {"t": table, "c": column},
        ).fetchone()
    )


def _index_exists(conn, name: str) -> bool:
    return bool(
        conn.execute(
            sa.text(
                """
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema() AND indexname = :n
                """
            ),
            {"n": name},
        ).fetchone()
    )


def upgrade():
    conn = op.get_bind()

    # --- topic_entity_tag ---
    if not _col_exists(conn, 'topic_entity_tag', 'data_context'):
        op.add_column('topic_entity_tag',
                      sa.Column('data_context', sa.String(), nullable=True))
    if not _index_exists(conn, 'ix_topic_entity_tag_data_context'):
        op.create_index(op.f('ix_topic_entity_tag_data_context'),
                        'topic_entity_tag', ['data_context'], unique=False)

    # --- topic_entity_tag_version (sqlalchemy-continuum) ---
    # The version table needs both the value column and the companion
    # "_mod" boolean; continuum writes the latter on every UPDATE and the
    # audit trail breaks without it.
    if not _col_exists(conn, 'topic_entity_tag_version', 'data_context'):
        op.add_column('topic_entity_tag_version',
                      sa.Column('data_context', sa.String(),
                                autoincrement=False, nullable=True))
    if not _col_exists(conn, 'topic_entity_tag_version', 'data_context_mod'):
        op.add_column('topic_entity_tag_version',
                      sa.Column('data_context_mod', sa.Boolean(),
                                server_default=sa.text('false'),
                                nullable=False))
    if not _index_exists(conn, 'ix_topic_entity_tag_version_data_context'):
        op.create_index(op.f('ix_topic_entity_tag_version_data_context'),
                        'topic_entity_tag_version', ['data_context'],
                        unique=False)

    # --- ml_model ---
    if not _col_exists(conn, 'ml_model', 'data_context'):
        op.add_column('ml_model',
                      sa.Column('data_context', sa.String(), nullable=True))


def downgrade():
    conn = op.get_bind()

    if _col_exists(conn, 'ml_model', 'data_context'):
        op.drop_column('ml_model', 'data_context')

    if _index_exists(conn, 'ix_topic_entity_tag_version_data_context'):
        op.drop_index(op.f('ix_topic_entity_tag_version_data_context'),
                      table_name='topic_entity_tag_version')
    if _col_exists(conn, 'topic_entity_tag_version', 'data_context_mod'):
        op.drop_column('topic_entity_tag_version', 'data_context_mod')
    if _col_exists(conn, 'topic_entity_tag_version', 'data_context'):
        op.drop_column('topic_entity_tag_version', 'data_context')

    if _index_exists(conn, 'ix_topic_entity_tag_data_context'):
        op.drop_index(op.f('ix_topic_entity_tag_data_context'),
                      table_name='topic_entity_tag')
    if _col_exists(conn, 'topic_entity_tag', 'data_context'):
        op.drop_column('topic_entity_tag', 'data_context')
