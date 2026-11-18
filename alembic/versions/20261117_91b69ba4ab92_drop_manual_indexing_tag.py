"""drop_manual_indexing_tag

Revision ID: 91b69ba4ab92
Revises: c4e1a7d90b23
Create Date: 2026-11-17

Retires the manual_indexing_tag table (SCRUM-6861). It only ever held FlyBase's
"no genetic data" (ATP:0000207) predictions, which SCRUM-6670 moved to topic
entity tags; its --delete-source run emptied the table on prod on 2026-11-12,
after backing up all 7,368 rows to CSV.

The upgrade refuses to run if the table still has rows. Dropping the table also
drops its sequence and removes it from the debezium_unified publication; the
Debezium configuration in the same release no longer lists it, and a full
Debezium rebuild must follow the deploy.

The downgrade recreates the table empty, as it was on stage before the drop;
the rows are not restored.
"""
from alembic import op


revision = "91b69ba4ab92"
down_revision = "c4e1a7d90b23"
branch_labels = None
depends_on = None


def upgrade():
    # EXECUTE, so the row check is only planned when the table exists (a plain SELECT in
    # the IF fails with "relation does not exist" even behind the to_regclass test).
    op.execute("""
        DO $$
        DECLARE
            has_rows boolean;
        BEGIN
            IF to_regclass('public.manual_indexing_tag') IS NOT NULL THEN
                EXECUTE 'SELECT EXISTS (SELECT 1 FROM manual_indexing_tag)' INTO has_rows;
                IF has_rows THEN
                    RAISE EXCEPTION 'manual_indexing_tag still has rows; back them up and delete '
                                    'them before dropping the table (SCRUM-6861)';
                END IF;
            END IF;
        END
        $$;
    """)
    op.execute("DROP TABLE IF EXISTS manual_indexing_tag")


def downgrade():
    op.execute("""
        CREATE TABLE manual_indexing_tag (
            id bigserial PRIMARY KEY,
            reference_id bigint NOT NULL REFERENCES reference(id) ON DELETE CASCADE,
            mod_id bigint NOT NULL REFERENCES mod(id) ON DELETE CASCADE,
            curation_tag varchar(64) NOT NULL,
            confidence_score double precision,
            validation_by_biocurator varchar(32),
            note varchar,
            date_created timestamp without time zone NOT NULL,
            date_updated timestamp without time zone,
            created_by_id bigint,
            updated_by_id bigint,
            db_date_created timestamp without time zone,
            db_date_updated timestamp without time zone,
            internal boolean NOT NULL DEFAULT false,
            obsolete boolean NOT NULL DEFAULT false,
            CONSTRAINT uq_mod_ref_tag UNIQUE (mod_id, reference_id, curation_tag),
            CONSTRAINT ck_confval_curation_tag_prefix CHECK (curation_tag LIKE 'ATP:%'),
            CONSTRAINT ck_confval_confidence_range
                CHECK (confidence_score IS NULL OR (confidence_score >= 0.0 AND confidence_score <= 1.0)),
            CONSTRAINT ck_manual_indexing_tag_validation
                CHECK (validation_by_biocurator IS NULL OR validation_by_biocurator IN ('right', 'wrong')),
            CONSTRAINT fk_manual_indexing_tag_created_by_id FOREIGN KEY (created_by_id) REFERENCES agent(id),
            CONSTRAINT fk_manual_indexing_tag_updated_by_id FOREIGN KEY (updated_by_id) REFERENCES agent(id)
        )
    """)
    for column in ("curation_tag", "date_created", "date_updated", "mod_id", "reference_id"):
        op.execute(f"CREATE INDEX ix_manual_indexing_tag_{column} ON manual_indexing_tag ({column})")
