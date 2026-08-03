"""vocabulary fk linking and lab_position archive

FK-links the retired String relationship/lab_position fields to the ABC
controlled vocabularies (SCRUM-6311):

* seeds the ``default_user`` automation user, the ``lab_position`` and
  ``person_person_relationship`` vocabularies, and their terms;
* adds the ``*_vocab_term_abc_id`` FK columns (base + continuum
  ``_version`` twins) to laboratory_person, person_lineage and
  person_lineage_submission;
* cleans out non-loader laboratory_person rows and archives each non-blank
  ``lab_position`` into person_note;
* backfills the two relationship FKs from the retired slug values; and
* drops the retired String columns, swapping the person_lineage uniqueness
  constraint onto the new FK column.

Revision ID: 8603439f2008
Revises: 91c50e342859
Create Date: 2026-07-29 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from datetime import datetime, timezone

from agr_literature_service.api.crud.vocabulary_seed_data import (
    SEED_USER_ID, LAB_POSITION_VOCAB, PERSON_LINEAGE_VOCAB,
    LAB_POSITION_TERMS, PERSON_LINEAGE_SLUG_TO_LABEL,
)


# revision identifiers, used by Alembic.
revision = "8603439f2008"
down_revision = "91c50e342859"
branch_labels = None
depends_on = None

# The person_lineage uniqueness constraint that currently references the
# retired ``relationship`` String column; it has to be swapped onto the new FK.
_PL_UNIQUE = "uq_person_lineage_person_ids_relationship"


def _fk_col(tbl: str) -> str:
    return "lab_position_vocab_term_abc_id" if tbl == "laboratory_person" \
        else "relationship_vocab_term_abc_id"


def _fk_name(tbl: str, col: str) -> str:
    return f"fk_{tbl}_{col}"


def _ix_name(tbl: str, col: str) -> str:
    return f"ix_{tbl}_{col}"


# Guard against regressing past Postgres's 63-char identifier limit: every
# CONSTRAINT / INDEX name this migration creates must fit. Checked at import.
_CREATED_IDENTIFIERS = [_PL_UNIQUE] + [
    name
    for tbl in ("laboratory_person", "person_lineage", "person_lineage_submission")
    for name in (_fk_name(tbl, _fk_col(tbl)), _ix_name(tbl, _fk_col(tbl)))
]
_OVERLONG = [n for n in _CREATED_IDENTIFIERS if len(n) > 63]
if _OVERLONG:  # not assert: must survive `python -O`, which strips assert statements
    raise RuntimeError(f"identifiers exceed the 63-char Postgres limit: {_OVERLONG}")


def _add_fk_columns():
    for tbl in ("laboratory_person", "person_lineage", "person_lineage_submission"):
        col = _fk_col(tbl)
        op.add_column(tbl, sa.Column(col, sa.Integer(), nullable=True))
        op.create_index(op.f(_ix_name(tbl, col)), tbl, [col], unique=False)
        op.create_foreign_key(
            _fk_name(tbl, col), tbl, "vocabulary_term_abc",
            [col], ["vocabulary_term_abc_id"],
        )
        # mirror on the _version table (plain column, no index/FK), matching the
        # continuum template: add the column + its *_mod boolean.
        op.add_column(f"{tbl}_version", sa.Column(col, sa.Integer(), autoincrement=False, nullable=True))
        op.add_column(f"{tbl}_version",
                      sa.Column(f"{col}_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False))


def upgrade():
    conn = op.get_bind()
    now = datetime.now(timezone.utc)

    # 1. ensure the seed user exists
    conn.execute(sa.text(
        "INSERT INTO users (id, automation_username) VALUES (:u, :u) ON CONFLICT DO NOTHING"
    ), {"u": SEED_USER_ID})

    # 2. seed vocabularies + terms (ON CONFLICT DO NOTHING on the unique keys)
    def _seed_vocab(key, labels):
        conn.execute(sa.text(
            "INSERT INTO vocabulary_abc (vocabulary, date_created, date_updated, created_by, updated_by) "
            "VALUES (:v, :now, :now, :u, :u) ON CONFLICT (vocabulary) DO NOTHING"
        ), {"v": key, "now": now, "u": SEED_USER_ID})
        vid = conn.execute(sa.text(
            "SELECT vocabulary_abc_id FROM vocabulary_abc WHERE vocabulary = :v"
        ), {"v": key}).scalar_one()
        for label in labels:
            conn.execute(sa.text(
                "INSERT INTO vocabulary_term_abc "
                "(vocabulary_abc_id, name, is_obsolete, date_created, date_updated, created_by, updated_by) "
                "VALUES (:vid, :name, false, :now, :now, :u, :u) "
                "ON CONFLICT (vocabulary_abc_id, name) DO NOTHING"
            ), {"vid": vid, "name": label, "now": now, "u": SEED_USER_ID})

    _seed_vocab(LAB_POSITION_VOCAB, LAB_POSITION_TERMS)
    _seed_vocab(PERSON_LINEAGE_VOCAB, list(PERSON_LINEAGE_SLUG_TO_LABEL.values()))

    # 3. add FK columns (base + _version)
    _add_fk_columns()

    # 4. cleanup non-loader laboratory_person rows.
    #
    #    Ordering note (deliberate): this DELETE runs BEFORE the archive in step 5,
    #    so any non-loader row's lab_position is discarded without being archived.
    #    That is intended, and safe on prod, because on prod every laboratory_person
    #    row was written by the load_sgd_colleagues loader (created_by =
    #    'load_sgd_colleagues'): this DELETE therefore matches nothing and step 5 goes
    #    on to archive every row's lab_position. Non-loader rows exist only on dev /
    #    beta / restored dumps as throwaway test data, whose positions we do not want
    #    in person_note — so deleting them unarchived is the desired behaviour, not a
    #    data-loss bug. (created_by IS NULL rows, if any, are likewise non-loader; they
    #    survive this DELETE and would be archived — again a non-issue on prod, which
    #    has none.)
    conn.execute(sa.text(
        "DELETE FROM laboratory_person WHERE created_by <> 'load_sgd_colleagues'"
    ))

    # 5. archive lab_position into person_note (copy who/when verbatim)
    conn.execute(sa.text("""
        INSERT INTO person_note (person_id, note, date_created, date_updated, created_by, updated_by)
        SELECT lp.person_id,
               'Laboratory ' || COALESCE(l.curie, '') || ' : ' || lp.lab_position,
               lp.date_created, lp.date_updated, lp.created_by, lp.updated_by
        FROM laboratory_person lp
        JOIN laboratory l ON l.laboratory_id = lp.laboratory_id
        WHERE lp.lab_position IS NOT NULL AND lp.lab_position <> ''
    """))

    # 6. backfill relationship FK on both person_lineage and person_lineage_submission
    ppr_id = conn.execute(sa.text(
        "SELECT vocabulary_abc_id FROM vocabulary_abc WHERE vocabulary = :v"
    ), {"v": PERSON_LINEAGE_VOCAB}).scalar_one()
    for slug, label in PERSON_LINEAGE_SLUG_TO_LABEL.items():
        term_id = conn.execute(sa.text(
            "SELECT vocabulary_term_abc_id FROM vocabulary_term_abc "
            "WHERE vocabulary_abc_id = :vid AND name = :name"
        ), {"vid": ppr_id, "name": label}).scalar_one()
        for tbl in ("person_lineage", "person_lineage_submission"):
            conn.execute(sa.text(
                f"UPDATE {tbl} SET relationship_vocab_term_abc_id = :tid "
                f"WHERE relationship = :slug"
            ), {"tid": term_id, "slug": slug})

    # 7. guard: no unmapped relationship rows before NOT NULL
    for tbl in ("person_lineage", "person_lineage_submission"):
        left = conn.execute(sa.text(
            f"SELECT count(*) FROM {tbl} WHERE relationship_vocab_term_abc_id IS NULL"
        )).scalar_one()
        if left:  # not assert: must survive `python -O`, which strips assert statements
            raise RuntimeError(f"{tbl}: {left} rows have an unmapped relationship slug")

    # 8. swap the person_lineage uniqueness constraint off the retired String
    #    column before it can be dropped.
    op.drop_constraint(_PL_UNIQUE, "person_lineage", type_="unique")

    # 9. drop the retired String columns (base + _version)
    op.drop_column("laboratory_person", "lab_position")
    op.drop_column("laboratory_person_version", "lab_position")
    op.drop_column("person_lineage", "relationship")
    op.drop_column("person_lineage_version", "relationship")
    op.drop_column("person_lineage_submission", "relationship")
    op.drop_column("person_lineage_submission_version", "relationship")

    # 10. now that the data is populated, enforce NOT NULL on the two required FKs
    op.alter_column("person_lineage", "relationship_vocab_term_abc_id", nullable=False)
    op.alter_column("person_lineage_submission", "relationship_vocab_term_abc_id", nullable=False)

    # 11. recreate the uniqueness constraint on the new FK column
    op.create_unique_constraint(
        _PL_UNIQUE, "person_lineage",
        ["person_subject_id", "person_object_id", "relationship_vocab_term_abc_id"],
    )


def downgrade():
    conn = op.get_bind()
    label_to_slug = {v: k for k, v in PERSON_LINEAGE_SLUG_TO_LABEL.items()}

    # reverse of upgrade step 11: drop the FK-based uniqueness constraint before
    # the FK column can be dropped.
    op.drop_constraint(_PL_UNIQUE, "person_lineage", type_="unique")

    # re-add relationship String cols (nullable), repopulate from FK via inverse map
    for tbl in ("person_lineage", "person_lineage_submission"):
        op.add_column(tbl, sa.Column("relationship", sa.String(), nullable=True))
        op.add_column(f"{tbl}_version", sa.Column("relationship", sa.String(), autoincrement=False, nullable=True))
        for label, slug in label_to_slug.items():
            conn.execute(sa.text(f"""
                UPDATE {tbl} SET relationship = :slug
                FROM vocabulary_term_abc t
                WHERE {tbl}.relationship_vocab_term_abc_id = t.vocabulary_term_abc_id
                  AND t.name = :label
            """), {"slug": slug, "label": label})
        op.alter_column(tbl, "relationship", nullable=False)

    # recreate the original uniqueness constraint on the String column
    op.create_unique_constraint(
        _PL_UNIQUE, "person_lineage",
        ["person_subject_id", "person_object_id", "relationship"],
    )

    # re-add lab_position String col, left empty (values remain in person_note)
    op.add_column("laboratory_person", sa.Column("lab_position", sa.String(), nullable=True))
    op.add_column("laboratory_person_version",
                  sa.Column("lab_position", sa.String(), autoincrement=False, nullable=True))

    # drop the FK columns (base + _version) — reverse of _add_fk_columns
    for tbl in ("laboratory_person", "person_lineage", "person_lineage_submission"):
        col = _fk_col(tbl)
        op.drop_constraint(_fk_name(tbl, col), tbl, type_="foreignkey")
        op.drop_index(op.f(_ix_name(tbl, col)), table_name=tbl)
        op.drop_column(tbl, col)
        op.drop_column(f"{tbl}_version", f"{col}_mod")
        op.drop_column(f"{tbl}_version", col)

    # delete seeded terms + vocabularies (default_user users row left in place)
    for key in (LAB_POSITION_VOCAB, PERSON_LINEAGE_VOCAB):
        conn.execute(sa.text(
            "DELETE FROM vocabulary_term_abc WHERE vocabulary_abc_id = "
            "(SELECT vocabulary_abc_id FROM vocabulary_abc WHERE vocabulary = :v)"
        ), {"v": key})
        conn.execute(sa.text("DELETE FROM vocabulary_abc WHERE vocabulary = :v"), {"v": key})
    # NOT reversed (one-way): archived person_note rows; the cleanup DELETE.
