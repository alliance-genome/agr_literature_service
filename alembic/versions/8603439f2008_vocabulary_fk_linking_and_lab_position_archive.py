"""vocabulary fk linking and lab_position mapping

(Filename still says "archive" -- it encodes the immutable revision id; the
migration now maps lab_position into the vocabulary rather than archiving it.)

FK-links the retired String relationship/lab_position fields to the ABC
controlled vocabularies (SCRUM-6311):

* seeds the ``default_user`` automation user, the ``lab_position`` and
  ``person_person_relationship`` vocabularies, and their terms;
* adds the ``*_vocab_term_abc_id`` FK columns (base + continuum
  ``_version`` twins) to laboratory_person, person_lineage and
  person_lineage_submission;
* maps each non-blank ``lab_position`` into the controlled vocabulary via the
  hardcoded curator mapping -- a role FK id, or ``is_pi`` for the
  Principal-Investigator/Director bucket -- failing closed on any unmapped value;
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


# Curator-generated free-text lab_position -> controlled-vocabulary mapping,
# a one-time snapshot of
#   https://dev.alliancegenome.org/reports/lab_title_mapping/lab_position_flat_mapping.txt
# (302 entries; keys are lower(btrim())-normalized). Hardcoded on purpose: a
# migration must be reproducible and must not fetch a URL at runtime. The lone
# 'associate professor (research)' entry was added by hand (the flatfile lacked
# that exact string). 'Principal Investigator/Director' is a sentinel target, not
# a seeded term -- that bucket sets is_pi instead of a role FK (see upgrade step 5).
_LAB_POSITION_TEXT_TO_TERM = {
    "a/prof": "Principal Investigator/Director",
    "adjunct professor": "Principal Investigator/Director",
    "adjunt investigator": "Senior Scientist",
    "agg.professor": "Principal Investigator/Director",
    "assistant fellow": "Post-Doc",
    "assistant member": "Lab Member",
    "assistant prof": "Principal Investigator/Director",
    "assistant prof.": "Principal Investigator/Director",
    "assistant professor": "Principal Investigator/Director",
    "assistant professor genetics": "Principal Investigator/Director",
    "assistant professor of biological science": "Principal Investigator/Director",
    "assistant professor of biology": "Principal Investigator/Director",
    "assistant professor of biology and biochemistry": "Principal Investigator/Director",
    "assistant professor of genetics": "Principal Investigator/Director",
    "assistant professor of medicine": "Principal Investigator/Director",
    "assistant professor of molecular genetics and microbiology": "Principal Investigator/Director",
    "assistant professor of research": "Principal Investigator/Director",
    "assistant research professor": "Principal Investigator/Director",
    "assistant researcher": "Research Associate/Assistant",
    "assoc. member": "Lab Member",
    "assoc. prof.": "Principal Investigator/Director",
    "assoc. professor": "Principal Investigator/Director",
    "associate investigator": "Senior Scientist",
    "associate member": "Lab Member",
    "associate professor": "Principal Investigator/Director",
    "associate professor (research)": "Principal Investigator/Director",
    "associate professor genetics": "Principal Investigator/Director",
    "associate professor of biology": "Principal Investigator/Director",
    "associate professor of genetics": "Principal Investigator/Director",
    "associate professor of microbiology": "Principal Investigator/Director",
    "associate professor of microbiology and immunology": "Principal Investigator/Director",
    "associate professor of pathology and microbiology-immunology": "Principal Investigator/Director",
    "associate professor, investigator hhmi": "Principal Investigator/Director",
    "associate professor, pi": "Principal Investigator/Director",
    "associate professor/associate dean": "Principal Investigator/Director",
    "associate professor/robert w. beart chair in chemistry": "Principal Investigator/Director",
    "associate research fellow": "Post-Doc",
    "associate research scientist": "Senior Scientist",
    "associate research specialist": "Research Associate/Assistant",
    "associate researcher": "Research Associate/Assistant",
    "associate scientist": "Senior Scientist",
    "associate staff": "Research Associate/Assistant",
    "associated professor": "Principal Investigator/Director",
    "asst. prof.": "Principal Investigator/Director",
    "asst. professor": "Principal Investigator/Director",
    "asst. professor of biology": "Principal Investigator/Director",
    "biologist": "Research Associate/Assistant",
    "bloomberg distinguished professor": "Principal Investigator/Director",
    "business unit head": "Principal Investigator/Director",
    "ceo": "Administrative Staff",
    "charge de recherches": "Research Associate/Assistant",
    "chief executive officer": "Administrative Staff",
    "chief research scientist": "Senior Scientist",
    "chief scientific officer": "Administrative Staff",
    "chief, laboratory of biochemistry & genetics": "Principal Investigator/Director",
    "clinical scientist": "Senior Scientist",
    "cnr scientist": "Senior Scientist",
    "curator": "Professional Biocurator",
    "data architect/senior scientific curator": "Professional Biocurator",
    "database curator, pombase": "Professional Biocurator",
    "developer": "Programmer/Bioinformatician",
    "directeur de recherche": "Senior Scientist",
    "directeur de recherche cnrs": "Senior Scientist",
    "director": "Principal Investigator/Director",
    "director department of molecular cell biology": "Principal Investigator/Director",
    "director of genetic engineering division": "Principal Investigator/Director",
    "director, bioassay technologies": "Principal Investigator/Director",
    "director, michael smith laboratories": "Principal Investigator/Director",
    "doc student": "Graduate Student",
    "docent (associate professor)": "Principal Investigator/Director",
    "doctoral student": "Graduate Student",
    "dr": "Senior Scientist",
    "dr.": "Senior Scientist",
    "faculty": "Principal Investigator/Director",
    "faculty member": "Principal Investigator/Director",
    "full member": "Lab Member",
    "full professor": "Principal Investigator/Director",
    "gaiser professor": "Principal Investigator/Director",
    "grad stud": "Graduate Student",
    "grad student": "Graduate Student",
    "graduate research assistant": "Graduate Student",
    "graduate student": "Graduate Student",
    "graduate student researcher": "Graduate Student",
    "graduate teaching assistant": "Graduate Student",
    "graduate_student": "Graduate Student",
    "group head": "Principal Investigator/Director",
    "group leader": "Group Leader",
    "group leader, strain development": "Group Leader",
    "gyula and katica tauber professor of biochemistry": "Principal Investigator/Director",
    "halstead-bent professor of biology": "Principal Investigator/Director",
    "harvard fellow/group leader": "Post-Doc",
    "head of a group in the department of cell biology": "Principal Investigator/Director",
    "head of department": "Principal Investigator/Director",
    "head of division": "Principal Investigator/Director",
    "head of institute": "Principal Investigator/Director",
    "head of lab": "Principal Investigator/Director",
    "head of laboratory": "Principal Investigator/Director",
    "head of molecular biology": "Principal Investigator/Director",
    "head of molecular biology and biotechnology": "Principal Investigator/Director",
    "head of process division": "Principal Investigator/Director",
    "honours student": "Graduate Student",
    "imaging manager": "Lab Manager",
    "inserm group leader": "Group Leader",
    "instructor": "Research Associate/Assistant",
    "investigator": "Senior Scientist",
    "irta fellow": "Post-Doc",
    "james b duke professor": "Principal Investigator/Director",
    "junior professor": "Principal Investigator/Director",
    "junior research fellow": "Post-Doc",
    "junior research group leader": "Group Leader",
    "junior researcher": "Research Associate/Assistant",
    "lab head": "Principal Investigator/Director",
    "lab manager": "Lab Manager",
    "lab technician": "Technician",
    "laboratories manager": "Lab Manager",
    "laboratory chief": "Principal Investigator/Director",
    "laboratory supervisor": "Lab Manager",
    "lecturer": "Principal Investigator/Director",
    "lecturer in cell biology": "Principal Investigator/Director",
    "lecturer in microbiology": "Principal Investigator/Director",
    "lecturer/senior researcher": "Principal Investigator/Director",
    "leverhulme emeritus fellow": "Post-Doc",
    "lewis-sigler fellow": "Post-Doc",
    "m.d. ph.d.": "MD/Veterinarian",
    "m.d. student": "Graduate Student",
    "maitre de conferences": "Research Associate/Assistant",
    "manager of the imaging facility": "Lab Manager",
    "manager, type 2 diabetes knowledge portal": "Lab Manager",
    "master student": "Graduate Student",
    "max planck research group leader": "Group Leader",
    "medical microbiologist": "Research Associate/Assistant",
    "medical professor": "Principal Investigator/Director",
    "member": "Lab Member",
    "member and associate investigator": "Senior Scientist",
    "member professional staff (mps)": "Principal Investigator/Director",
    "molecular biology manager": "Lab Manager",
    "mrc senior non-clinical research fellow": "Post-Doc",
    "oncology fellow, post-doc": "Post-Doc",
    "p.i.": "Principal Investigator/Director",
    "pd": "Post-Doc",
    "permanent/research": "Research Associate/Assistant",
    "ph.d student": "Graduate Student",
    "ph.d. candidate": "Graduate Student",
    "ph.d. student": "Graduate Student",
    "phd": "Graduate Student",
    "phd candidate": "Graduate Student",
    "phd graduate student": "Graduate Student",
    "phd student": "Graduate Student",
    "phd, senior investigator": "Graduate Student",
    "phd. student": "Graduate Student",
    "pi": "Principal Investigator/Director",
    "post doc": "Post-Doc",
    "post doctoral fellow": "Post-Doc",
    "post doctoral research associate": "Post-Doc",
    "post doctoral research fellow": "Post-Doc",
    "post-doc": "Post-Doc",
    "post-doc fellow": "Post-Doc",
    "post-doc fulbright fellow": "Post-Doc",
    "post-doc researcher": "Post-Doc",
    "post-doc, industrial project leader": "Post-Doc",
    "post-doctoral fellow": "Post-Doc",
    "post-doctoral student": "Post-Doc",
    "post-doctorate": "Post-Doc",
    "postbacc researcher": "Research Associate/Assistant",
    "postdoc": "Post-Doc",
    "postdoc fellow": "Post-Doc",
    "postdoctoral associate": "Post-Doc",
    "postdoctoral fellow": "Post-Doc",
    "postdoctoral research associate": "Post-Doc",
    "postdoctoral research fellow": "Post-Doc",
    "postdoctoral researcher": "Post-Doc",
    "postdoctoral resercher": "Post-Doc",
    "postdoctoral scholar": "Post-Doc",
    "postgraduate student": "Graduate Student",
    "president": "Administrative Staff",
    "principal biocuration scientist": "Senior Scientist",
    "principal investigator": "Principal Investigator/Director",
    "principal investigator, asst. professor": "Principal Investigator/Director",
    "principal research investigator": "Senior Scientist",
    "principal research scientist": "Senior Scientist",
    "principal scientist": "Senior Scientist",
    "principle investigator": "Principal Investigator/Director",
    "prof and chair": "Principal Investigator/Director",
    "prof.": "Principal Investigator/Director",
    "professor": "Principal Investigator/Director",
    "professor & vice-chair": "Principal Investigator/Director",
    "professor (head of lab)": "Principal Investigator/Director",
    "professor and acting head": "Principal Investigator/Director",
    "professor and chair": "Principal Investigator/Director",
    "professor and chairman": "Principal Investigator/Director",
    "professor and eagles chair in food biotechnology": "Principal Investigator/Director",
    "professor and executive director": "Principal Investigator/Director",
    "professor and head": "Principal Investigator/Director",
    "professor and senior research fellow": "Post-Doc",
    "professor in wine biotechnology": "Principal Investigator/Director",
    "professor of biochemistry": "Principal Investigator/Director",
    "professor of biochemistry and cell biology": "Principal Investigator/Director",
    "professor of biochemistry and of genetics": "Principal Investigator/Director",
    "professor of biological sciences": "Principal Investigator/Director",
    "professor of cell biology": "Principal Investigator/Director",
    "professor of genetics": "Principal Investigator/Director",
    "professor of genetics and medicine": "Principal Investigator/Director",
    "professor of microbiology": "Principal Investigator/Director",
    "professor of molecular biology": "Principal Investigator/Director",
    "professor of molecular, cell & developmental biology": "Principal Investigator/Director",
    "professor of oncology": "Principal Investigator/Director",
    "professor of pharmacology": "Principal Investigator/Director",
    "professor of pharmacology and medicine": "Principal Investigator/Director",
    "professor of physiology": "Principal Investigator/Director",
    "professor of yeast molecular biology": "Principal Investigator/Director",
    "professor, department of molecular genetics and microbiology": "Principal Investigator/Director",
    "professor, director ctn for biomedical inventions": "Principal Investigator/Director",
    "professor, head of department": "Principal Investigator/Director",
    "professor, senior scientist": "Principal Investigator/Director",
    "professor.retired": "Principal Investigator/Director",
    "professsor of biology": "Principal Investigator/Director",
    "programme leader": "Group Leader",
    "project director": "Principal Investigator/Director",
    "project manager, dba": "Lab Manager",
    "reader": "Principal Investigator/Director",
    "reader in genetics": "Principal Investigator/Director",
    "reasearch fellow": "Post-Doc",
    "res. assist. prof.": "Principal Investigator/Director",
    "res. assoc. professor": "Principal Investigator/Director",
    "res./lab director": "Principal Investigator/Director",
    "research": "Research Associate/Assistant",
    "research & collection scientist": "Senior Scientist",
    "research asistant": "Research Associate/Assistant",
    "research assistant": "Research Associate/Assistant",
    "research assistant professor": "Principal Investigator/Director",
    "research associate": "Research Associate/Assistant",
    "research associate ii": "Research Associate/Assistant",
    "research associate professor": "Principal Investigator/Director",
    "research associate/faculty": "Principal Investigator/Director",
    "research consultant": "Research Associate/Assistant",
    "research coordinator": "Administrative Staff",
    "research director": "Principal Investigator/Director",
    "research fellow": "Post-Doc",
    "research fellow - postdoc": "Post-Doc",
    "research fellow / postdoc": "Post-Doc",
    "research from c. n. r. s.": "Research Associate/Assistant",
    "research manager-biosciences": "Lab Manager",
    "research molecular biologist": "Research Associate/Assistant",
    "research scholar": "Research Associate/Assistant",
    "research scientist": "Senior Scientist",
    "research specialist": "Research Associate/Assistant",
    "research student": "Graduate Student",
    "research tech": "Technician",
    "research technician": "Technician",
    "research_professor": "Principal Investigator/Director",
    "researcher": "Research Associate/Assistant",
    "researcher fellow": "Post-Doc",
    "science director": "Principal Investigator/Director",
    "scientific curator": "Professional Biocurator",
    "scientific database curator": "Professional Biocurator",
    "scientific officer": "Administrative Staff",
    "scientist": "Senior Scientist",
    "scientist fellow": "Post-Doc",
    "scientist ii": "Senior Scientist",
    "senior computer biologist/database curator": "Professional Biocurator",
    "senior database curator": "Professional Biocurator",
    "senior fellow": "Senior Scientist",
    "senior group leader": "Group Leader",
    "senior investigator": "Senior Scientist",
    "senior investigator/professor": "Principal Investigator/Director",
    "senior lecturer": "Principal Investigator/Director",
    "senior lecturer in biochemistry": "Principal Investigator/Director",
    "senior member": "Lab Member",
    "senior post-doc": "Post-Doc",
    "senior research associate": "Senior Scientist",
    "senior research microbiologist": "Senior Scientist",
    "senior research scientist": "Senior Scientist",
    "senior research technician": "Technician",
    "senior researcher": "Senior Scientist",
    "senior researcher, cnr": "Senior Scientist",
    "senior scientist": "Senior Scientist",
    "senior scientist & group leader": "Group Leader",
    "senior staff scientist": "Senior Scientist",
    "service head": "Principal Investigator/Director",
    "smits professor of cell biology": "Principal Investigator/Director",
    "snf professor": "Principal Investigator/Director",
    "software developer": "Programmer/Bioinformatician",
    "sr research investigator": "Senior Scientist",
    "sr. scientist i": "Senior Scientist",
    "staff researcher": "Research Associate/Assistant",
    "staff scientist": "Senior Scientist",
    "student": "Graduate Student",
    "swiss-prot group": "Lab Member",
    "teaching technician": "Technician",
    "technician": "Technician",
    "tenure-track investigator": "Senior Scientist",
    "thad l. beyle distinguished professor": "Principal Investigator/Director",
    "ucsf professor, hhmi investigator": "Principal Investigator/Director",
    "undergraduate": "Undergraduate Student",
    "undergraduate student": "Undergraduate Student",
    "university lecturer": "Principal Investigator/Director",
    "university professor": "Principal Investigator/Director",
    "university teacher": "Research Associate/Assistant",
    "vice provost": "Administrative Staff",
    "visiting post-doctoral scholar": "Post-Doc",
    "visiting scholar": "Research Associate/Assistant",
    "wellcome principal fellow": "Post-Doc",
}


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

    # 4. (removed) A "cleanup" DELETE of non-loader laboratory_person rows once ran
    #    here. It was dev-only test-data hygiene, justified by a survey that found prod
    #    100% written by the load_sgd_colleagues loader. That assumption is now stale:
    #    a real curator-created membership exists on prod (laboratory_person 5620 —
    #    Ceri Van Slyke in the Elizabeth Grayhack Lab, created 2026-07-29, after the
    #    survey). Deleting non-loader rows would destroy that real data, so the step is
    #    gone; the mapping below only touches rows with a non-blank lab_position. (SCRUM-6311)

    # 5. map each non-blank lab_position into the controlled vocabulary using the
    #    hardcoded curator mapping (_LAB_POSITION_TEXT_TO_TERM above). Fail-closed:
    #    any value not in the mapping halts the migration so the operator can add it.
    lab_vocab_id = conn.execute(sa.text(
        "SELECT vocabulary_abc_id FROM vocabulary_abc WHERE vocabulary = :v"
    ), {"v": LAB_POSITION_VOCAB}).scalar_one()
    term_id_by_name = {
        name: tid
        for tid, name in conn.execute(sa.text(
            "SELECT vocabulary_term_abc_id, name FROM vocabulary_term_abc "
            "WHERE vocabulary_abc_id = :vid"
        ), {"vid": lab_vocab_id})
    }

    # 'Principal Investigator/Director' is a sentinel, NOT a seeded term: PI status is
    # carried by the laboratory_person.is_pi timestamp, so that bucket sets is_pi
    # instead of a role FK.
    pi_director = "Principal Investigator/Director"

    # Every non-sentinel target the mapping points at must be a seeded term.
    missing_terms = sorted(
        n for n in set(_LAB_POSITION_TEXT_TO_TERM.values())
        if n != pi_director and n not in term_id_by_name
    )
    if missing_terms:
        raise RuntimeError(
            f"lab_position mapping targets terms not seeded in the vocabulary: {missing_terms}"
        )

    # Fail-closed: every distinct non-blank lab_position value must be in the mapping.
    present = [
        r[0] for r in conn.execute(sa.text(
            "SELECT DISTINCT lower(btrim(lab_position)) FROM laboratory_person "
            "WHERE lab_position IS NOT NULL AND btrim(lab_position) <> ''"
        ))
    ]
    unmapped = sorted(v for v in present if v not in _LAB_POSITION_TEXT_TO_TERM)
    if unmapped:
        raise RuntimeError(
            "lab_position values absent from the curator mapping "
            f"(add them and re-run): {unmapped}"
        )

    # Fail-closed: a PI-bucket membership already marked former_pi or alum but with
    # is_pi NULL is contradictory -- the is_pi stamp below would present a departed or
    # former PI as a sitting one. Abort and let a curator resolve it rather than guess.
    # (0 such rows on prod; a stray one must be cleared before the migration runs.)
    pi_keys = [k for k, v in _LAB_POSITION_TEXT_TO_TERM.items() if v == pi_director]
    conflict = conn.execute(
        sa.text(
            "SELECT count(*) FROM laboratory_person "
            "WHERE is_pi IS NULL AND (former_pi IS NOT NULL OR alum IS NOT NULL) "
            "AND lower(btrim(lab_position)) IN :keys"
        ).bindparams(sa.bindparam("keys", expanding=True)),
        {"keys": pi_keys},
    ).scalar_one()
    if conflict:
        raise RuntimeError(
            f"{conflict} PI-title laboratory_person row(s) are former_pi/alum with "
            "is_pi NULL; resolve the is_pi/former_pi/alum contradiction before migrating."
        )

    for value in present:
        target = _LAB_POSITION_TEXT_TO_TERM[value]
        if target == pi_director:
            # PI/Director bucket: record PI status via is_pi (from the row's own
            # date_created), only where unset; leave the role FK null. created_by /
            # updated_by are untouched, so the row keeps its original user.
            conn.execute(sa.text(
                "UPDATE laboratory_person SET is_pi = date_created "
                "WHERE lower(btrim(lab_position)) = :v AND is_pi IS NULL"
            ), {"v": value})
        else:
            conn.execute(sa.text(
                "UPDATE laboratory_person SET lab_position_vocab_term_abc_id = :tid "
                "WHERE lower(btrim(lab_position)) = :v"
            ), {"tid": term_id_by_name[target], "v": value})

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

    # re-add lab_position String col, left empty. The original free-text is NOT
    # recoverable: the upgrade mapped it into the CV FK / is_pi and did not archive
    # it, so this downgrade can only restore the (now-empty) column.
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
    # NOT reversed (one-way): the lab_position -> CV FK / is_pi mapping (the original
    # free-text was not preserved, so it cannot be reconstructed on downgrade).
