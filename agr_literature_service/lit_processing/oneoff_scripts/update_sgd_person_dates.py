"""
Set the dates of the SGD colleague data to when SGD created each colleague.

load_sgd_colleagues loaded the SGD colleagues into the person tables in one
run, so every row it created carries the load date (2026-06-27 on stage)
instead of the colleague's SGD date_created. This script reads the SGD
colleague_id -> date_created export and, for the rows load_sgd_colleagues
created, sets date_created and db_date_created to that date. date_updated and
db_date_updated are set to it as well when load_sgd_colleagues was also the
last to update the row; a row someone else updated since keeps its update
dates. A database still on the schema before SCRUM-6719 (no db_date_* columns,
created_by / updated_by holding the user id string, <table>_id primary keys)
is handled too.

The colleague is matched through person_cross_reference
(SGD:Colleague_<colleague_id>). Tables and the date each row gets:

  person, person_cross_reference, person_name, person_email, person_note,
  person_institution        the person's colleague date
  laboratory, laboratory_cross_reference
                            the head of lab's colleague date (SGD:Lab_<colleague_id>)
  laboratory_person         the later of the person's and the lab's date
  person_lineage            the later of the two persons' dates

A person that was in ABC before the SGD load, which only attached the
colleague to it, gets the colleague's date_created (and db_date_created) and
keeps its update dates. Other rows created by anyone else, such as the SGD
rows curators added, are not changed. The updates are plain SQL:
an ORM update would stamp db_date_updated with the current time, and these are
corrections of the load, so no history rows are written for them.

    python update_sgd_person_dates.py --datafile sgd_colleague_id_to_date_created.tsv           # dry run
    python update_sgd_person_dates.py --datafile sgd_colleague_id_to_date_created.tsv --apply
"""

import argparse
import csv
import logging
from datetime import datetime
from typing import Dict, List, Tuple

from sqlalchemy import text

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

LOADER_USER = "load_sgd_colleagues"

# table -> its person id column ({pk}: the table's primary key); each row gets
# its person's colleague date
PERSON_TABLES = {
    "person": "{pk}",
    "person_cross_reference": "person_id",
    "person_name": "person_id",
    "person_email": "person_id",
    "person_note": "person_id",
    "person_institution": "person_id",
}


def loader_conditions(db, columns, loader_user):
    """(created by the loader, last updated by the loader) SQL conditions on
    alias t. The SCRUM-6719 schema holds agent.id in created_by_id /
    updated_by_id; before it (prod for now) created_by / updated_by hold the
    user id string itself."""
    if "created_by_id" not in columns:
        return "t.created_by = :loader", "t.updated_by = :loader"
    if db.execute(text("SELECT 1 FROM agent WHERE automation_username = :loader"),
                  {"loader": loader_user}).scalar() is None:
        raise SystemExit(f"no agent for {loader_user}")
    agent = "(SELECT id FROM agent WHERE automation_username = :loader)"
    return f"t.created_by_id = {agent}", f"t.updated_by_id = {agent}"


def set_dates(columns, loader_row) -> str:
    """date_created (and the LinkML db_date_created where the table has it)
    always; the update dates only for a row the loader created and is still
    the last to have updated."""
    sets = ["date_created = d.sgd_date",
            f"date_updated = CASE WHEN {loader_row} THEN d.sgd_date ELSE t.date_updated END"]
    if "db_date_created" in columns:
        sets += ["db_date_created = d.sgd_date",
                 f"db_date_updated = CASE WHEN {loader_row} THEN d.sgd_date ELSE t.db_date_updated END"]
    return ", ".join(sets)


def read_dates(datafile) -> List[Tuple[int, datetime]]:
    """[(colleague_id, date_created)] from the SGD export (colleague_id, email, date_created)."""
    with open(datafile, newline="") as fh:
        return [(int(row["colleague_id"]), datetime.strptime(row["date_created"], "%Y-%m-%d %H:%M:%S"))
                for row in csv.DictReader(fh, delimiter="\t")]


def update_dates(db, colleague_dates: List[Tuple[int, datetime]], loader_user=LOADER_USER) -> Dict[str, int]:
    """Set the dates of the rows loader_user created; {table: rows updated}."""
    db.execute(text("CREATE TEMP TABLE sgd_colleague_date (colleague_id BIGINT PRIMARY KEY, "
                    "sgd_date TIMESTAMP NOT NULL) ON COMMIT DROP"))
    db.execute(text("INSERT INTO sgd_colleague_date VALUES (:c, :d)"),
               [{"c": c, "d": d} for c, d in colleague_dates])
    db.execute(text(
        "CREATE TEMP TABLE sgd_person_date ON COMMIT DROP AS "
        "SELECT x.person_id, c.sgd_date FROM person_cross_reference x "
        "JOIN sgd_colleague_date c ON x.curie = 'SGD:Colleague_' || c.colleague_id "
        "WHERE x.curie_prefix = 'SGD' AND x.person_id IS NOT NULL"))
    db.execute(text(
        "CREATE TEMP TABLE sgd_lab_date ON COMMIT DROP AS "
        "SELECT x.laboratory_id, c.sgd_date FROM laboratory_cross_reference x "
        "JOIN sgd_colleague_date c ON x.curie = 'SGD:Lab_' || c.colleague_id "
        "WHERE x.laboratory_id IS NOT NULL"))
    # table -> (the date each row gets, as d(id, sgd_date), and its key column);
    # {pk} is the table's primary key
    sources = {table: ("SELECT person_id AS id, sgd_date FROM sgd_person_date", column)
               for table, column in PERSON_TABLES.items()}
    sources["laboratory"] = ("SELECT laboratory_id AS id, sgd_date FROM sgd_lab_date", "{pk}")
    sources["laboratory_cross_reference"] = ("SELECT laboratory_id AS id, sgd_date FROM sgd_lab_date",
                                             "laboratory_id")
    # a membership or a relation dates from when both of its sides existed
    sources["laboratory_person"] = (
        "SELECT lp.{pk} AS id, GREATEST(p.sgd_date, l.sgd_date) AS sgd_date FROM laboratory_person lp "
        "LEFT JOIN sgd_person_date p ON p.person_id = lp.person_id "
        "LEFT JOIN sgd_lab_date l ON l.laboratory_id = lp.laboratory_id "
        "WHERE p.sgd_date IS NOT NULL OR l.sgd_date IS NOT NULL", "{pk}")
    sources["person_lineage"] = (
        "SELECT pl.{pk} AS id, GREATEST(s.sgd_date, o.sgd_date) AS sgd_date FROM person_lineage pl "
        "LEFT JOIN sgd_person_date s ON s.person_id = pl.person_subject_id "
        "LEFT JOIN sgd_person_date o ON o.person_id = pl.person_object_id "
        "WHERE s.sgd_date IS NOT NULL OR o.sgd_date IS NOT NULL", "{pk}")
    counts = {}
    for table, (source, key) in sources.items():
        columns = set(db.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = :t"), {"t": table}).scalars())
        pk = "id" if "id" in columns else f"{table}_id"
        created_by_loader, updated_by_loader = loader_conditions(db, columns, loader_user)
        loader_row = f"{created_by_loader} AND {updated_by_loader}"
        # a person in ABC before the SGD load still gets the colleague's date_created
        only_created_by_loader = "" if table == "person" else f" AND {created_by_loader}"
        updated = db.execute(text(
            f"UPDATE {table} t SET {set_dates(columns, loader_row)} "
            f"FROM ({source.format(pk=pk)}) d WHERE t.{key.format(pk=pk)} = d.id{only_created_by_loader} "
            f"RETURNING {loader_row}"), {"loader": loader_user}).scalars().all()
        counts[table] = len(updated)
        counts[f"{table} (date_created only)"] = updated.count(False)
    counts["unmatched colleague_ids"] = db.execute(text(
        "SELECT count(*) FROM sgd_colleague_date c WHERE NOT EXISTS ("
        "SELECT 1 FROM person_cross_reference x WHERE x.curie = 'SGD:Colleague_' || c.colleague_id)")).scalar()
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--datafile", required=True,
                        help="SGD export: colleague_id, email, date_created (tab separated)")
    parser.add_argument("--apply", action="store_true",
                        help="commit the changes (default is dry run)")
    args = parser.parse_args()

    colleague_dates = read_dates(args.datafile)
    logger.info(f"{len(colleague_dates)} colleague(s) in {args.datafile}")
    db = create_postgres_session(False)
    try:
        for name, count in update_dates(db, colleague_dates).items():
            logger.info(f"{name}: {count}")
        if args.apply:
            db.commit()
            logger.info("[APPLY] committed")
        else:
            db.rollback()
            logger.info("[DRY RUN] nothing committed")
    finally:
        db.close()


if __name__ == "__main__":
    main()
