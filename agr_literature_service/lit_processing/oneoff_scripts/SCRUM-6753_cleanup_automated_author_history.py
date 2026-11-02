#!/usr/bin/env python3
"""
Keep one copy of the automated author history of each paper (SCRUM-6753).

Every PubMed / DQM update dropped and reloaded a paper's authors, and each
reload added another full copy of its author rows to author_version. From
SCRUM-6753 on, replace_authors_from_json deletes a paper's automated author
history before the reload (delete_automated_author_history), so only the
latest load is kept. This script brings the history written before that to
the same state.

For history written by automation (a transaction whose user is not backed by
a person), it keeps only the current version row of each existing author and
deletes the rest: the superseded rows (end_transaction_id set), the delete
rows, and the rows of authors that no longer exist. The last come from the
reload's bulk delete, which history tracking does not see, so it never closes
the deleted authors' rows. History written by a person is never touched.

It works on both the SCRUM-6719 schema (transaction.user_id -> agent.id,
author key id) and the schema before it, still on prod (transaction.user_id ->
users.user_id, author key author_id).
The deletes run in reference_id batches, each committed on its own. Run
VACUUM (FULL) on author_version afterwards to give the space back.

    python SCRUM-6753_cleanup_automated_author_history.py          # dry run: counts only
    python SCRUM-6753_cleanup_automated_author_history.py --apply
"""

import argparse
import logging
import time

from sqlalchemy import text

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BATCH_REFERENCES = 20000


def human_transaction(db) -> str:
    """Condition on alias t for a transaction made by a person: transaction.user_id
    points to agent.id since SCRUM-6719, and to users.user_id before it."""
    target = db.execute(text(
        "SELECT confrelid::regclass::text FROM pg_constraint "
        "WHERE conrelid = 'transaction'::regclass AND contype = 'f' "
        "AND pg_get_constraintdef(oid) LIKE 'FOREIGN KEY (user_id)%'")).scalar()
    if target == "agent":
        return "EXISTS (SELECT 1 FROM agent a WHERE a.id = t.user_id AND a.person_id IS NOT NULL)"
    if target == "users":
        return "EXISTS (SELECT 1 FROM users u WHERE u.user_id = t.user_id AND u.person_id IS NOT NULL)"
    raise SystemExit(f"transaction.user_id references {target!r}, expected agent or users")


def old_automated_rows(db) -> str:
    """Conditions (aliases av, t) for the author_version rows to delete in the
    reference_id range [:lo, :hi): automated rows other than an existing
    author's current version."""
    key = "id" if db.execute(text(
        "SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() "
        "AND table_name = 'author' AND column_name = 'id'")).scalar() else "author_id"
    return ("av.reference_id >= :lo AND av.reference_id < :hi "
            "AND (av.end_transaction_id IS NOT NULL OR av.operation_type = 2 "
            f"OR NOT EXISTS (SELECT 1 FROM author a WHERE a.{key} = av.{key})) "
            f"AND NOT {human_transaction(db)}")


def cleanup(db, apply: bool, batch_references: int = BATCH_REFERENCES) -> int:
    """Delete (or with apply False, count) the old automated author history; rows affected."""
    rows = old_automated_rows(db)
    low, high = db.execute(text("SELECT min(reference_id), max(reference_id) FROM author_version")).one()
    db.rollback()
    if low is None:
        return 0
    total = 0
    started = time.time()
    for lo in range(low, high + 1, batch_references):
        params = {"lo": lo, "hi": lo + batch_references}
        if apply:
            count = db.execute(text(
                "DELETE FROM author_version av USING transaction t "
                f"WHERE t.id = av.transaction_id AND {rows}"), params).rowcount
            db.commit()
        else:
            count = db.execute(text(
                "SELECT count(*) FROM author_version av JOIN transaction t ON t.id = av.transaction_id "
                f"WHERE {rows}"), params).scalar()
            db.rollback()
        total += count
        logger.info(f"reference_id {lo}-{lo + batch_references - 1}: {count} row(s), total {total} "
                    f"({time.time() - started:.0f}s)")
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true",
                        help="delete the rows (default is dry run: count them)")
    parser.add_argument("--batch-references", type=int, default=BATCH_REFERENCES,
                        help=f"reference_ids per batch (default {BATCH_REFERENCES})")
    args = parser.parse_args()

    db = create_postgres_session(False)
    try:
        before = db.execute(text("SELECT count(*) FROM author_version")).scalar()
        db.rollback()
        logger.info(f"author_version: {before} row(s)")
        total = cleanup(db, args.apply, args.batch_references)
        if args.apply:
            logger.info(f"[APPLY] deleted {total} old automated author_version row(s); "
                        f"{before - total} left. Run VACUUM (FULL) author_version to free the space.")
        else:
            logger.info(f"[DRY RUN] {total} of {before} author_version row(s) would be deleted")
    finally:
        db.close()


if __name__ == "__main__":
    main()
