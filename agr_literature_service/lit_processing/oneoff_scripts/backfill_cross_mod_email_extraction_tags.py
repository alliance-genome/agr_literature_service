"""
Back-populate email-extraction tags for cross-MOD-inconsistent papers (SCRUM-6166).

A paper can be text-converted ("file converted to text", ATP:0000163) for several
MODs but only have email-extraction tags for some of them. This happens when one
MOD's text conversion was re-run under the current workflow (firing the
email-extraction action) while another MOD's conversion predates the workflow and
was never re-triggered. Example: reference_id 904282 has "email extraction
complete" for FB and ZFIN but NO email tag for WB, even though WB has
"file converted to text".

This script finds, for each email-extraction MOD, references that:
  * have "file converted to text" (ATP:0000163) for that MOD, AND
  * are in that MOD's corpus, AND
  * have NO email-extraction tag for that MOD, BUT
  * DO have an email-extraction tag for some OTHER MOD,
and seeds "email extraction needed" (ATP:0000358) for the missing MOD so the
normal extraction pipeline (extract_emails.py) will process it.

The "has an email tag on another MOD" condition keeps this targeted: it only
touches papers already known to be in scope for email extraction, NOT the entire
historical corpus of text-converted papers. (For the broad case, see
fix_email_extraction_workflow_tags.py.)

MODs that perform email extraction are detected from the workflow_transition table,
so MODs that do not run email extraction (e.g. MGI, RGD) are excluded.

Usage:
    # dry run (default): report only
    python backfill_cross_mod_email_extraction_tags.py

    # apply
    python backfill_cross_mod_email_extraction_tags.py --commit

    # limit to specific MODs
    python backfill_cross_mod_email_extraction_tags.py --commit --mods WB,FB
"""
import argparse
import logging
from os import path
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy import text

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import WorkflowTagModel
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Email-extraction process (ATP:0000354) and its sub-states.
EMAIL_PROCESS_PARENT = "ATP:0000354"          # email extraction
EMAIL_IN_PROGRESS = "ATP:0000357"             # email extraction in progress
EMAIL_COMPLETE = "ATP:0000355"                # email extraction complete
EMAIL_FAILED = "ATP:0000356"                  # email extraction failed
EMAIL_NEEDED = "ATP:0000358"                  # email extraction needed

EMAIL_TAGS: List[str] = [
    EMAIL_PROCESS_PARENT,
    EMAIL_IN_PROGRESS,
    EMAIL_COMPLETE,
    EMAIL_FAILED,
    EMAIL_NEEDED,
]

FILE_CONVERTED_TO_TEXT = "ATP:0000163"


def get_email_extraction_mod_ids(db) -> Dict[int, str]:
    """Return {mod_id: abbreviation} for MODs that run email extraction, i.e.
    those that have any workflow_transition within the email-extraction process.
    """
    rows = db.execute(
        text(
            """
            SELECT DISTINCT m.mod_id, m.abbreviation
            FROM workflow_transition wt
            JOIN mod m ON m.mod_id = wt.mod_id
            WHERE wt.transition_from = ANY(:email_tags)
               OR wt.transition_to = ANY(:email_tags)
               OR array_to_string(wt.actions, ',') LIKE '%email extraction%'
            ORDER BY m.mod_id
            """
        ),
        {"email_tags": EMAIL_TAGS},
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def find_cross_mod_inconsistent(db, mod_ids: List[int]) -> List[Tuple[int, int]]:
    """Return [(mod_id, reference_id), ...] for references that have
    "file converted to text" and are in the MOD's corpus, lack an email tag for
    that MOD, but have an email tag for some OTHER MOD.
    """
    rows = db.execute(
        text(
            """
            WITH refs_with_email AS (
                SELECT DISTINCT reference_id, mod_id
                FROM workflow_tag
                WHERE workflow_tag_id = ANY(:email_tags)
            )
            SELECT t.mod_id, t.reference_id
            FROM workflow_tag t
            JOIN mod_corpus_association mca
              ON mca.reference_id = t.reference_id
             AND mca.mod_id = t.mod_id
             AND mca.corpus IS TRUE
            WHERE t.workflow_tag_id = :file_converted
              AND t.mod_id = ANY(:mod_ids)
              AND NOT EXISTS (
                    SELECT 1 FROM refs_with_email rwe
                    WHERE rwe.reference_id = t.reference_id
                      AND rwe.mod_id = t.mod_id
              )
              AND EXISTS (
                    SELECT 1 FROM refs_with_email rwe2
                    WHERE rwe2.reference_id = t.reference_id
                      AND rwe2.mod_id <> t.mod_id
              )
            ORDER BY t.mod_id, t.reference_id
            """
        ),
        {
            "email_tags": EMAIL_TAGS,
            "file_converted": FILE_CONVERTED_TO_TEXT,
            "mod_ids": mod_ids,
        },
    ).fetchall()
    return [(row[0], row[1]) for row in rows]


def backfill(db, pairs: List[Tuple[int, int]],
             mod_id_to_abbr: Dict[int, str], dry_run: bool) -> int:
    """Seed "email extraction needed" for each (mod_id, reference_id)."""
    added = 0
    for mod_id, reference_id in pairs:
        logger.info(
            "mod=%s ref=%s: add %s (email extraction needed)",
            mod_id_to_abbr.get(mod_id, mod_id), reference_id, EMAIL_NEEDED,
        )
        if not dry_run:
            db.add(WorkflowTagModel(
                reference_id=reference_id,
                mod_id=mod_id,
                workflow_tag_id=EMAIL_NEEDED,
            ))
        added += 1
    return added


def run(dry_run: bool, only_mods: Optional[Set[str]]) -> None:
    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    try:
        mod_id_to_abbr = get_email_extraction_mod_ids(db)
        if only_mods:
            mod_id_to_abbr = {
                mid: abbr for mid, abbr in mod_id_to_abbr.items()
                if abbr.upper() in only_mods
            }
        if not mod_id_to_abbr:
            logger.warning("No email-extraction-enabled MODs found; nothing to do.")
            return

        mod_ids = list(mod_id_to_abbr.keys())
        logger.info("=" * 70)
        logger.info("Cross-MOD email-extraction backfill (dry_run=%s)", dry_run)
        logger.info("MODs in scope: %s",
                    ", ".join(f"{a}({i})" for i, a in sorted(mod_id_to_abbr.items())))
        logger.info("=" * 70)

        pairs = find_cross_mod_inconsistent(db, mod_ids)
        logger.info("Found %s reference+mod pairs to back-populate", len(pairs))

        added = backfill(db, pairs, mod_id_to_abbr, dry_run)

        if dry_run:
            db.rollback()
            logger.info("DRY RUN - rolled back. Would add %s 'email extraction needed' tag(s).",
                        added)
            logger.info("Re-run with --commit to apply.")
        else:
            db.commit()
            logger.info("COMMITTED - added %s 'email extraction needed' tag(s).", added)
    except Exception:
        db.rollback()
        logger.exception("Backfill aborted; rolled back.")
        raise
    finally:
        db.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--commit", action="store_true",
                   help="Apply the fixes. Without this flag the script only reports (dry run).")
    p.add_argument("--mods", default=None,
                   help="Comma-separated MOD abbreviations to limit the backfill to (e.g. WB,FB).")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    logger.setLevel(getattr(logging, args.log_level.upper(), logging.INFO))
    only_mods = {m.strip().upper() for m in args.mods.split(",")} if args.mods else None
    run(dry_run=not args.commit, only_mods=only_mods)


if __name__ == "__main__":
    main()
