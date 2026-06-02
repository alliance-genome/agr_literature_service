"""
Fix email-extraction workflow-tag inconsistencies (SCRUM-6166).

Two classes of bad data are repaired, both stemming from the
``proceed_on_value::all::email extraction`` action being (re-)fired by backfill
scripts that re-set "file converted to text" (ATP:0000163):

  1. DUPLICATE  - a reference+mod has more than one tag in the email-extraction
                  process (e.g. "email extraction complete" AND
                  "email extraction needed"). The action used to insert a fresh
                  "needed" tag without checking for an existing one.
                  Fix: keep the most-progressed tag, delete the rest.

  2. MISSING    - a reference+mod went through text conversion (has
                  "file converted to text") and is in that mod's corpus, the mod
                  performs email extraction, yet it has NO email-extraction tag
                  at all (its text conversion predated the email-extraction
                  workflow / was never picked up by a backfill).
                  Fix: add "email extraction needed" so the normal extraction
                  pipeline will process it.

The lists are computed dynamically from the connected database, so the same
script can be run against dev / stage / prod where the affected papers differ.

Mods that perform email extraction are detected from the ``workflow_transition``
table (those with transitions in the email-extraction process), so mods that do
not run email extraction (e.g. MGI, RGD) are automatically excluded.

Usage:
    # dry run (default): report only, no DB changes
    python fix_email_extraction_workflow_tags.py

    # apply the fixes
    python fix_email_extraction_workflow_tags.py --commit

    # limit to specific mods
    python fix_email_extraction_workflow_tags.py --commit --mods WB,FB,ZFIN,SGD,XB
"""
import argparse
import logging
from collections import defaultdict
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

# When a reference+mod has more than one email tag, keep the most-progressed
# one (work already done wins) and delete the others. Highest priority first.
KEEP_PRIORITY: List[str] = [
    EMAIL_COMPLETE,
    EMAIL_FAILED,
    EMAIL_IN_PROGRESS,
    EMAIL_NEEDED,
    EMAIL_PROCESS_PARENT,
]


def get_email_extraction_mod_ids(db) -> Dict[int, str]:
    """Return {mod_id: abbreviation} for mods that run email extraction, i.e.
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


def find_duplicates(db, mod_ids: List[int]) -> Dict[Tuple[int, int], List[Tuple[int, str]]]:
    """Return {(mod_id, reference_id): [(reference_workflow_tag_id, workflow_tag_id), ...]}
    for every reference+mod that has more than one email-extraction tag.
    """
    rows = db.execute(
        text(
            """
            SELECT wt.mod_id, wt.reference_id, wt.reference_workflow_tag_id, wt.workflow_tag_id
            FROM workflow_tag wt
            WHERE wt.workflow_tag_id = ANY(:email_tags)
              AND wt.mod_id = ANY(:mod_ids)
              AND (wt.mod_id, wt.reference_id) IN (
                    SELECT mod_id, reference_id
                    FROM workflow_tag
                    WHERE workflow_tag_id = ANY(:email_tags)
                      AND mod_id = ANY(:mod_ids)
                    GROUP BY mod_id, reference_id
                    HAVING COUNT(*) > 1
              )
            ORDER BY wt.mod_id, wt.reference_id, wt.workflow_tag_id
            """
        ),
        {"email_tags": EMAIL_TAGS, "mod_ids": mod_ids},
    ).fetchall()

    grouped: Dict[Tuple[int, int], List[Tuple[int, str]]] = defaultdict(list)
    for mod_id, reference_id, rwt_id, tag_id in rows:
        grouped[(mod_id, reference_id)].append((rwt_id, tag_id))
    return grouped


def find_missing(db, mod_ids: List[int], since_date: str) -> List[Tuple[int, int]]:
    """Return [(mod_id, reference_id), ...] for references that have
    "file converted to text", are in that mod's corpus, run email extraction,
    but have no email-extraction tag at all.

    Scoped to references added to the corpus on/after ``since_date``. This is
    essential: email extraction was only rolled out for the recent corpus, so
    without the date scope this would (wrongly) flag every text-converted paper
    ever added (~hundreds of thousands) as "missing" and seed "needed" tags for
    all of them. The default matches the email-extraction rollout window.
    """
    rows = db.execute(
        text(
            """
            SELECT t.mod_id, t.reference_id
            FROM workflow_tag t
            JOIN mod_corpus_association mca
              ON mca.reference_id = t.reference_id
             AND mca.mod_id = t.mod_id
             AND mca.corpus IS TRUE
            WHERE t.workflow_tag_id = :file_converted
              AND t.mod_id = ANY(:mod_ids)
              AND mca.date_created >= :since_date
              AND NOT EXISTS (
                    SELECT 1 FROM workflow_tag e
                    WHERE e.reference_id = t.reference_id
                      AND e.mod_id = t.mod_id
                      AND e.workflow_tag_id = ANY(:email_tags)
              )
            ORDER BY t.mod_id, t.reference_id
            """
        ),
        {
            "file_converted": FILE_CONVERTED_TO_TEXT,
            "email_tags": EMAIL_TAGS,
            "mod_ids": mod_ids,
            "since_date": since_date,
        },
    ).fetchall()
    return [(row[0], row[1]) for row in rows]


def choose_tag_to_keep(tags: List[Tuple[int, str]]) -> Tuple[int, str]:
    """Pick the (reference_workflow_tag_id, workflow_tag_id) to keep based on
    KEEP_PRIORITY (most-progressed state wins). Falls back to the lowest
    reference_workflow_tag_id for any tag not in the priority list.
    """
    def sort_key(item: Tuple[int, str]):
        rwt_id, tag_id = item
        try:
            priority = KEEP_PRIORITY.index(tag_id)
        except ValueError:
            priority = len(KEEP_PRIORITY)
        return (priority, rwt_id)

    return sorted(tags, key=sort_key)[0]


def fix_duplicates(db, duplicates: Dict[Tuple[int, int], List[Tuple[int, str]]],
                   mod_id_to_abbr: Dict[int, str], dry_run: bool) -> int:
    """Collapse each duplicated reference+mod to a single email-extraction tag."""
    deleted = 0
    for (mod_id, reference_id), tags in sorted(duplicates.items()):
        keep_rwt_id, keep_tag = choose_tag_to_keep(tags)
        tag_set = sorted({t for _, t in tags})
        if tag_set != [EMAIL_COMPLETE, EMAIL_NEEDED]:
            # Anything other than the known complete+needed combo is unexpected;
            # surface it loudly so a human can confirm the keep choice.
            logger.warning(
                "UNEXPECTED duplicate combo for mod=%s ref=%s: %s (keeping %s)",
                mod_id_to_abbr.get(mod_id, mod_id), reference_id, tag_set, keep_tag,
            )
        for rwt_id, tag_id in tags:
            if rwt_id == keep_rwt_id:
                continue
            logger.info(
                "DUP  mod=%s ref=%s: delete %s (id=%s), keep %s",
                mod_id_to_abbr.get(mod_id, mod_id), reference_id, tag_id, rwt_id, keep_tag,
            )
            if not dry_run:
                obj = db.query(WorkflowTagModel).filter(
                    WorkflowTagModel.reference_workflow_tag_id == rwt_id
                ).one_or_none()
                if obj is not None:
                    db.delete(obj)
            deleted += 1
    return deleted


def fix_missing(db, missing: List[Tuple[int, int]],
                mod_id_to_abbr: Dict[int, str], dry_run: bool) -> int:
    """Add "email extraction needed" to references missing an email tag."""
    added = 0
    for mod_id, reference_id in missing:
        logger.info(
            "MISS mod=%s ref=%s: add %s (email extraction needed)",
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


def run(dry_run: bool, only_mods: Optional[Set[str]], since_date: str) -> None:
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
            logger.warning("No email-extraction-enabled mods found; nothing to do.")
            return

        mod_ids = list(mod_id_to_abbr.keys())
        logger.info("=" * 70)
        logger.info("Email-extraction workflow-tag fix (dry_run=%s)", dry_run)
        logger.info("Mods in scope: %s",
                    ", ".join(f"{a}({i})" for i, a in sorted(mod_id_to_abbr.items())))
        logger.info("Missing scope: corpus added on/after %s", since_date)
        logger.info("=" * 70)

        duplicates = find_duplicates(db, mod_ids)
        missing = find_missing(db, mod_ids, since_date)

        logger.info("Found %s reference+mod pairs with DUPLICATE email tags", len(duplicates))
        logger.info("Found %s reference+mod pairs MISSING an email tag", len(missing))

        deleted = fix_duplicates(db, duplicates, mod_id_to_abbr, dry_run)
        added = fix_missing(db, missing, mod_id_to_abbr, dry_run)

        if dry_run:
            db.rollback()
            logger.info("DRY RUN - rolled back. Would delete %s tag(s) and add %s tag(s).",
                        deleted, added)
            logger.info("Re-run with --commit to apply.")
        else:
            db.commit()
            logger.info("COMMITTED - deleted %s duplicate tag(s), added %s needed tag(s).",
                        deleted, added)
    except Exception:
        db.rollback()
        logger.exception("Fix aborted; rolled back.")
        raise
    finally:
        db.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--commit", action="store_true",
                   help="Apply the fixes. Without this flag the script only reports (dry run).")
    p.add_argument("--mods", default=None,
                   help="Comma-separated mod abbreviations to limit the fix to (e.g. WB,FB).")
    p.add_argument("--since-date", default="2025-01-01",
                   help="Only treat references added to the corpus on/after this date "
                        "(YYYY-MM-DD) as candidates for the MISSING fix. Matches the "
                        "email-extraction rollout window. Default: 2025-01-01.")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    logger.setLevel(getattr(logging, args.log_level.upper(), logging.INFO))
    only_mods = {m.strip().upper() for m in args.mods.split(",")} if args.mods else None
    run(dry_run=not args.commit, only_mods=only_mods, since_date=args.since_date)


if __name__ == "__main__":
    main()
