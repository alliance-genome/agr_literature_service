"""
SCRUM-6845: "email extraction needed" for the papers that entered a corpus since 2024.

Corpus entry now gives "email extraction needed" (ATP:0000358) to papers of the
MODs that extract emails (CORPUS_ENTRY_TAGS), and the weekly PubMed/PMC script
works through that job list instead of seeding the tag itself. This back-fills
the tag for corpus associations created since 2024-01-01 that have no email
extraction tag for the MOD, as if they had entered the corpus under the new rule.
Older papers are left alone.

It is the same direct write corpus entry makes, so no transition runs.

    python3 backfill_email_extraction_needed_scrum6845.py            # dry run
    python3 backfill_email_extraction_needed_scrum6845.py --commit
"""
import argparse
import logging
from os import path
from typing import Dict, List, Tuple

from sqlalchemy import text

from agr_literature_service.api.crud.utils.corpus_entry_tags import EMAIL_EXTRACTION_ENTRY_TAG, \
    EMAIL_EXTRACTION_MODS, EMAIL_EXTRACTION_NEEDED
from agr_literature_service.api.models import WorkflowTagModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format="%(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

SINCE = "2024-01-01"
EMAIL_STATES: List[str] = EMAIL_EXTRACTION_ENTRY_TAG[2]

PAIRS_SQL = text("""
SELECT mca.reference_id, mca.mod_id, m.abbreviation
FROM mod_corpus_association mca
JOIN mod m ON m.id = mca.mod_id
WHERE mca.corpus IS TRUE
  AND mca.date_created >= :since
  AND m.abbreviation = ANY(:mods)
  AND NOT EXISTS (SELECT 1 FROM workflow_tag wt
                  WHERE wt.reference_id = mca.reference_id AND wt.mod_id = mca.mod_id
                    AND wt.workflow_tag_id = ANY(:states))
ORDER BY m.abbreviation, mca.reference_id
""")


def find_pairs(db) -> List[Tuple[int, int, str]]:
    """(reference_id, mod_id, MOD) of the corpus associations since SINCE without an email tag"""
    rows = db.execute(PAIRS_SQL, {"since": SINCE, "mods": list(EMAIL_EXTRACTION_MODS),
                                  "states": EMAIL_STATES}).fetchall()
    return [(row[0], row[1], row[2]) for row in rows]


def backfill(db, commit: bool = False) -> Dict[str, int]:
    """add "email extraction needed" to every pair; returns the count per MOD. Commits only with commit."""
    counts: Dict[str, int] = {}
    for reference_id, mod_id, mod in find_pairs(db):
        db.add(WorkflowTagModel(reference_id=reference_id, mod_id=mod_id, workflow_tag_id=EMAIL_EXTRACTION_NEEDED))
        counts[mod] = counts.get(mod, 0) + 1
    if commit:
        db.commit()
    else:
        db.rollback()
    logger.info(f"{'Added' if commit else 'Dry run, would add'} 'email extraction needed': "
                f"{counts} (total {sum(counts.values())})")
    return counts


def main():  # pragma: no cover
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--commit", action="store_true", help="write the tags (default: dry run)")
    args = parser.parse_args()
    db = create_postgres_session(False)
    set_global_user_id(db, path.basename(__file__).replace(".py", ""))
    try:
        backfill(db, commit=args.commit)
    finally:
        db.close()


if __name__ == "__main__":  # pragma: no cover
    main()
