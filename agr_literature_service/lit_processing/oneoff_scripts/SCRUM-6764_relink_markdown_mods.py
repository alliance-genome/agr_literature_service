"""Give Markdown linked to no MOD the MODs of the file it was converted from (SCRUM-6764).

A converted file linked to no MOD (a referencefile_mod row with mod_id NULL)
can be downloaded by every signed-in curator and observer, whatever the
paper's license. That is right for Markdown made from a file that is itself
linked to no MOD (PMC's nXML, PDFs and supplements), but the old batch
conversion (oneoff_scripts/nxml2md.py --source tei) uploaded the Markdown it
made from GROBID TEI of MOD-supplied PDFs with no MOD as well. On stage that
left about 95,000 converted_merged_main files, e.g. FBrf0233986_tei from FB's
FBrf0233986.pdf, open to every MOD.

A converted file is named after its source: the source's display_name plus
"_<method>" (_tei, _nxml, _merged, _grobid, ...), with a "_<n>" counter in
between when the upload had to make the name unique (10543952_1_tei from
10543952.pdf). For each Markdown file
linked to no MOD, this script finds the non-Markdown file of the same paper
with that display_name and:

- relinks it to the source's MODs when every link of the source names a MOD;
- leaves it as is when the source itself is linked to no MOD (open anyway),
  or when no source file can be found.

Embedding parquets made from a relinked Markdown file copy its access, so they
are re-synced after each change (resync_derived_embeddings).

Usage:
    python SCRUM-6764_relink_markdown_mods.py --dry-run
    python SCRUM-6764_relink_markdown_mods.py
"""
import argparse
import logging
from collections import Counter
from typing import Dict, List, NamedTuple, Optional, Set, Tuple

from sqlalchemy import text

from agr_literature_service.api.crud.referencefile_mod_utils import resync_derived_embeddings
from agr_literature_service.api.models import ReferencefileModAssociationModel, ReferencefileModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

# basicConfig, not fileConfig: fileConfig disables every logger already
# created, which breaks log-capturing tests that import the script
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCRIPT_NAME = "SCRUM-6764_relink_markdown_mods"
BATCH_COMMIT_SIZE = 500

RELINK = "relink"
OPEN_SOURCE = "source linked to no MOD"
NO_SOURCE = "no source file found"

# One row per (Markdown file, source file, source link); a Markdown file with no
# matching source gets a single row with NULL source columns.
CANDIDATES_SQL = """
    SELECT md.id, md.display_name, s.id AS source_id, sm.mod_id AS source_mod_id,
           EXISTS (SELECT 1 FROM referencefile_mod sm2
                   WHERE sm2.referencefile_id = s.id) AS source_has_links
    FROM referencefile md
    LEFT JOIN referencefile s
           ON s.reference_id = md.reference_id
          AND s.file_extension <> 'md'
          AND s.display_name IN (regexp_replace(md.display_name, '_[^_]*$', ''),
                                 regexp_replace(md.display_name, '_[0-9]+_[^_]*$', ''))
    LEFT JOIN referencefile_mod sm ON sm.referencefile_id = s.id
    WHERE md.file_extension = 'md'
      AND EXISTS (SELECT 1 FROM referencefile_mod x
                  WHERE x.referencefile_id = md.id AND x.mod_id IS NULL)
    ORDER BY md.id
"""


class Plan(NamedTuple):
    referencefile_id: int
    display_name: str
    action: str
    mod_ids: Tuple[int, ...]


def decide(source_links: Dict[int, List[Optional[int]]]) -> Tuple[str, Tuple[int, ...]]:
    """(action, MOD ids) for one Markdown file, given {source file id: its mod_ids}.

    The Markdown inherits the source's visibility: a source with any no-MOD
    link (or no link at all) is open to everyone, so the Markdown stays open."""
    if not source_links:
        return NO_SOURCE, ()
    mod_ids: Set[int] = set()
    for links in source_links.values():
        if not links or any(mod_id is None for mod_id in links):
            return OPEN_SOURCE, ()
        mod_ids.update(m for m in links if m is not None)
    return RELINK, tuple(sorted(mod_ids))


def plan_relinks(db) -> List[Plan]:
    sources: Dict[int, Dict[int, List[Optional[int]]]] = {}
    names: Dict[int, str] = {}
    for md_id, display_name, source_id, source_mod_id, source_has_links in db.execute(text(CANDIDATES_SQL)):
        names[md_id] = display_name
        md_sources = sources.setdefault(md_id, {})
        if source_id is None:
            continue
        links = md_sources.setdefault(source_id, [])
        if source_has_links:
            links.append(source_mod_id)
    plans = []
    for md_id, md_sources in sources.items():
        action, mod_ids = decide(md_sources)
        plans.append(Plan(md_id, names[md_id], action, mod_ids))
    return plans


def suffix_of(display_name: str) -> str:
    return display_name.rsplit("_", 1)[-1] if "_" in display_name else "(none)"


def report(db, plans: List[Plan]) -> None:
    mod_names = dict(db.execute(text("SELECT id, abbreviation FROM mod")).fetchall())
    logger.info(f"{len(plans)} Markdown file(s) are linked to no MOD")
    by_action = Counter(p.action for p in plans)
    for action, count in by_action.most_common():
        logger.info(f"  {action}: {count}")
        suffixes = Counter(suffix_of(p.display_name) for p in plans if p.action == action)
        logger.info("    by suffix: " + ", ".join(f"_{s} {n}" for s, n in suffixes.most_common(8)))
    relinks = [p for p in plans if p.action == RELINK]
    by_mods = Counter(",".join(mod_names[m] for m in p.mod_ids) for p in relinks)
    logger.info("  relink to: " + ", ".join(f"{m} {n}" for m, n in by_mods.most_common()))
    for p in relinks[:5]:
        logger.info(f"  e.g. referencefile {p.referencefile_id} {p.display_name} -> "
                    f"{','.join(mod_names[m] for m in p.mod_ids)}")
    if relinks:
        embeddings = db.execute(text(
            "SELECT count(*) FROM embedding_file WHERE source_referencefile_id = ANY(:ids)"),
            {"ids": [p.referencefile_id for p in relinks]}).scalar()
        logger.info(f"  embedding file(s) made from the relinked Markdown, re-synced too: {embeddings}")


def apply_relink(db, plan: Plan) -> None:
    """Replace the Markdown file's no-MOD link with links to the source's MODs."""
    links = db.query(ReferencefileModAssociationModel).filter_by(
        referencefile_id=plan.referencefile_id).all()
    existing = {link.mod_id for link in links if link.mod_id is not None}
    for link in links:
        if link.mod_id is None:
            db.delete(link)
    for mod_id in plan.mod_ids:
        if mod_id not in existing:
            db.add(ReferencefileModAssociationModel(referencefile_id=plan.referencefile_id, mod_id=mod_id))
    db.flush()
    # the resync reads the Markdown file's links through this relationship
    referencefile = db.get(ReferencefileModel, plan.referencefile_id)
    if referencefile is not None:
        db.expire(referencefile, ["referencefile_mods"])
    resync_derived_embeddings(db, plan.referencefile_id)


def relink_markdown_mods(db, dry_run: bool = False) -> int:
    """Returns the number of Markdown files relinked (would be, on a dry run)."""
    plans = plan_relinks(db)
    report(db, plans)
    relinks = [p for p in plans if p.action == RELINK]
    if dry_run:
        logger.info(f"DRY RUN: would relink {len(relinks)} Markdown file(s)")
        return len(relinks)
    for count, plan in enumerate(relinks, start=1):
        apply_relink(db, plan)
        if count % BATCH_COMMIT_SIZE == 0:
            db.commit()
            logger.info(f"relinked {count} of {len(relinks)}")
    db.commit()
    logger.info(f"Relinked {len(relinks)} Markdown file(s)")
    return len(relinks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Give no-MOD Markdown the MODs of its source file")
    parser.add_argument('--dry-run', action='store_true', help="Report what would change, change nothing")
    args = parser.parse_args()

    session = create_postgres_session(False)
    set_global_user_id(session, SCRIPT_NAME)
    try:
        relink_markdown_mods(session, dry_run=args.dry_run)
    finally:
        session.close()
