"""
SCRUM-6513_backfill_author_emails.py
====================================

Backfill ``author.email_address`` from PubMed for references added to a MOD
corpus since ``--since`` (default 2024-01-01): the reference has a
``mod_corpus_association`` row with ``corpus = True`` for any MOD whose
``date_created`` is on or after that date. This is the corpus-entry rule of
the reference-level email loader (get_emails_from_pubmed_pmc.py, SCRUM-6430),
whose 2024 cutoff it shares, but without that loader's restriction to the
email-extraction MODs: author emails are author metadata for every MOD.

New references get the email when the PubMed search loads them, and the
PubMed update fills it in on the references it processes; this script catches
up the existing ones. It uses the same pipeline steps as the PubMed update:
``download_pubmed_xml`` into ``XML_PATH/pubmed_xml/`` (PMIDs already there are
reused, not re-downloaded), ``generate_json`` into ``XML_PATH/pubmed_json/``,
so each author's email is extracted exactly as ingest does
(``xml_to_json.extract_author_email``).

Per reference, emails are written only when it is safe to match authors by
position:

- the PubMed author list equals the one in the ABC (same names in the same
  order, ``authors_lists_are_equal``); otherwise the reference is counted as
  ``author_mismatch`` and left for the regular PubMed update, which reloads
  its authors (with emails) when they differ;
- no curator has touched the reference's authors (the same gate the PubMed
  update uses); such references are counted as ``curator_managed``.

Only ``email_address`` changes (``sync_author_emails``): author ids, order and
every other field stay as they are, and an existing email is never cleared.

Safe by default: without ``--commit`` the script reports what it would set and
rolls every change back. ``--limit N`` stops after N references.
"""
import argparse
import json
import logging
from os import environ, path
from typing import Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from agr_literature_service.api.models import AuthorModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import (
    download_pubmed_xml,
)
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import generate_json
from agr_literature_service.lit_processing.data_ingest.utils.author import (
    Author,
    authors_lists_are_equal,
)
from agr_literature_service.lit_processing.data_ingest.utils.db_write_utils import (
    _reference_touched_by_curator,
    sync_author_emails,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_SINCE = "2024-01-01"
# PMIDs per download / JSON generation / commit round (the PubMed update's
# download slice size).
CHUNK_SIZE = 5000

OUTCOMES = ("updated", "already_set", "no_email_in_pubmed", "author_mismatch",
            "curator_managed", "no_authors", "not_found_in_pubmed")


def candidate_references(db: Session, since: str) -> List[Tuple[int, str]]:
    """(reference_id, pmid) of references added to any MOD's corpus on or
    after ``since`` (an in-corpus mod_corpus_association created then) that
    have a current PMID and at least one ordered author."""
    rows = db.execute(text(
        "SELECT r.reference_id, MIN(cr.curie) "
        "FROM   reference r "
        "JOIN   cross_reference cr ON cr.reference_id = r.reference_id "
        "       AND cr.curie_prefix = 'PMID' AND cr.is_obsolete IS FALSE "
        "WHERE  EXISTS (SELECT 1 FROM mod_corpus_association mca "
        "               WHERE mca.reference_id = r.reference_id "
        "               AND mca.corpus IS TRUE "
        "               AND mca.date_created >= :since) "
        "AND    EXISTS (SELECT 1 FROM author a WHERE a.reference_id = r.reference_id "
        "               AND a.author_order IS NOT NULL) "
        "GROUP  BY r.reference_id "
        "ORDER  BY r.reference_id"
    ), {"since": since}).fetchall()
    return [(row[0], row[1].replace("PMID:", "")) for row in rows]


def _db_author_dicts(db: Session, reference_id: int) -> List[Dict]:
    """The reference's ordered authors in the shape Author.load_from_db_dict reads."""
    return [
        {"name": a.name, "first_name": a.first_name, "last_name": a.last_name,
         "first_initial": a.first_initial, "author_order": a.author_order,
         "orcid": a.orcid, "affiliations": a.affiliations or [],
         "email_address": a.email_address}
        for a in db.query(AuthorModel)
        .filter(AuthorModel.reference_id == reference_id,
                AuthorModel.author_order.isnot(None))
        .order_by(AuthorModel.author_order)
    ]


def backfill_reference(db: Session, reference_id: int,
                       pubmed_authors: Optional[List[Dict]]) -> Tuple[str, int]:
    """Set the reference's author emails from its PubMed authors. Returns
    (outcome, number of authors changed); outcome is one of OUTCOMES."""
    if not pubmed_authors:
        return "no_authors", 0
    authors_from_json = Author.load_list_of_authors_from_json_dict_list(pubmed_authors)
    if not any(author.email for author in authors_from_json):
        return "no_email_in_pubmed", 0
    if _reference_touched_by_curator(db, reference_id):
        return "curator_managed", 0
    authors_from_db = Author.load_list_of_authors_from_db_dict_list(_db_author_dicts(db, reference_id))
    if not authors_lists_are_equal(authors_from_json, authors_from_db):
        return "author_mismatch", 0
    changed = sync_author_emails(db, reference_id, authors_from_json)
    return ("updated" if changed else "already_set"), changed


def _pubmed_authors(json_dir: str, pmid: str) -> Optional[List[Dict]]:
    """Authors from the generated PubMed JSON, or None when PubMed returned
    no record for the PMID."""
    json_file = path.join(json_dir, pmid + ".json")
    if not path.exists(json_file):
        return None
    with open(json_file) as fh:
        return json.load(fh).get("authors") or []


def backfill_author_emails(since: str = DEFAULT_SINCE, commit: bool = False,
                           limit: Optional[int] = None) -> Dict[str, int]:  # pragma: no cover
    db = create_postgres_session(False)
    set_global_user_id(db, path.basename(__file__).replace(".py", ""))
    references = candidate_references(db, since)
    if limit:
        references = references[:limit]
    logger.info("%s reference(s) added to a MOD corpus since %s with a PMID and authors%s",
                len(references), since, "" if commit else " (dry run)")

    json_dir = path.join(environ.get("XML_PATH", ""), "pubmed_json")
    counts: Dict[str, int] = {outcome: 0 for outcome in OUTCOMES}
    counts["authors_changed"] = 0
    for start in range(0, len(references), CHUNK_SIZE):
        chunk = references[start:start + CHUNK_SIZE]
        pmids = [pmid for _, pmid in chunk]
        download_pubmed_xml(pmids)
        generate_json(pmids, [], set())
        for reference_id, pmid in chunk:
            authors = _pubmed_authors(json_dir, pmid)
            if authors is None:
                counts["not_found_in_pubmed"] += 1
                continue
            try:
                outcome, changed = backfill_reference(db, reference_id, authors)
            except Exception as e:  # noqa: BLE001 - one bad reference must not stop the run
                db.rollback()
                logger.error("PMID:%s reference_id=%s failed: %s", pmid, reference_id, e)
                continue
            counts[outcome] += 1
            counts["authors_changed"] += changed
            if changed:
                logger.info("%s PMID:%s reference_id=%s: %s author email(s)",
                            "SET" if commit else "WOULD SET", pmid, reference_id, changed)
        if commit:
            db.commit()
        else:
            db.rollback()
        logger.info("processed %s/%s references", min(start + CHUNK_SIZE, len(references)), len(references))

    logger.info("done%s: %s", "" if commit else " (dry run)",
                ", ".join(f"{key}={value}" for key, value in counts.items()))
    db.close()
    return counts


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill author.email_address from PubMed (SCRUM-6513). Dry run unless --commit.")
    parser.add_argument("--since", default=DEFAULT_SINCE,
                        help=f"references added to any MOD corpus on or after this date (default {DEFAULT_SINCE})")
    parser.add_argument("--commit", action="store_true", help="write the emails (default: report only)")
    parser.add_argument("--limit", type=int, default=None, help="stop after N references")
    return parser.parse_args(argv)


if __name__ == "__main__":  # pragma: no cover
    args = parse_args()
    backfill_author_emails(since=args.since, commit=args.commit, limit=args.limit)
