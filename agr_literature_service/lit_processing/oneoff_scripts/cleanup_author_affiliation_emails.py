"""
cleanup_author_affiliation_emails.py
====================================

Remove email addresses from the stored author affiliations: PubMed appends
the author's address to the institution ("... Shanghai 200000, China.
Electronic address: name@domain.org."), and curators do not want it there.
The PubMed ingest now strips it (xml_to_json, strip_emails_from_affiliations);
this script cleans up the author rows loaded before. The PubMed update does not
do it by itself: affiliations are not part of its author comparison, so an
unchanged author list is never reloaded.

Per reference that has an address in any author's affiliations:

- each author's affiliations are replaced with the stripped ones (an
  affiliation that held only addresses is dropped; none left -> NULL);
- an author with no ``email_address`` first gets the one the PubMed ingest
  would attribute from the stored affiliations (``assign_author_emails``, per
  paper across its authors), so the address is not lost; an existing email is
  never changed.

Curator-edited references are cleaned up too (only the address goes), and
``updated_by`` / ``date_updated`` are left as they are, so the script does not
hide a curator's edit from the PubMed update's curator gate
(``_reference_touched_by_curator``).

Run it after the SCRUM-6513 backfill: ``--recheck-existing`` there reads the
addresses from the stored affiliations.

Safe by default: without ``--commit`` the script reports what it would change
and rolls every change back. ``--limit N`` stops after N references.
"""
import argparse
import logging
from os import path
from typing import Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from agr_literature_service.api.models import AuthorModel
from agr_literature_service.api.models.audited_model import disable_set_updated_by_onupdate
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.utils.author_email_utils import (
    assign_author_emails,
    strip_emails_from_affiliations,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# references per commit round
CHUNK_SIZE = 5000


def references_with_affiliation_emails(db: Session) -> List[int]:
    """reference_ids with an '@' in any author's affiliations."""
    rows = db.execute(text(
        "SELECT DISTINCT reference_id FROM author "
        "WHERE  array_to_string(affiliations, ' ') LIKE '%@%' "
        "ORDER  BY reference_id"
    )).fetchall()
    return [row[0] for row in rows]


def cleanup_reference(db: Session, reference_id: int) -> Tuple[int, int]:
    """Strip the addresses from the reference's author affiliations, first
    giving an author without an email the one PubMed attributes to them.
    Returns (authors whose affiliations changed, authors given an email)."""
    authors = (db.query(AuthorModel)
               .filter(AuthorModel.reference_id == reference_id)
               .order_by(AuthorModel.author_order.nullslast(), AuthorModel.author_id)
               .all())
    ordered = [author for author in authors if author.author_order is not None]
    attributed = dict(zip(
        (author.author_id for author in ordered),
        assign_author_emails([(a.first_name, a.last_name, a.affiliations) for a in ordered])))
    affiliations_changed = emails_set = 0
    for author in authors:
        affiliations = strip_emails_from_affiliations(author.affiliations) or None
        email = attributed.get(author.author_id) if not author.email_address else None
        if affiliations == author.affiliations and not email:
            continue
        disable_set_updated_by_onupdate(author)
        if affiliations != author.affiliations:
            author.affiliations = affiliations
            affiliations_changed += 1
        if email:
            author.email_address = email
            emails_set += 1
    return affiliations_changed, emails_set


def cleanup_author_affiliation_emails(commit: bool = False,
                                      limit: Optional[int] = None) -> Dict[str, int]:  # pragma: no cover
    db = create_postgres_session(False)
    set_global_user_id(db, path.basename(__file__).replace(".py", ""))
    reference_ids = references_with_affiliation_emails(db)
    if limit:
        reference_ids = reference_ids[:limit]
    logger.info("%s reference(s) with an address in author affiliations%s",
                len(reference_ids), "" if commit else " (dry run)")
    counts = {"references_changed": 0, "affiliations_changed": 0, "emails_set": 0, "errors": 0}
    for done, reference_id in enumerate(reference_ids, start=1):
        try:
            with db.begin_nested():  # savepoint: a failure rolls back only this reference
                affiliations_changed, emails_set = cleanup_reference(db, reference_id)
        except Exception as e:  # noqa: BLE001 - one bad reference must not stop the run
            counts["errors"] += 1
            logger.error("reference_id=%s failed: %s", reference_id, e)
            continue
        if affiliations_changed or emails_set:
            counts["references_changed"] += 1
            counts["affiliations_changed"] += affiliations_changed
            counts["emails_set"] += emails_set
            logger.info("%s reference_id=%s: %s affiliation(s) cleaned, %s email(s) set",
                        "FIXED" if commit else "WOULD FIX", reference_id, affiliations_changed, emails_set)
        if done % CHUNK_SIZE == 0 or done == len(reference_ids):
            if commit:
                db.commit()
            else:
                db.rollback()
            logger.info("processed %s/%s references", done, len(reference_ids))
    logger.info("done%s: %s", "" if commit else " (dry run)",
                ", ".join(f"{key}={value}" for key, value in counts.items()))
    db.close()
    return counts


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove email addresses from author affiliations. Dry run unless --commit.")
    parser.add_argument("--commit", action="store_true", help="write the changes (default: report only)")
    parser.add_argument("--limit", type=int, default=None, help="stop after N references")
    return parser.parse_args(argv)


if __name__ == "__main__":  # pragma: no cover
    args = parse_args()
    cleanup_author_affiliation_emails(commit=args.commit, limit=args.limit)
