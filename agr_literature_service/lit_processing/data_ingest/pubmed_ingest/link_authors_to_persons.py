"""
link_authors_to_persons.py
==========================

SCRUM-5744: link PubMed authors to ABC person records automatically, after the
weekly PubMed update has refreshed author ORCIDs and emails.

Two passes, tried in order; an author linked by the first is not considered by
the second. Each pass stamps updated_by with its own label so curators can tell
how a link was made.

1. ORCID (updated_by = pubmed_orcid)
   - author.orcid equals a non-obsolete ORCID person_cross_reference
   - that ORCID appears on no other author of the same paper
   - author last name matches one of that person's names

2. Email (updated_by = pubmed_email)
   - author.email_address equals (case-insensitively) an email of exactly one
     person
   - that email appears on no other author of the same paper
   - the author's ORCID, if any, is unique on the paper and matches no person
   - the person has no ORCID that differs from the author's ORCID
   - author last name matches one of that person's names

In both passes an author is skipped when:
   - it is already linked, or is a person-only link row (no author_order)
   - the person is already an ordered author on the same paper
   - the author was linked to that same person before and has since been
     unlinked (a curator undid the link; do not redo it every week)

A person-only stub for the person on the same paper is absorbed, as the API does.
"""

import argparse
import logging
import unicodedata
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Set

from sqlalchemy import text
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.author_crud import link_person
from agr_literature_service.api.models import AuthorModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ORCID_USER = "pubmed_orcid"
EMAIL_USER = "pubmed_email"

# Unlinked ordered authors whose ORCID resolves to a person and is unique on the paper.
ORCID_CANDIDATES_SQL = text("""
    SELECT a.id, a.reference_id, a.last_name, a.name, pcr.person_id
    FROM author a
    JOIN person_cross_reference pcr
      ON pcr.curie_prefix = 'ORCID'
     AND pcr.obsolete = FALSE
     AND pcr.person_id IS NOT NULL
     AND UPPER(pcr.curie) = UPPER(a.orcid)
    WHERE a.person_id IS NULL
      AND a.author_order IS NOT NULL
      AND a.orcid IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM author b
          WHERE b.reference_id = a.reference_id
            AND b.id <> a.id
            AND UPPER(b.orcid) = UPPER(a.orcid)
      )
""")

# Unlinked ordered authors whose email matches a person's email, is unique on the
# paper, and whose ORCID (if any) is unique on the paper and matches no person.
EMAIL_CANDIDATES_SQL = text("""
    SELECT a.id, a.reference_id, a.last_name, a.name, pe.person_id, a.orcid
    FROM author a
    JOIN person_email pe
      ON LOWER(pe.email_address) = LOWER(BTRIM(a.email_address))
    WHERE a.person_id IS NULL
      AND a.author_order IS NOT NULL
      AND a.email_address IS NOT NULL
      AND BTRIM(a.email_address) <> ''
      AND NOT EXISTS (
          SELECT 1 FROM author b
          WHERE b.reference_id = a.reference_id
            AND b.id <> a.id
            AND LOWER(BTRIM(b.email_address)) = LOWER(BTRIM(a.email_address))
      )
      AND (
          a.orcid IS NULL
          OR (
              NOT EXISTS (
                  SELECT 1 FROM author b
                  WHERE b.reference_id = a.reference_id
                    AND b.id <> a.id
                    AND UPPER(b.orcid) = UPPER(a.orcid)
              )
              AND NOT EXISTS (
                  SELECT 1 FROM person_cross_reference x
                  WHERE x.curie_prefix = 'ORCID'
                    AND x.obsolete = FALSE
                    AND x.person_id IS NOT NULL
                    AND UPPER(x.curie) = UPPER(a.orcid)
              )
          )
      )
""")


def normalize_name(name: Optional[str]) -> str:
    """Fold case and accents and drop everything but letters and digits, so
    'García-López' and 'Garcia Lopez' compare equal."""
    if not name:
        return ""
    decomposed = unicodedata.normalize("NFKD", name)
    return "".join(c for c in decomposed if c.isalnum() and not unicodedata.combining(c)).casefold()


def _chunks(items: List[int], size: int = 10000) -> Iterable[List[int]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def get_person_last_names(db: Session, person_ids: Iterable[int]) -> Dict[int, Set[str]]:
    """Normalized last names (all name variants) for each person."""
    names: Dict[int, Set[str]] = defaultdict(set)
    ids = sorted(set(person_ids))
    for chunk in _chunks(ids):
        rows = db.execute(text("""
            SELECT person_id, last_name FROM person_name
            WHERE person_id = ANY(:ids)
        """), {"ids": chunk}).fetchall()
        for person_id, last_name in rows:
            normalized = normalize_name(last_name)
            if normalized:
                names[person_id].add(normalized)
    return names


def get_person_orcids(db: Session, person_ids: Iterable[int]) -> Dict[int, Set[str]]:
    """Upper-cased non-obsolete ORCID curies for each person."""
    orcids: Dict[int, Set[str]] = defaultdict(set)
    ids = sorted(set(person_ids))
    for chunk in _chunks(ids):
        rows = db.execute(text("""
            SELECT person_id, UPPER(curie) FROM person_cross_reference
            WHERE curie_prefix = 'ORCID'
              AND obsolete = FALSE
              AND person_id = ANY(:ids)
        """), {"ids": chunk}).fetchall()
        for person_id, curie in rows:
            orcids[person_id].add(curie)
    return orcids


def get_previously_unlinked(db: Session, author_ids: Iterable[int]) -> Set[tuple]:
    """(author_id, person_id) pairs that were linked at some point in the
    author's history. Every candidate is currently unlinked, so any such pair
    was undone, normally by a curator."""
    pairs: Set[tuple] = set()
    ids = sorted(set(author_ids))
    for chunk in _chunks(ids):
        rows = db.execute(text("""
            SELECT DISTINCT id, person_id FROM author_version
            WHERE id = ANY(:ids) AND person_id IS NOT NULL
        """), {"ids": chunk}).fetchall()
        pairs.update((author_id, person_id) for author_id, person_id in rows)
    return pairs


def last_name_matches(author_last_name: Optional[str], person_last_names: Set[str]) -> bool:
    normalized = normalize_name(author_last_name)
    return bool(normalized) and normalized in person_last_names


def find_orcid_matches(db: Session, counts: Dict[str, int]) -> Dict[int, int]:
    """author_id -> person_id for the ORCID pass."""
    rows = db.execute(ORCID_CANDIDATES_SQL).fetchall()
    counts["orcid_candidates"] = len(rows)
    last_names = get_person_last_names(db, [r[4] for r in rows])
    matches: Dict[int, int] = {}
    for author_id, _reference_id, last_name, name, person_id in rows:
        if not last_name_matches(last_name, last_names.get(person_id, set())):
            counts["orcid_last_name_mismatch"] += 1
            logger.debug(f"ORCID: last name mismatch author_id={author_id} name={name!r} person_id={person_id}")
            continue
        matches[author_id] = person_id
    return matches


def find_email_matches(db: Session, counts: Dict[str, int],
                       exclude_author_ids: Set[int]) -> Dict[int, int]:
    """author_id -> person_id for the email pass."""
    rows = [r for r in db.execute(EMAIL_CANDIDATES_SQL).fetchall() if r[0] not in exclude_author_ids]
    persons_by_author: Dict[int, Set[int]] = defaultdict(set)
    author_info: Dict[int, tuple] = {}
    for author_id, _reference_id, last_name, name, person_id, orcid in rows:
        persons_by_author[author_id].add(person_id)
        author_info[author_id] = (last_name, name, orcid)
    counts["email_candidates"] = len(persons_by_author)

    person_ids = {p for persons in persons_by_author.values() for p in persons}
    last_names = get_person_last_names(db, person_ids)
    person_orcids = get_person_orcids(db, person_ids)

    matches: Dict[int, int] = {}
    for author_id, persons in persons_by_author.items():
        last_name, name, orcid = author_info[author_id]
        if len(persons) > 1:
            counts["email_ambiguous_person"] += 1
            logger.debug(f"EMAIL: email shared by persons {sorted(persons)} author_id={author_id}")
            continue
        person_id = next(iter(persons))
        known_orcids = person_orcids.get(person_id, set())
        if orcid and known_orcids and orcid.upper() not in known_orcids:
            counts["email_orcid_conflict"] += 1
            logger.debug(f"EMAIL: author ORCID {orcid} differs from person_id={person_id} ORCID")
            continue
        if not last_name_matches(last_name, last_names.get(person_id, set())):
            counts["email_last_name_mismatch"] += 1
            logger.debug(f"EMAIL: last name mismatch author_id={author_id} name={name!r} person_id={person_id}")
            continue
        matches[author_id] = person_id
    return matches


def apply_links(db: Session, matches: Dict[int, int], label: str,
                counts: Dict[str, int], dry_run: bool) -> None:
    """Link each author to its person as ``label``, one commit per link."""
    set_global_user_id(db, label)
    unlinked_before = get_previously_unlinked(db, matches.keys())
    for author_id, person_id in sorted(matches.items()):
        if (author_id, person_id) in unlinked_before:
            counts[f"{label}_skipped_previously_unlinked"] += 1
            continue
        author = db.query(AuthorModel).filter(AuthorModel.author_id == author_id).one_or_none()
        if author is None or author.person_id is not None:
            # deleted or linked since the candidate query
            continue
        on_paper = db.query(AuthorModel.author_order).filter(
            AuthorModel.reference_id == author.reference_id,
            AuthorModel.person_id == person_id,
            AuthorModel.author_order.isnot(None),
        ).first()
        if on_paper is not None:
            counts[f"{label}_skipped_person_on_paper"] += 1
            logger.debug(f"{label}: person_id={person_id} is already author #{on_paper[0]} "
                         f"on reference_id={author.reference_id}")
            continue
        try:
            # absorbs a person-only stub for this person on the paper, as the API does
            link_person(db, author, person_id)
            db.flush()
        except Exception as e:
            db.rollback()
            counts[f"{label}_errors"] += 1
            logger.error(f"{label}: failed to link author_id={author_id} to person_id={person_id}: {e}")
            continue
        logger.info(f"{label}: linked author_id={author_id} ({author.name}) "
                    f"reference_id={author.reference_id} to person_id={person_id}")
        if dry_run:
            db.rollback()
        else:
            db.commit()
        counts[f"{label}_linked"] += 1


def link_authors_to_persons(db: Session, dry_run: bool = False) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)

    orcid_matches = find_orcid_matches(db, counts)
    apply_links(db, orcid_matches, ORCID_USER, counts, dry_run)

    # The ORCID pass has committed, but in a dry run its links were rolled back,
    # so exclude its matches explicitly to keep the passes exclusive either way.
    email_matches = find_email_matches(db, counts, exclude_author_ids=set(orcid_matches))
    apply_links(db, email_matches, EMAIL_USER, counts, dry_run)

    return dict(counts)


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Link authors to persons by ORCID, then by PubMed author email (SCRUM-5744)")
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would be linked and roll back")
    parser.add_argument("--verbose", action="store_true",
                        help="log every skipped candidate")
    args = parser.parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    db = create_postgres_session(False)
    try:
        counts = link_authors_to_persons(db, dry_run=args.dry_run)
    finally:
        db.close()
    logger.info(("DRY RUN " if args.dry_run else "") + "summary:")
    for key in sorted(counts):
        logger.info(f"  {key}: {counts[key]}")


if __name__ == "__main__":
    main()
