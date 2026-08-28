"""
doi_backfill_utils.py
=====================

Shared helpers for the monthly DOI backfill (SCRUM-4525), currently used by
add_missing_dois_from_europepmc.py: select references that have no
non-obsolete DOI cross-reference, normalize DOIs, and insert DOI
cross-references with the duplicate / conflict / curator-removed guards.

Kept source-agnostic on purpose: the Candidate metadata (title, volume,
pages, year, journal) and the require_pmid=False selection path serve
bibliographic matchers like the parked CrossRef script, so that script can
be restored by dropping its file back in.
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from sqlalchemy import bindparam, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from agr_literature_service.api.models import CrossReferenceModel

logger = logging.getLogger(__name__)

BATCH_COMMIT_SIZE = 250

# DOI directory identifier: "10.<registrant>/<suffix>"
DOI_PATTERN = re.compile(r"^10\.\d{4,9}/\S+$")


@dataclass
class Candidate:
    """A reference missing a DOI, with the metadata the lookups need."""
    reference_id: int
    curie: str                      # AGRKB curie
    pmid: Optional[str] = None      # digits only, no "PMID:" prefix
    title: Optional[str] = None
    volume: Optional[str] = None
    page_range: Optional[str] = None
    year: Optional[str] = None      # 4-digit string when known
    journal: Optional[str] = None            # resource title
    journal_abbreviation: Optional[str] = None


@dataclass
class BackfillStats:
    candidates: int = 0
    no_searchable_title: int = 0
    dois_found: int = 0
    added: int = 0
    invalid_doi: int = 0
    conflict_other_reference: int = 0
    lost_race: int = 0
    removed_by_curator: int = 0
    conflicts: List[Tuple[str, str, str]] = field(default_factory=list)
    # (ref curie, doi curie, owner curie) rows for the conflict report;
    # lost-race rows carry "concurrent writer" as the owner

    def summary(self) -> str:
        return (f"candidates={self.candidates} no_searchable_title={self.no_searchable_title} "
                f"dois_found={self.dois_found} added={self.added} "
                f"invalid_doi={self.invalid_doi} conflict_other_reference={self.conflict_other_reference} "
                f"lost_race={self.lost_race} removed_by_curator={self.removed_by_curator}")


def normalize_doi(raw: Optional[str]) -> Optional[str]:
    """Normalize a DOI string from an external API to the bare '10.x/y' form,
    or return None when it does not look like a DOI. Common wrappers
    ('doi:' / 'https://doi.org/' prefixes, surrounding whitespace) are
    stripped; the case of the suffix is preserved as delivered."""
    if not raw:
        return None
    doi = raw.strip()
    lowered = doi.lower()
    for prefix in ("https://doi.org/", "http://doi.org/", "https://dx.doi.org/",
                   "http://dx.doi.org/", "doi.org/", "doi:"):
        if lowered.startswith(prefix):
            doi = doi[len(prefix):].strip()
            lowered = doi.lower()
    if not DOI_PATTERN.match(doi):
        return None
    return doi


def get_references_missing_doi(db_session: Session, require_pmid: bool = False,
                               limit: Optional[int] = None) -> List[Candidate]:
    """References with no non-obsolete DOI cross-reference, plus the metadata
    the external lookups need. With require_pmid, only references that carry a
    non-obsolete PMID cross-reference are returned (the Europe PMC lookup is
    by PMID); otherwise references without a PMID are included too (the
    CrossRef lookup is bibliographic)."""
    pmid_join = "JOIN" if require_pmid else "LEFT JOIN"
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    # With a LIMIT, rotate the window randomly: a fixed reference_id order
    # would pin permanently-unmatchable references (no DOI registered
    # anywhere) at the head, and a monthly bounded run would re-query the
    # same head forever without ever reaching the rest of the backlog.
    # Random order makes every candidate reachable across successive runs.
    order_clause = "ORDER BY random()" if limit else "ORDER BY r.reference_id"
    rows = db_session.execute(text(f"""
        SELECT r.reference_id, r.curie, p.curie AS pmid_curie, r.title, r.volume,
               r.page_range, SUBSTRING(r.date_published, 1, 4) AS year,
               res.title AS journal, res.title_abbreviation AS journal_abbreviation
        FROM reference r
        {pmid_join} cross_reference p
          ON p.reference_id = r.reference_id
          AND p.curie_prefix = 'PMID' AND p.is_obsolete IS FALSE
        LEFT JOIN resource res ON res.resource_id = r.resource_id
        WHERE NOT EXISTS (
            SELECT 1 FROM cross_reference d
            WHERE d.reference_id = r.reference_id
              AND d.curie_prefix = 'DOI' AND d.is_obsolete IS FALSE
        )
        {order_clause}
        {limit_clause}
    """)).fetchall()
    candidates = []
    for x in rows:
        pmid = x[2].replace("PMID:", "") if x[2] else None
        year = x[6] if x[6] and str(x[6]).isdigit() and len(str(x[6])) == 4 else None
        candidates.append(Candidate(reference_id=x[0], curie=x[1], pmid=pmid, title=x[3],
                                    volume=x[4], page_range=x[5], year=year,
                                    journal=x[7], journal_abbreviation=x[8]))
    return candidates


def _doi_key(doi_curie: str) -> str:
    """DOI suffixes are case-insensitive (DOI handbook) and sources differ in
    the case they deliver, so every comparison uses the lowercased curie —
    an obsoleted DOI:10.x/ABC must block re-adding 10.x/abc, and a case
    variant must not slip past the other-reference conflict check."""
    return doi_curie.lower()


def _normalize_additions(additions: List[Tuple[Candidate, str]],
                         stats: BackfillStats) -> List[Tuple[Candidate, str]]:
    """Normalize raw DOIs, dropping (and counting) the unparseable ones."""
    normalized: List[Tuple[Candidate, str]] = []
    for cand, raw_doi in additions:
        doi = normalize_doi(raw_doi)
        if doi is None:
            stats.invalid_doi += 1
            logger.warning("%s: dropping invalid DOI %r", cand.curie, raw_doi)
            continue
        normalized.append((cand, doi))
    return normalized


def _lookup_existing_dois(db_session: Session,
                          normalized: List[Tuple[Candidate, str]]) -> Dict[str, List[Tuple[int, bool, str]]]:
    """One batch lookup of every DOI curie about to be touched (any
    obsolescence), keyed case-insensitively: {lower curie: [(reference_id,
    is_obsolete, owner AGRKB curie), ...]}. The owner curie comes from a join
    to reference because the conflict report must name it — the owner of a
    cross-run conflict already has a non-obsolete DOI, so it is never in the
    candidate batch, and curators work in AGRKB curies, not reference_ids.
    (The inner join drops resource-owned DOI rows, whose reference_id is
    NULL — matching the previous behaviour where they never counted as an
    owner; if one collides at insert time the lost_race path reports it.)"""
    doi_keys = list({_doi_key(f"DOI:{doi}") for _, doi in normalized})
    existing: Dict[str, List[Tuple[int, bool, str]]] = {}
    lookup_sql = text(
        "SELECT cr.curie, cr.reference_id, cr.is_obsolete, r.curie "
        "FROM cross_reference cr "
        "JOIN reference r ON r.reference_id = cr.reference_id "
        "WHERE cr.curie_prefix = 'DOI' AND lower(cr.curie) IN :curies"
    ).bindparams(bindparam("curies", expanding=True))
    for i in range(0, len(doi_keys), 1000):
        chunk = doi_keys[i:i + 1000]
        rows = db_session.execute(lookup_sql, {"curies": chunk}).fetchall()
        for curie, reference_id, is_obsolete, owner_curie in rows:
            existing.setdefault(_doi_key(curie), []).append((reference_id, is_obsolete, owner_curie))
    return existing


def add_doi_cross_references(db_session: Session, additions: List[Tuple[Candidate, str]],
                             stats: BackfillStats, dry_run: bool = False) -> None:
    """Insert 'DOI:<doi>' cross-references for (candidate, doi) pairs, guarding:

    - invalid/unparseable DOIs are dropped (normalize_doi);
    - a DOI already attached (non-obsolete) to ANOTHER reference is a conflict:
      skipped and reported for curator review, never re-pointed;
    - a DOI present on the SAME reference but marked obsolete was removed by a
      curator on purpose: skipped;
    - all DOI comparisons are case-insensitive (_doi_key);
    - commits in batches, and a unique-index race with a concurrent DOI writer
      (daily DQM sync, weekly PubMed sync) downgrades to a per-row retry
      instead of aborting the run;
    - a dry run logs what would be added and writes nothing, while still
      tracking pending DOIs so intra-run duplicates report the same
      add/conflict split a real run would.
    """
    normalized = _normalize_additions(additions, stats)
    if not normalized:
        return
    existing = _lookup_existing_dois(db_session, normalized)
    pending: List[Tuple[Candidate, str]] = []  # rows added since the last commit

    def flush_pending() -> None:
        """Commit the pending batch. If the unique index fires because a
        concurrent writer inserted one of these DOIs after our snapshot,
        retry row by row so the one losing row is skipped and reported
        instead of the whole batch (and run) being lost."""
        if not pending:
            return
        try:
            db_session.commit()
        except IntegrityError:
            db_session.rollback()
            for row_cand, row_doi_curie in pending:
                try:
                    db_session.add(CrossReferenceModel(curie=row_doi_curie, curie_prefix="DOI",
                                                       reference_id=row_cand.reference_id,
                                                       is_obsolete=False))
                    db_session.commit()
                except IntegrityError:
                    db_session.rollback()
                    stats.added -= 1
                    stats.lost_race += 1
                    stats.conflicts.append((row_cand.curie, row_doi_curie, "concurrent writer"))
                    # Either unique index may have fired: this DOI landing on
                    # another reference, or another DOI landing on this
                    # reference (one non-obsolete DOI per reference).
                    logger.warning("%s: inserting %s lost a race with a concurrent writer "
                                   "(this DOI or this reference was claimed mid-run); skipped",
                                   row_cand.curie, row_doi_curie)
        pending.clear()

    for cand, doi in normalized:
        doi_curie = f"DOI:{doi}"
        key = _doi_key(doi_curie)
        rows = existing.get(key, [])
        active = next(((rid, owner) for rid, obsolete, owner in rows if not obsolete), None)
        if active is not None and active[0] != cand.reference_id:
            stats.conflict_other_reference += 1
            stats.conflicts.append((cand.curie, doi_curie, active[1]))
            logger.warning("%s: %s already on reference %s; skipping (curator review needed)",
                           cand.curie, doi_curie, active[1])
            continue
        if active is not None:
            # already on this reference (e.g. added between selection and insertion)
            continue
        if any(rid == cand.reference_id and obsolete for rid, obsolete, _ in rows):
            stats.removed_by_curator += 1
            logger.info("%s: %s was made obsolete on this reference by a curator; skipping",
                        cand.curie, doi_curie)
            continue
        stats.added += 1
        existing.setdefault(key, []).append((cand.reference_id, False, cand.curie))
        if dry_run:
            logger.info("DRY RUN: would add %s to %s", doi_curie, cand.curie)
            continue
        db_session.add(CrossReferenceModel(curie=doi_curie, curie_prefix="DOI",
                                           reference_id=cand.reference_id, is_obsolete=False))
        pending.append((cand, doi_curie))
        logger.info("Added %s to %s", doi_curie, cand.curie)
        if len(pending) >= BATCH_COMMIT_SIZE:
            flush_pending()
    if not dry_run:
        flush_pending()
