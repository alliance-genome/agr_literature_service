"""
Incrementally sync SGD colleague data into ABC person / laboratory tables.

Re-runnable whenever SGD data changes: keyed on ``colleague_id`` (via the
``SGD:Colleague_<id>`` cross-reference) it ADDS colleagues new to ABC, UPDATES
those whose SGD fields changed (SGD is the source of truth), and -- only with
``--prune`` on a full run -- DELETES rows whose colleague_id has disappeared from
the SGD dump. The default run is a read-only dry-run that previews the exact
add/update/delete counts (the same code path, with writes suppressed + rolled
back), so a commit holds no surprises.

Source : JSON dump produced by SGDBackend-Nex2
         ``scripts/dumping/colleague/dumpColleague.py`` (the --datafile arg).
         This loader no longer connects to SGD directly.
Target : ABC literature DB (via ``create_postgres_session`` / ``PSQL_*`` env)

Decisions encoded (discussed 2026-06-22):
  * Lab strategy: one ``laboratory`` per distinct "Head of Lab" PI found in
    ``nex.colleague_relation`` (1,235 labs), not per ``is_pi`` flag.
  * ``laboratory_person.lab_position`` <- ``colleague.job_title``; the head of
    each lab also gets ``laboratory_person.is_pi`` (timestamp) set.
  * ``person.privacy`` honours SGD ``display_email``:
        display_email = true  -> 'show_all'
        display_email = false -> 'hide_email'   (also the column default)
    'show_all' is the only allowed value that exposes the email
    (allowed: show_all / logged_in_only / fully_hidden / hide_email).
  * ``colleague_keyword`` and ``colleague.profession`` are folded into
    ``person.biography_research_interest`` (with research_interest).
  * ``colleague.suffix`` (Jr./Sr./III...) is appended to ``person.display_name``.
  * Emails are ADDITIVE for existing persons: an update never overwrites or
    removes a person's existing ``person_email`` rows; the colleague's current
    SGD email is appended only when not already present (case-insensitive). A
    person may legitimately hold several emails.
  * ``colleague_relation`` 'Associate' -> ``person_lineage`` collaborator_of.
    SGD stores these mirrored (A<->B); collaborator_of is symmetric, so each
    pair is normalized to ascending person-id order and deduped.
  * Phone numbers are skipped.
  * Match keys: ``SGD:Colleague_<colleague_id>`` (person), ``SGD:Lab_<pi_id>``
    (laboratory), (subject, object, relationship) for person_lineage, and
    (laboratory_id, person_id) for laboratory_person.
  * Email de-duplication: a colleague new to ABC whose email already belongs to
    a pre-existing *non-SGD* person (e.g. AGR staff/author) is attached to that
    person (its SGD:Colleague xref is added + fields synced) instead of creating
    a duplicate. Matching is restricted to non-SGD persons because institutional
    emails are shared by distinct colleagues. ``--merge-email-dups`` repairs
    such duplicates that already exist (one non-SGD + one SGD person per email).

Delete safety (``--prune`` only, and never under ``--limit``):
  * A person linked to a ``users`` account is never hard-deleted (reported).
  * A laboratory with ``laboratory_allele_designation`` rows is never deleted
    (reported). Deleting a person/lab cascades to its SGD-managed children.
  * collaborator_of deletes only touch edges between two SGD persons.

Reported but not loaded (no clean ABC target):
  * ``colleague_locus`` (colleague <-> gene links)
  * phones / ``is_beta_tester``

Usage::

    # 1. produce the dump (in the SGDBackend-Nex2 repo)
    #    set -a; source .env_cc; set +a
    #    python scripts/dumping/colleague/dumpColleague.py --outfile /tmp/colleague.json

    # 2. load it (in this repo)
    set -a
    source agr_literature_service/.env_cc          # ABC: PSQL_*, ID_MATI_URL, ENV_STATE
    set +a

    # read-only dry-run (default) -> prints projected counts, writes TSV previews
    python -m agr_literature_service.lit_processing.oneoff_scripts.load_sgd_colleagues \
        --datafile /tmp/colleague.json

    # small test against the dev DB (mints real MATI curies for new rows)
    python -m agr_literature_service.lit_processing.oneoff_scripts.load_sgd_colleagues \
        --datafile /tmp/colleague.json --commit --limit 50

    # full sync, add + update only (no deletes)
    python -m agr_literature_service.lit_processing.oneoff_scripts.load_sgd_colleagues \
        --datafile /tmp/colleague.json --commit

    # full sync incl. deleting colleagues/labs/lineage gone from SGD
    python -m agr_literature_service.lit_processing.oneoff_scripts.load_sgd_colleagues \
        --datafile /tmp/colleague.json --commit --prune

NOTE: in ``--commit`` mode each newly created person and laboratory consumes one
real MATI id (ENV_STATE != 'test'); MATI's counter does not roll back. Updates
and deletes consume none. Re-runs are safe and convergent: a second run with no
SGD changes is a no-op.
"""

import argparse
import csv
import json
import logging
from collections import defaultdict
from datetime import datetime
from os import path
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy import text

from agr_literature_service.api.models import (
    PersonModel,
    PersonNameModel,
    PersonEmailModel,
    PersonCrossReferenceModel,
    PersonNoteModel,
    LaboratoryModel,
    LaboratoryPersonModel,
    LaboratoryCrossReferenceModel,
    PersonLineageModel,
)
from agr_literature_service.api.schemas import PersonPersonRole
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.global_utils import (
    get_next_person_curie,
    get_next_laboratory_curie,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session,
)

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# privacy values honour SGD display_email (column allows
# show_all / logged_in_only / fully_hidden / hide_email)
PRIVACY_PUBLIC = "show_all"
PRIVACY_PRIVATE = "hide_email"

SGD_PERSON_PREFIX = "SGD"
SGD_PERSON_CURIE_FMT = "SGD:Colleague_{}"
SGD_LAB_CURIE_FMT = "SGD:Lab_{}"
ORCID_PREFIX = "ORCID"

# SGD colleague_relation 'Associate' -> person_lineage. collaborator_of is a
# symmetric role, so the pair is stored in ascending person-id order and the
# uq_person_lineage_person_ids_relationship constraint dedups it.
COLLABORATOR_OF = PersonPersonRole.collaborator_of.value

# does the ORM map person.privacy on this branch? (SCRUM-6157 adds it). The dev
# DB already has the column with a NOT NULL default of 'hide_email', so when the
# model lacks it we set the non-default ('show_all') values with a raw UPDATE.
PERSON_HAS_PRIVACY = hasattr(PersonModel, "privacy")

COMMIT_BATCH = 200


# --------------------------------------------------------------------------- #
# Dump reader (input is the JSON produced by dumpColleague.py)
# --------------------------------------------------------------------------- #
def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = " ".join(str(value).split())
    return value or None


def load_dump(datafile: str):
    """
    Parse the dumpColleague.py JSON and return the same structures the loader
    previously built straight from SGD:

        (colleagues, research_urls, lab_urls, keywords, lab_members,
         associate_pairs, skipped)

    where colleagues is {colleague_id: {field: value, ...}}, the *_urls and
    keywords are {colleague_id: [...]}, lab_members is
    {pi_colleague_id: {member_colleague_id, ...}}, associate_pairs is a list of
    (colleague_id_1, colleague_id_2) deduped unordered collaborator pairs, and
    skipped is the not-loaded counts carried in the dump's metadata.
    """
    if not path.exists(datafile):
        raise SystemExit(
            f"datafile not found: {datafile}\n"
            "Produce it first with SGDBackend-Nex2 "
            "scripts/dumping/colleague/dumpColleague.py"
        )
    with open(datafile) as fh:
        payload = json.load(fh)

    colleagues: Dict[int, dict] = {}
    research_urls: Dict[int, List[str]] = {}
    lab_urls: Dict[int, List[str]] = {}
    keywords: Dict[int, List[str]] = {}
    for col in payload["colleagues"]:
        cid = int(col["colleague_id"])
        research_urls[cid] = col.pop("research_summary_urls", []) or []
        lab_urls[cid] = col.pop("lab_urls", []) or []
        keywords[cid] = col.pop("keywords", []) or []
        col["colleague_id"] = cid
        colleagues[cid] = col

    lab_members: Dict[int, Set[int]] = {}
    for rel in payload.get("lab_relations", []):
        pi = int(rel["pi_colleague_id"])
        member = int(rel["member_colleague_id"])
        lab_members.setdefault(pi, set()).add(member)

    associate_pairs: List[Tuple[int, int]] = []
    for rel in payload.get("associate_relations", []):
        c1 = int(rel["colleague_id_1"])
        c2 = int(rel["colleague_id_2"])
        if c1 != c2:
            associate_pairs.append((c1, c2))

    meta = payload.get("metadata", {})
    skipped = {
        "colleague_locus": meta.get("skipped_colleague_locus", 0),
    }
    return (colleagues, research_urls, lab_urls, keywords, lab_members,
            associate_pairs, skipped)


# --------------------------------------------------------------------------- #
# Field mapping helpers
# --------------------------------------------------------------------------- #
def build_biography(col: dict, keywords: List[str]) -> Optional[str]:
    parts: List[str] = []
    profession = _clean(col.get("profession"))
    if profession:
        parts.append("Profession: " + profession)
    ri = _clean(col.get("research_interest"))
    if ri:
        parts.append(ri)
    if keywords:
        parts.append("Keywords: " + "; ".join(keywords))
    return "\n".join(parts) or None


def build_street_address(col: dict) -> Optional[str]:
    pieces = [_clean(col.get(f"address{i}")) for i in (1, 2, 3)]
    pieces = [p for p in pieces if p]
    return ", ".join(pieces) or None


def privacy_for(col: dict) -> str:
    return PRIVACY_PUBLIC if col.get("display_email") else PRIVACY_PRIVATE


def display_name_for(col: dict) -> str:
    dn = _clean(col.get("display_name"))
    if not dn:
        first = _clean(col.get("first_name")) or ""
        last = _clean(col.get("last_name")) or ""
        dn = _clean(f"{first} {last}") or f"Colleague {col['colleague_id']}"
    suffix = _clean(col.get("suffix"))
    if suffix:
        dn = f"{dn} {suffix}"
    return dn


def orcid_curie(col: dict) -> Optional[str]:
    orcid = _clean(col.get("orcid"))
    if not orcid:
        return None
    if "/" in orcid:                       # tolerate a full ORCID URL
        orcid = orcid.rstrip("/").rsplit("/", 1)[-1]
    if ":" in orcid:                       # already prefixed
        return orcid if orcid.count(":") == 1 else None
    return f"{ORCID_PREFIX}:{orcid}"


# --------------------------------------------------------------------------- #
# Existing-state lookup (idempotency)
# --------------------------------------------------------------------------- #
def all_sgd_person_ids_by_cid(abc) -> Dict[int, int]:
    """Map every SGD colleague_id present in ABC to its person_id, via the
    ``SGD:Colleague_<id>`` cross-reference (the incremental-sync match key)."""
    rows = abc.execute(text(
        "SELECT curie, person_id FROM person_cross_reference "
        "WHERE curie_prefix = :p AND curie LIKE 'SGD:Colleague_%'"
    ), {"p": SGD_PERSON_PREFIX}).fetchall()
    out: Dict[int, int] = {}
    for curie, pid in rows:
        try:
            out[int(curie.rsplit("_", 1)[-1])] = pid
        except ValueError:
            continue
    return out


def existing_person_curies(abc) -> Set[str]:
    """All person_cross_reference curies (curie is globally unique)."""
    return set(
        r[0] for r in abc.execute(
            text("SELECT curie FROM person_cross_reference")
        ).fetchall()
    )


def existing_lab_xrefs(abc) -> Dict[int, int]:
    rows = abc.execute(text(
        "SELECT x.curie, x.laboratory_id FROM laboratory_cross_reference x "
        "WHERE x.curie_prefix = :p AND x.curie LIKE 'SGD:Lab_%'"
    ), {"p": SGD_PERSON_PREFIX}).fetchall()
    out: Dict[int, int] = {}
    for curie, lab_id in rows:
        try:
            out[int(curie.rsplit("_", 1)[-1])] = lab_id
        except ValueError:
            continue
    return out


def nonsgd_person_email_map(abc) -> Dict[str, int]:
    """Map lower(email) -> person_id for persons that are NOT SGD colleagues
    (no ``SGD:Colleague_*`` xref). Used to merge an incoming colleague onto a
    pre-existing ABC person (e.g. AGR staff/author) instead of duplicating it.
    Email is not a unique identity key (institutional addresses are shared), so
    matching is deliberately restricted to non-SGD persons; the lowest person_id
    wins when several non-SGD persons share an address."""
    rows = abc.execute(text(
        "SELECT lower(pe.email_address) AS email, pe.person_id "
        "FROM person_email pe "
        "WHERE pe.person_id NOT IN ("
        "  SELECT person_id FROM person_cross_reference "
        "  WHERE curie LIKE 'SGD:Colleague_%' AND person_id IS NOT NULL) "
        "ORDER BY pe.person_id"
    )).fetchall()
    out: Dict[str, int] = {}
    for email, pid in rows:
        if email and email not in out:
            out[email] = pid
    return out


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #
def _report(counts, skipped, apply, prune, limit, scope) -> None:
    """Print the add/update/delete tally produced by run_sync (identical shape
    for a dry-run preview and a real commit)."""
    mode = "COMMIT (writes applied)" if apply else "DRY-RUN (no writes)"
    note = "" if limit is None else f"  (--limit {limit})"
    logger.info("=" * 70)
    logger.info("%s   scope=%d colleagues%s", mode, scope, note)
    logger.info("=" * 70)
    if counts["email_dups_merged"] or counts["email_dups_skipped"]:
        logger.info("email-dup repair  =%d merged (%d skipped: ambiguous)",
                    counts["email_dups_merged"], counts["email_dups_skipped"])
    logger.info("person             +add %-6d ~update %-6d -delete %-6d "
                "(=%d merged onto existing, %d kept: user-linked)",
                counts["persons_added"], counts["persons_updated"],
                counts["persons_deleted"], counts["persons_merged"],
                counts["persons_delete_skipped"])
    if counts["orcid_skipped"]:
        logger.info("  ORCID xrefs skipped (curie already exists): %d",
                    counts["orcid_skipped"])
    logger.info("laboratory         +add %-6d ~update %-6d -delete %-6d "
                "(%d kept: allele designations)",
                counts["labs_added"], counts["labs_updated"],
                counts["labs_deleted"], counts["labs_delete_skipped"])
    logger.info("laboratory_person  +add %-6d ~update %-6d -delete %-6d",
                counts["members_added"], counts["members_updated"],
                counts["members_deleted"])
    logger.info("person_lineage     +add %-6d %14s-delete %-6d "
                "(%d unresolved pairs)", counts["lineage_added"], "",
                counts["lineage_deleted"],
                counts["lineage_skipped_unresolved"])
    logger.info("-" * 70)
    if not prune:
        logger.info("DELETES DISABLED (pass --prune to remove obsolete rows)")
    elif limit is not None:
        logger.info("DELETES DISABLED (--limit set; a full run is required to "
                    "reconcile deletes safely)")
    logger.info("SKIPPED (no ABC target): colleague_locus=%d; "
                "phones / is_beta_tester dropped",
                skipped["colleague_locus"])
    logger.info("=" * 70)
    if not PERSON_HAS_PRIVACY:
        logger.warning("PersonModel does not map 'privacy' on this branch; "
                       "privacy is set via raw UPDATE / DB default.")


def _write_previews(colleagues, research_urls, keywords, lab_members,
                    in_scope, outdir) -> None:
    cids = sorted(in_scope)
    labs = [pi for pi in lab_members if pi in in_scope and pi in colleagues]
    ppath = path.join(outdir, "preview_persons.tsv")
    with open(ppath, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["sgd_colleague_id", "display_name", "last_name", "email",
                    "privacy", "orcid_curie", "n_webpages", "biography_snippet"])
        for cid in cids[:500]:
            col = colleagues[cid]
            bio = build_biography(col, keywords.get(cid, [])) or ""
            w.writerow([
                cid, display_name_for(col), _clean(col.get("last_name")) or "",
                _clean(col.get("email")) or "", privacy_for(col),
                orcid_curie(col) or "", len(research_urls.get(cid, [])),
                bio.replace("\n", " ")[:80],
            ])

    lpath = path.join(outdir, "preview_labs.tsv")
    with open(lpath, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["pi_sgd_colleague_id", "lab_name", "institution",
                    "n_members"])
        for pi in labs[:500]:
            col = colleagues[pi]
            w.writerow([
                pi, f"{display_name_for(col)} Lab",
                _clean(col.get("institution")) or "",
                len({m for m in lab_members[pi] if m in in_scope}),
            ])

    logger.info("previews written:\n  %s\n  %s", ppath, lpath)


# --------------------------------------------------------------------------- #
# Commit
# --------------------------------------------------------------------------- #
def _add_person_xrefs(abc, person, cid, col, existing_curies) -> bool:
    """
    Add the SGD and (when present) ORCID cross-references, skipping any curie
    already in ``existing_curies`` (globally unique). Returns True if an ORCID
    was skipped because of a collision.
    """
    sgd_curie = SGD_PERSON_CURIE_FMT.format(cid)
    if sgd_curie not in existing_curies:
        abc.add(PersonCrossReferenceModel(
            person_id=person.person_id,
            curie=sgd_curie,
            curie_prefix=SGD_PERSON_PREFIX,
            pages=[col["obj_url"]] if _clean(col.get("obj_url")) else None,
        ))
        existing_curies.add(sgd_curie)

    oc = orcid_curie(col)
    if oc and oc not in existing_curies:
        abc.add(PersonCrossReferenceModel(
            person_id=person.person_id,
            curie=oc,
            curie_prefix=ORCID_PREFIX,
        ))
        existing_curies.add(oc)
    elif oc:
        logger.warning(
            "colleague %s (person_id %s): ORCID %s already exists, "
            "skipping ORCID cross-reference", cid, person.person_id, oc)
        return True
    return False


def _create_person(abc, cid, col, research_urls, keywords, existing_curies):
    """Create a person row and its child rows. Returns (person, orcid_skipped)."""
    kwargs = dict(
        display_name=display_name_for(col),
        curie=get_next_person_curie(abc),
        institution=[_clean(col.get("institution"))]
        if _clean(col.get("institution")) else None,
        webpage=research_urls.get(cid) or None,
        city=_clean(col.get("city")),
        state=_clean(col.get("state")),
        postal_code=_clean(col.get("postal_code")),
        country=_clean(col.get("country")),
        street_address=build_street_address(col),
        biography_research_interest=build_biography(col, keywords.get(cid, [])),
    )
    if PERSON_HAS_PRIVACY:
        kwargs["privacy"] = privacy_for(col)
    person = PersonModel(**kwargs)
    abc.add(person)
    abc.flush()

    abc.add(PersonNameModel(
        person_id=person.person_id,
        first_name=_clean(col.get("first_name")),
        middle_name=_clean(col.get("middle_name")),
        last_name=_clean(col.get("last_name")) or display_name_for(col),
        is_primary=True,
    ))
    if _clean(col.get("other_last_name")):
        abc.add(PersonNameModel(
            person_id=person.person_id,
            first_name=_clean(col.get("first_name")),
            last_name=_clean(col.get("other_last_name")),
            is_primary=False,
        ))
    if _clean(col.get("email")):
        abc.add(PersonEmailModel(
            person_id=person.person_id,
            email_address=_clean(col.get("email")),
        ))
    if _clean(col.get("colleague_note")):
        abc.add(PersonNoteModel(
            person_id=person.person_id,
            note=_clean(col.get("colleague_note")),
        ))

    orcid_skipped = _add_person_xrefs(abc, person, cid, col, existing_curies)
    return person, orcid_skipped


def _sync_person_scalars(person, cid, col, research_urls, keywords,
                         apply) -> bool:
    """Overwrite person scalar/array columns from SGD; return True if any
    differed. When ``apply`` is False, only detect (no writes)."""
    desired = {
        "display_name": display_name_for(col),
        "institution": [_clean(col.get("institution"))]
        if _clean(col.get("institution")) else None,
        "webpage": research_urls.get(cid) or None,
        "city": _clean(col.get("city")),
        "state": _clean(col.get("state")),
        "postal_code": _clean(col.get("postal_code")),
        "country": _clean(col.get("country")),
        "street_address": build_street_address(col),
        "biography_research_interest": build_biography(col,
                                                       keywords.get(cid, [])),
    }
    if PERSON_HAS_PRIVACY:
        desired["privacy"] = privacy_for(col)
    changed = False
    for attr, val in desired.items():
        if getattr(person, attr) != val:
            changed = True
            if apply:
                setattr(person, attr, val)
    return changed


def _sync_person_names(abc, person, col, apply) -> bool:
    """Reconcile person_name rows to SGD: one primary (first/middle/last) and an
    optional secondary from other_last_name; drop stale secondaries."""
    pid = person.person_id
    first = _clean(col.get("first_name"))
    middle = _clean(col.get("middle_name"))
    last = _clean(col.get("last_name")) or display_name_for(col)
    other = _clean(col.get("other_last_name"))
    names = abc.query(PersonNameModel).filter_by(person_id=pid).all()
    primary = next((n for n in names if n.is_primary), None)
    secondaries = [n for n in names if not n.is_primary]
    changed = False

    if primary is None:
        changed = True
        if apply:
            abc.add(PersonNameModel(person_id=pid, first_name=first,
                                    middle_name=middle, last_name=last,
                                    is_primary=True))
    elif (primary.first_name, primary.middle_name,
          primary.last_name) != (first, middle, last):
        changed = True
        if apply:
            primary.first_name, primary.middle_name, primary.last_name = (
                first, middle, last)

    if other:
        match = next((s for s in secondaries if s.last_name == other), None)
        if match is None:
            changed = True
            if apply:
                abc.add(PersonNameModel(person_id=pid, first_name=first,
                                        last_name=other, is_primary=False))
        elif match.first_name != first:
            changed = True
            if apply:
                match.first_name = first
        stale = [s for s in secondaries if s.last_name != other]
    else:
        stale = secondaries
    for s in stale:
        changed = True
        if apply:
            abc.delete(s)
    return changed


def _sync_single_child(abc, model, pid, attr, value, apply) -> bool:
    """Reconcile a single-valued SGD child collection (note) to ``value``
    (None => remove all). Updates the first row, removes extras."""
    rows = abc.query(model).filter_by(person_id=pid).all()
    changed = False
    if value:
        if not rows:
            changed = True
            if apply:
                abc.add(model(person_id=pid, **{attr: value}))
        else:
            if getattr(rows[0], attr) != value:
                changed = True
                if apply:
                    setattr(rows[0], attr, value)
            for extra in rows[1:]:
                changed = True
                if apply:
                    abc.delete(extra)
    else:
        for r in rows:
            changed = True
            if apply:
                abc.delete(r)
    return changed


def _sync_person_email_additive(abc, pid, email, apply) -> bool:
    """Add the colleague's SGD email to person_email when it is not already
    present, and NEVER overwrite or remove an existing email. A person may hold
    several emails (e.g. an AGR/author address plus the SGD one); SGD updates
    only contribute the colleague's current address. Matching is
    case-insensitive. Returns True if a new email row was added."""
    if not email:
        return False
    rows = abc.query(PersonEmailModel).filter_by(person_id=pid).all()
    existing = {(r.email_address or "").lower() for r in rows}
    if email.lower() in existing:
        return False
    if apply:
        abc.add(PersonEmailModel(person_id=pid, email_address=email))
    return True


def _sync_orcid_xref(abc, person, cid, col, existing_curies, apply) -> bool:
    """Reconcile the person's ORCID cross-reference to SGD (add/remove), honoring
    the global-unique curie rule, and refresh the SGD xref's pages (obj_url)."""
    pid = person.person_id
    desired = orcid_curie(col)
    xrefs = abc.query(PersonCrossReferenceModel).filter_by(
        person_id=pid, curie_prefix=ORCID_PREFIX).all()
    changed = False
    stale = [x for x in xrefs if x.curie != desired] if desired else xrefs
    # Delete stale ORCID rows (and flush) before adding the new one: a person
    # may hold at most one curie per prefix (uq_person_xref_person_prefix), and
    # the unit of work flushes INSERTs before DELETEs, so adding first would
    # momentarily leave two ORCID rows for this person and fail on re-runs where
    # the colleague's ORCID changed.
    for x in stale:
        changed = True
        existing_curies.discard(x.curie)
        if apply:
            abc.delete(x)
    if apply and stale:
        abc.flush()
    if desired and not any(x.curie == desired for x in xrefs):
        if desired in existing_curies:
            logger.warning("colleague %s: ORCID %s already exists "
                           "elsewhere; not adding", cid, desired)
        else:
            changed = True
            existing_curies.add(desired)
            if apply:
                abc.add(PersonCrossReferenceModel(
                    person_id=pid, curie=desired,
                    curie_prefix=ORCID_PREFIX))

    sgd_curie = SGD_PERSON_CURIE_FMT.format(cid)
    sx = abc.query(PersonCrossReferenceModel).filter_by(
        person_id=pid, curie=sgd_curie).first()
    pages = [col["obj_url"]] if _clean(col.get("obj_url")) else None
    if sx is not None and sx.pages != pages:
        changed = True
        if apply:
            sx.pages = pages
    return changed


def _update_person(abc, person, cid, col, research_urls, keywords,
                   existing_curies, apply) -> bool:
    """Sync an already-loaded person and its child rows to current SGD data.
    Returns True if anything differed."""
    changed = _sync_person_scalars(person, cid, col, research_urls, keywords,
                                   apply)
    changed = _sync_person_names(abc, person, col, apply) or changed
    changed = _sync_person_email_additive(abc, person.person_id,
                                          _clean(col.get("email")),
                                          apply) or changed
    changed = _sync_single_child(abc, PersonNoteModel, person.person_id,
                                 "note", _clean(col.get("colleague_note")),
                                 apply) or changed
    changed = _sync_orcid_xref(abc, person, cid, col, existing_curies,
                               apply) or changed
    return changed


def _delete_person_cascade(abc, pid) -> None:
    """Hard-delete a person and all SGD-managed dependents. user / lineage
    submission links are NULLed (callers skip user-linked persons)."""
    for sql in (
        "DELETE FROM person_lineage "
        "WHERE person_subject_id = :p OR person_object_id = :p",
        "UPDATE person_lineage_submission SET person_subject_id = NULL "
        "WHERE person_subject_id = :p",
        "UPDATE person_lineage_submission SET person_object_id = NULL "
        "WHERE person_object_id = :p",
        "DELETE FROM laboratory_person WHERE person_id = :p",
        "DELETE FROM person_cross_reference WHERE person_id = :p",
        "DELETE FROM person_email WHERE person_id = :p",
        "DELETE FROM person_note WHERE person_id = :p",
        "DELETE FROM person_name WHERE person_id = :p",
        "DELETE FROM person_setting WHERE person_id = :p",
        "UPDATE users SET person_id = NULL WHERE person_id = :p",
        "DELETE FROM person WHERE person_id = :p",
    ):
        abc.execute(text(sql), {"p": pid})


def _update_lab(abc, lab_id, col, lab_url, apply) -> bool:
    """Overwrite laboratory columns from the PI's SGD data (status untouched)."""
    lab = abc.get(LaboratoryModel, lab_id)
    if lab is None:
        return False
    desired = {
        "name": f"{display_name_for(col)} Lab",
        "institution": [_clean(col.get("institution"))]
        if _clean(col.get("institution")) else None,
        "webpage": lab_url or None,
        "city": _clean(col.get("city")),
        "state": _clean(col.get("state")),
        "postal_code": _clean(col.get("postal_code")),
        "country": _clean(col.get("country")),
        "street_address": build_street_address(col),
    }
    changed = False
    for attr, val in desired.items():
        if getattr(lab, attr) != val:
            changed = True
            if apply:
                setattr(lab, attr, val)
    return changed


def _delete_lab_cascade(abc, lab_id) -> None:
    for sql in (
        "DELETE FROM laboratory_person WHERE laboratory_id = :l",
        "DELETE FROM laboratory_cross_reference WHERE laboratory_id = :l",
        "DELETE FROM laboratory WHERE laboratory_id = :l",
    ):
        abc.execute(text(sql), {"l": lab_id})


def _sync_lab_members(abc, lab_id, pi, member_cids, colleagues,
                      person_by_cid, in_scope, now, apply, do_delete,
                      counts) -> None:
    """Reconcile laboratory_person rows for one lab to the SGD membership set
    (PI + members). Adds missing rows, fixes is_pi/lab_position, and (when
    do_delete) removes members no longer in SGD."""
    # desired: person_id -> (colleague_id, is_pi)
    desired: Dict[int, Tuple[int, bool]] = {}
    pi_pid = person_by_cid.get(pi)
    if pi_pid:
        desired[pi_pid] = (pi, True)
    for m in sorted(member_cids):
        if m not in in_scope:
            continue
        mp = person_by_cid.get(m)
        if mp and mp not in desired:
            desired[mp] = (m, False)

    current: Dict[int, Tuple] = {}
    if lab_id is not None:
        for ppid, is_pi, position in abc.execute(text(
            "SELECT person_id, is_pi, lab_position "
            "FROM laboratory_person WHERE laboratory_id = :l"
        ), {"l": lab_id}).fetchall():
            current[ppid] = (is_pi, position)

    for ppid, (ccid, is_pi) in desired.items():
        position = (_clean(colleagues[ccid].get("job_title"))
                    or ("Principal Investigator" if is_pi else None))
        if ppid not in current:
            counts["members_added"] += 1
            if apply and lab_id is not None:
                abc.add(LaboratoryPersonModel(
                    laboratory_id=lab_id, person_id=ppid,
                    is_pi=now if is_pi else None, lab_position=position))
        else:
            cur_is_pi, cur_pos = current[ppid]
            needs = (bool(cur_is_pi) != is_pi) or (cur_pos != position)
            if needs:
                counts["members_updated"] += 1
                if apply:
                    abc.execute(text(
                        "UPDATE laboratory_person SET is_pi = :ispi, "
                        "lab_position = :pos "
                        "WHERE laboratory_id = :l AND person_id = :p"
                    ), {"ispi": now if is_pi else None, "pos": position,
                        "l": lab_id, "p": ppid})

    if do_delete and lab_id is not None:
        for ppid in current:
            if ppid not in desired:
                counts["members_deleted"] += 1
                if apply:
                    abc.execute(text(
                        "DELETE FROM laboratory_person "
                        "WHERE laboratory_id = :l AND person_id = :p"
                    ), {"l": lab_id, "p": ppid})


def _sync_collaborators(abc, associate_pairs, person_by_cid, in_scope,
                        apply, do_delete, counts) -> None:
    """Reconcile person_lineage collaborator_of edges to the SGD Associate set.
    Symmetric, so pairs are compared in ascending person-id order. Deletes only
    touch edges between two SGD persons (never curator/other-sourced edges)."""
    sgd_pids = set(person_by_cid.values())
    desired = set()
    for c1, c2 in associate_pairs:
        if c1 not in in_scope or c2 not in in_scope:
            counts["lineage_skipped_unresolved"] += 1
            continue
        p1 = person_by_cid.get(c1)
        p2 = person_by_cid.get(c2)
        if not p1 or not p2 or p1 == p2:
            counts["lineage_skipped_unresolved"] += 1
            continue
        desired.add((min(p1, p2), max(p1, p2)))

    current = set()
    for s, o in abc.execute(text(
        "SELECT person_subject_id, person_object_id "
        "FROM person_lineage WHERE relationship = :r"
    ), {"r": COLLABORATOR_OF}).fetchall():
        current.add((min(s, o), max(s, o)))

    for s, o in desired:
        if (s, o) not in current:
            counts["lineage_added"] += 1
            if apply:
                abc.add(PersonLineageModel(person_subject_id=s,
                                           person_object_id=o,
                                           relationship=COLLABORATOR_OF))

    if do_delete:
        for s, o in current:
            if (s, o) not in desired and s in sgd_pids and o in sgd_pids:
                counts["lineage_deleted"] += 1
                if apply:
                    abc.execute(text(
                        "DELETE FROM person_lineage WHERE relationship = :r "
                        "AND ((person_subject_id = :s AND person_object_id = :o)"
                        " OR (person_subject_id = :o AND person_object_id = :s))"
                    ), {"r": COLLABORATOR_OF, "s": s, "o": o})


def _add_one_person(abc, cid, col, research_urls, keywords, existing_curies,
                    person_by_cid, pending_privacy, counts) -> None:
    person, orcid_skipped = _create_person(
        abc, cid, col, research_urls, keywords, existing_curies)
    person_by_cid[cid] = person.person_id
    if not PERSON_HAS_PRIVACY and privacy_for(col) != PRIVACY_PRIVATE:
        pending_privacy.append((person, privacy_for(col)))
    if orcid_skipped:
        counts["orcid_skipped"] += 1


def _merge_onto_existing_person(abc, match_pid, cid, col, research_urls,
                                keywords, existing_curies, person_by_cid,
                                counts) -> None:
    """Attach colleague <cid> to a pre-existing non-SGD person (matched by
    email): add its SGD:Colleague xref and sync the rest, instead of creating a
    duplicate person."""
    person = abc.get(PersonModel, match_pid)
    if person is None:
        return
    if _add_person_xrefs(abc, person, cid, col, existing_curies):
        counts["orcid_skipped"] += 1
    _update_person(abc, person, cid, col, research_urls, keywords,
                   existing_curies, apply=True)
    person_by_cid[cid] = match_pid


def _add_update_persons(abc, colleagues, research_urls, keywords, in_scope,
                        person_by_cid, nonsgd_email, existing_curies, apply,
                        counts) -> None:
    pending_privacy: List[Tuple[PersonModel, str]] = []
    # Dry-run placeholder ids for not-yet-created persons; negative so they
    # never collide with a real (positive) person_id.
    placeholder_id = -1
    for cid in sorted(in_scope):
        col = colleagues[cid]
        pid = person_by_cid.get(cid)
        if pid is not None:
            person = abc.get(PersonModel, pid)
            if person is not None and _update_person(
                    abc, person, cid, col, research_urls, keywords,
                    existing_curies, apply):
                counts["persons_updated"] += 1
                if apply and counts["persons_updated"] % COMMIT_BATCH == 0:
                    abc.commit()
            continue
        # New colleague: reuse a pre-existing non-SGD person with the same
        # email (e.g. AGR staff/author) rather than duplicating it.
        email = _clean(col.get("email"))
        match_pid = nonsgd_email.pop(email.lower(), None) if email else None
        if match_pid is not None:
            counts["persons_merged"] += 1
            if apply:
                _merge_onto_existing_person(
                    abc, match_pid, cid, col, research_urls, keywords,
                    existing_curies, person_by_cid, counts)
                if counts["persons_merged"] % COMMIT_BATCH == 0:
                    abc.commit()
            else:
                # dry-run: record the matched id so member/lineage previews
                # resolve this colleague to its would-be person.
                person_by_cid[cid] = match_pid
            continue
        counts["persons_added"] += 1
        if apply:
            _add_one_person(abc, cid, col, research_urls, keywords,
                            existing_curies, person_by_cid, pending_privacy,
                            counts)
            if counts["persons_added"] % COMMIT_BATCH == 0:
                _flush_privacy(abc, pending_privacy)
                pending_privacy.clear()
                abc.commit()
        else:
            # dry-run: stand-in id so member/lineage previews count this
            # not-yet-created person (matching the committed code path).
            person_by_cid[cid] = placeholder_id
            placeholder_id -= 1
    if apply:
        _flush_privacy(abc, pending_privacy)
        abc.commit()
    logger.info("persons: +%d added, ~%d updated, =%d merged onto existing",
                counts["persons_added"], counts["persons_updated"],
                counts["persons_merged"])


def _delete_obsolete_persons(abc, person_by_cid, dump_all_cids, apply,
                             counts) -> None:
    for cid in [c for c in person_by_cid if c not in dump_all_cids]:
        pid = person_by_cid[cid]
        if abc.execute(text("SELECT 1 FROM users WHERE person_id = :p LIMIT 1"),
                       {"p": pid}).first():
            counts["persons_delete_skipped"] += 1
            logger.warning("obsolete colleague %s (person_id %s) is linked to a "
                           "user account; not deleting", cid, pid)
            continue
        counts["persons_deleted"] += 1
        if apply:
            _delete_person_cascade(abc, pid)
            if counts["persons_deleted"] % COMMIT_BATCH == 0:
                abc.commit()
        del person_by_cid[cid]
    if apply:
        abc.commit()
    logger.info("persons: -%d deleted (%d skipped: user-linked)",
                counts["persons_deleted"], counts["persons_delete_skipped"])


def _add_lab(abc, pi, col, lab_url) -> int:
    lab = LaboratoryModel(
        curie=get_next_laboratory_curie(abc),
        name=f"{display_name_for(col)} Lab",
        institution=[_clean(col.get("institution"))]
        if _clean(col.get("institution")) else None,
        webpage=lab_url or None,
        city=_clean(col.get("city")),
        state=_clean(col.get("state")),
        postal_code=_clean(col.get("postal_code")),
        country=_clean(col.get("country")),
        street_address=build_street_address(col),
        status="active",
    )
    abc.add(lab)
    abc.flush()
    abc.add(LaboratoryCrossReferenceModel(
        laboratory_id=lab.laboratory_id,
        curie=SGD_LAB_CURIE_FMT.format(pi),
        curie_prefix=SGD_PERSON_PREFIX,
    ))
    return lab.laboratory_id


def _add_update_labs(abc, colleagues, lab_urls, lab_members, dump_pis,
                     person_by_cid, already_labs, in_scope, now, apply,
                     do_delete, counts) -> None:
    for pi in dump_pis:
        col = colleagues[pi]
        lab_id = already_labs.get(pi)
        if lab_id is None:
            counts["labs_added"] += 1
            if apply:
                lab_id = _add_lab(abc, pi, col, lab_urls.get(pi))
                already_labs[pi] = lab_id
        elif _update_lab(abc, lab_id, col, lab_urls.get(pi), apply):
            counts["labs_updated"] += 1
        _sync_lab_members(abc, lab_id, pi, lab_members[pi], colleagues,
                          person_by_cid, in_scope, now, apply, do_delete,
                          counts)
        if apply:
            abc.commit()
    logger.info("labs: +%d added, ~%d updated; members +%d/~%d/-%d",
                counts["labs_added"], counts["labs_updated"],
                counts["members_added"], counts["members_updated"],
                counts["members_deleted"])


def _delete_obsolete_labs(abc, already_labs, dump_pis, apply, counts) -> None:
    dump_pi_set = set(dump_pis)
    for pi, lab_id in already_labs.items():
        if pi in dump_pi_set:
            continue
        if abc.execute(text("SELECT 1 FROM laboratory_allele_designation "
                            "WHERE laboratory_id = :l LIMIT 1"),
                       {"l": lab_id}).first():
            counts["labs_delete_skipped"] += 1
            logger.warning("obsolete lab (pi colleague %s, laboratory_id %s) "
                           "has allele designations; not deleting", pi, lab_id)
            continue
        counts["labs_deleted"] += 1
        if apply:
            _delete_lab_cascade(abc, lab_id)
    if apply:
        abc.commit()
    logger.info("labs: -%d deleted (%d skipped: allele designations)",
                counts["labs_deleted"], counts["labs_delete_skipped"])


def _merge_person(abc, dup, keep) -> None:
    """Merge SGD-created person ``dup`` into pre-existing person ``keep``: move
    cross-references, lab memberships and lineage edges, then hard-delete dup.
    Used to repair existing email duplicates (one non-SGD + one SGD person)."""
    keep_curies = {r[0] for r in abc.execute(text(
        "SELECT curie FROM person_cross_reference WHERE person_id = :k"),
        {"k": keep}).fetchall()}
    xrefs = abc.execute(text(
        "SELECT person_cross_reference_id, curie FROM person_cross_reference "
        "WHERE person_id = :d"), {"d": dup}).fetchall()
    for xid, curie in xrefs:
        if curie in keep_curies:
            abc.execute(text("DELETE FROM person_cross_reference "
                             "WHERE person_cross_reference_id = :x"), {"x": xid})
        else:
            abc.execute(text("UPDATE person_cross_reference SET person_id = :k "
                             "WHERE person_cross_reference_id = :x"),
                        {"k": keep, "x": xid})

    memberships = abc.execute(text(
        "SELECT laboratory_person_id, laboratory_id FROM laboratory_person "
        "WHERE person_id = :d"), {"d": dup}).fetchall()
    for lp_id, lab_id in memberships:
        if abc.execute(text("SELECT 1 FROM laboratory_person "
                            "WHERE laboratory_id = :l AND person_id = :k "
                            "LIMIT 1"), {"l": lab_id, "k": keep}).first():
            abc.execute(text("DELETE FROM laboratory_person "
                             "WHERE laboratory_person_id = :i"), {"i": lp_id})
        else:
            abc.execute(text("UPDATE laboratory_person SET person_id = :k "
                             "WHERE laboratory_person_id = :i"),
                        {"k": keep, "i": lp_id})

    edges = abc.execute(text(
        "SELECT person_lineage_id, person_subject_id, person_object_id, "
        "relationship FROM person_lineage "
        "WHERE person_subject_id = :d OR person_object_id = :d"),
        {"d": dup}).fetchall()
    for ll_id, subj, obj, rel in edges:
        ns = keep if subj == dup else subj
        no = keep if obj == dup else obj
        s, o = (ns, no) if ns <= no else (no, ns)
        if s == o or abc.execute(text(
            "SELECT 1 FROM person_lineage WHERE person_subject_id = :s "
            "AND person_object_id = :o AND relationship = :r LIMIT 1"),
                {"s": s, "o": o, "r": rel}).first():
            abc.execute(text("DELETE FROM person_lineage "
                             "WHERE person_lineage_id = :i"), {"i": ll_id})
        else:
            abc.execute(text("UPDATE person_lineage SET person_subject_id = :s, "
                             "person_object_id = :o WHERE person_lineage_id = :i"),
                        {"s": s, "o": o, "i": ll_id})

    _delete_person_cascade(abc, dup)


def merge_email_duplicates(abc, apply, counts) -> None:
    """Repair existing duplicates: where one email is shared by exactly one
    non-SGD person and one SGD person, merge the SGD person into the non-SGD one
    (category ① only). Mixed/ambiguous groups (institutional shared emails) are
    reported and left untouched."""
    rows = abc.execute(text(
        "SELECT lower(email_address) AS email, "
        "array_agg(DISTINCT person_id) AS pids "
        "FROM person_email GROUP BY lower(email_address) "
        "HAVING count(DISTINCT person_id) > 1")).fetchall()
    for email, pids in rows:
        sgd, nonsgd = [], []
        for pid in pids:
            is_sgd = abc.execute(text(
                "SELECT 1 FROM person_cross_reference WHERE person_id = :p "
                "AND curie LIKE 'SGD:Colleague_%' LIMIT 1"),
                {"p": pid}).first() is not None
            (sgd if is_sgd else nonsgd).append(pid)
        if len(nonsgd) == 1 and len(sgd) == 1:
            counts["email_dups_merged"] += 1
            logger.info("merge email %s: SGD person %d -> existing person %d",
                        email, sgd[0], nonsgd[0])
            if apply:
                _merge_person(abc, sgd[0], nonsgd[0])
        else:
            counts["email_dups_skipped"] += 1
            logger.warning("email %s shared by %d non-SGD / %d SGD persons; "
                           "skipping (ambiguous)", email, len(nonsgd), len(sgd))
    if apply:
        abc.commit()


def run_sync(abc, colleagues, research_urls, lab_urls, keywords, lab_members,
             associate_pairs, person_by_cid, nonsgd_email, already_labs,
             existing_curies, in_scope, dump_all_cids, apply, prune, limit,
             merge_dups):
    """Incremental add/update/delete of SGD colleague data into ABC, keyed on
    colleague_id. ``apply`` writes (and commits in batches); otherwise every
    change is detected but rolled back so the returned counts preview a commit.
    Deletes run only with ``prune`` and a full (non-limited) scope."""
    now = datetime.utcnow()
    do_delete = prune and limit is None
    if prune and limit is not None:
        logger.warning("--limit set: skipping all deletes "
                       "(delete reconciliation requires a full run)")
    if apply:
        set_global_user_id(abc, path.basename(__file__).replace(".py", ""))
    counts: Dict[str, int] = defaultdict(int)

    if merge_dups:
        merge_email_duplicates(abc, apply, counts)
        if apply and counts["email_dups_merged"]:
            # Merges moved each SGD:Colleague xref onto the surviving non-SGD
            # person and deleted the old SGD person; refresh the cid->person_id
            # map (built before the merge) so the merged colleagues are
            # re-synced this run rather than only on the next one.
            person_by_cid.clear()
            person_by_cid.update(all_sgd_person_ids_by_cid(abc))

    _add_update_persons(abc, colleagues, research_urls, keywords, in_scope,
                        person_by_cid, nonsgd_email, existing_curies, apply,
                        counts)
    if do_delete:
        _delete_obsolete_persons(abc, person_by_cid, dump_all_cids, apply,
                                 counts)

    dump_pis = [pi for pi in lab_members if pi in in_scope and pi in colleagues]
    _add_update_labs(abc, colleagues, lab_urls, lab_members, dump_pis,
                     person_by_cid, already_labs, in_scope, now, apply,
                     do_delete, counts)
    if do_delete:
        _delete_obsolete_labs(abc, already_labs, dump_pis, apply, counts)

    _sync_collaborators(abc, associate_pairs, person_by_cid, in_scope,
                        apply, do_delete, counts)
    if apply:
        abc.commit()
    logger.info("collaborator_of: +%d added, -%d deleted (%d unresolved)",
                counts["lineage_added"], counts["lineage_deleted"],
                counts["lineage_skipped_unresolved"])

    if not apply:
        abc.rollback()
    return counts


def _flush_privacy(abc, pending: List[Tuple[PersonModel, str]]) -> None:
    for person, value in pending:
        abc.execute(
            text("UPDATE person SET privacy = :v WHERE person_id = :id"),
            {"v": value, "id": person.person_id},
        )


# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--datafile", required=True,
                    help="JSON dump produced by dumpColleague.py")
    ap.add_argument("--commit", action="store_true",
                    help="write to ABC (default is a read-only dry-run that "
                         "previews the same add/update/delete counts)")
    ap.add_argument("--prune", action="store_true",
                    help="also DELETE obsolete rows (colleagues/labs/lineage "
                         "no longer in the SGD dump); requires a full run "
                         "(ignored with --limit)")
    ap.add_argument("--merge-email-dups", action="store_true",
                    help="repair existing duplicates: merge an SGD person into "
                         "a pre-existing non-SGD person sharing its email "
                         "(one-to-one matches only)")
    ap.add_argument("--limit", type=int, default=None,
                    help="process only the first N colleagues (testing)")
    ap.add_argument("--outdir", default=".",
                    help="directory for dry-run TSV previews")
    args = ap.parse_args()

    logger.info("loading SGD dump: %s", args.datafile)
    (colleagues, research_urls, lab_urls, keywords,
     lab_members, associate_pairs, skipped) = load_dump(args.datafile)
    logger.info("dump: %d colleagues, %d labs (Head-of-Lab PIs), "
                "%d Associate pairs",
                len(colleagues), len(lab_members), len(associate_pairs))

    cids = sorted(colleagues)
    if args.limit:
        cids = cids[:args.limit]
    in_scope = set(cids)
    dump_all_cids = set(colleagues)

    abc = create_postgres_session(False)
    try:
        person_by_cid = all_sgd_person_ids_by_cid(abc)
        nonsgd_email = nonsgd_person_email_map(abc)
        already_labs = existing_lab_xrefs(abc)
        existing_curies = existing_person_curies(abc)
        logger.info("ABC already has %d SGD persons, %d SGD labs, "
                    "%d person cross-reference curies, "
                    "%d non-SGD person emails",
                    len(person_by_cid), len(already_labs),
                    len(existing_curies), len(nonsgd_email))

        counts = run_sync(
            abc, colleagues, research_urls, lab_urls, keywords, lab_members,
            associate_pairs, person_by_cid, nonsgd_email, already_labs,
            existing_curies, in_scope, dump_all_cids, apply=args.commit,
            prune=args.prune, limit=args.limit,
            merge_dups=args.merge_email_dups)
        _report(counts, skipped, apply=args.commit, prune=args.prune,
                limit=args.limit, scope=len(in_scope))
        if not args.commit:
            _write_previews(colleagues, research_urls, keywords, lab_members,
                            in_scope, args.outdir)
    finally:
        abc.close()


if __name__ == "__main__":
    main()
