"""
Load SGD colleague data into ABC person / laboratory tables.

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
  * ``colleague_keyword`` is folded into ``person.biography_research_interest``.
  * ``colleague.suffix`` (Jr./Sr./III...) is appended to ``person.display_name``.
  * ``colleague_relation`` 'Associate' -> ``person_lineage`` collaborator_of.
    SGD stores these mirrored (A<->B); collaborator_of is symmetric, so each
    pair is normalized to ascending person-id order and deduped.
  * Phone numbers are skipped.
  * Idempotent on cross-reference ``SGD:Colleague_<colleague_id>`` (person) and
    ``SGD:Lab_<pi_colleague_id>`` (laboratory); person_lineage collaborator_of
    rows are idempotent on (subject, object, relationship).

Reported but not loaded (no clean ABC target):
  * ``colleague_locus`` (colleague <-> gene links)
  * ``colleague.profession`` / phones / ``is_beta_tester``

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

    # small test against the dev DB (mints real MATI curies for the subset)
    python -m agr_literature_service.lit_processing.oneoff_scripts.load_sgd_colleagues \
        --datafile /tmp/colleague.json --commit --limit 50

    # full load
    python -m agr_literature_service.lit_processing.oneoff_scripts.load_sgd_colleagues \
        --datafile /tmp/colleague.json --commit

NOTE: in ``--commit`` mode each person and laboratory consumes one real MATI id
(ENV_STATE != 'test'); MATI's counter does not roll back. Re-runs are safe
because already-loaded colleagues are detected via their SGD cross-reference.
"""

import argparse
import csv
import json
import logging
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
def existing_person_xrefs(abc) -> Set[int]:
    rows = abc.execute(text(
        "SELECT curie FROM person_cross_reference "
        "WHERE curie_prefix = :p AND curie LIKE 'SGD:Colleague_%'"
    ), {"p": SGD_PERSON_PREFIX}).fetchall()
    out: Set[int] = set()
    for (curie,) in rows:
        try:
            out.add(int(curie.rsplit("_", 1)[-1]))
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


# --------------------------------------------------------------------------- #
# Dry-run report
# --------------------------------------------------------------------------- #
def dry_run(colleagues, research_urls, keywords, lab_members, associate_pairs,
            already_persons, existing_curies, skipped, limit, outdir) -> None:
    cids = sorted(colleagues)
    if limit:
        cids = cids[:limit]
    in_scope = set(cids)
    to_create = [c for c in cids if c not in already_persons]

    n_email = n_public = n_private = n_orcid = n_orcid_skip = 0
    n_note = n_bio = n_secondary_name = 0
    seen_curies = set(existing_curies)
    for cid in to_create:
        col = colleagues[cid]
        if _clean(col.get("email")):
            n_email += 1
            if col.get("display_email"):
                n_public += 1
            else:
                n_private += 1
        oc = orcid_curie(col)
        if oc:
            if oc in seen_curies:
                n_orcid_skip += 1
            else:
                n_orcid += 1
                seen_curies.add(oc)
        if _clean(col.get("colleague_note")):
            n_note += 1
        if build_biography(col, keywords.get(cid, [])):
            n_bio += 1
        if _clean(col.get("other_last_name")):
            n_secondary_name += 1

    # labs: PIs in scope; members restricted to in-scope persons
    labs = [pi for pi in lab_members if pi in in_scope and pi in colleagues]
    pi_rows = 0
    member_rows = 0
    for pi in labs:
        pi_rows += 1
        member_rows += len({m for m in lab_members[pi] if m in in_scope})

    show_all_total = sum(1 for c in to_create
                         if colleagues[c].get("display_email"))

    # collaborator_of rows: both endpoints must be in scope (commit also
    # requires both to resolve to loaded persons; here we project on scope).
    collaborator_rows = sum(1 for c1, c2 in associate_pairs
                            if c1 in in_scope and c2 in in_scope and c1 != c2)

    logger.info("=" * 64)
    logger.info("DRY-RUN  (no writes)   scope=%s colleagues",
                len(cids))
    logger.info("=" * 64)
    logger.info("persons already loaded (skipped) : %d", len(in_scope) - len(to_create))
    logger.info("person                  to create: %d", len(to_create))
    logger.info("person_name (primary)   to create: %d", len(to_create))
    logger.info("person_name (secondary) to create: %d", n_secondary_name)
    logger.info("person_email            to create: %d "
                "(public=%d -> show_all, private=%d -> hide_email)",
                n_email, n_public, n_private)
    logger.info("person.privacy = show_all set for : %d", show_all_total)
    logger.info("person_cross_reference (SGD)      : %d", len(to_create))
    logger.info("person_cross_reference (ORCID)    : %d "
                "(%d skipped: curie already exists)", n_orcid, n_orcid_skip)
    logger.info("person_note                       : %d", n_note)
    logger.info("biography_research_interest filled: %d", n_bio)
    logger.info("-" * 64)
    logger.info("laboratory              in scope  : %d", len(labs))
    logger.info("laboratory_person (PI)            : %d", pi_rows)
    logger.info("laboratory_person (member)        : %d", member_rows)
    logger.info("laboratory_person TOTAL           : %d", pi_rows + member_rows)
    logger.info("-" * 64)
    logger.info("person_lineage (collaborator_of)  : %d "
                "(from %d deduped Associate pairs)",
                collaborator_rows, len(associate_pairs))
    logger.info("-" * 64)
    logger.info("SKIPPED (no ABC target):")
    logger.info("  colleague_locus (gene links)    : %d", skipped["colleague_locus"])
    logger.info("  phones / profession / is_beta_tester: dropped "
                "(suffix -> appended to display_name)")
    logger.info("=" * 64)
    if not PERSON_HAS_PRIVACY:
        logger.warning("PersonModel does not map 'privacy' on this branch "
                       "(SCRUM-6157 not merged here). Commit mode would set "
                       "'show_all' via raw UPDATE; 'hide_email' via DB default.")

    _write_previews(colleagues, research_urls, keywords, lab_members,
                    to_create, labs, in_scope, outdir)


def _write_previews(colleagues, research_urls, keywords, lab_members,
                    to_create, labs, in_scope, outdir) -> None:
    ppath = path.join(outdir, "preview_persons.tsv")
    with open(ppath, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["sgd_colleague_id", "display_name", "last_name", "email",
                    "privacy", "orcid_curie", "n_webpages", "biography_snippet"])
        for cid in to_create[:500]:
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


def commit(abc, colleagues, research_urls, lab_urls, keywords, lab_members,
           associate_pairs, already_persons, already_labs, existing_curies,
           limit) -> None:
    now = datetime.utcnow()
    cids = sorted(colleagues)
    if limit:
        cids = cids[:limit]
    in_scope = set(cids)
    to_create = [c for c in cids if c not in already_persons]

    set_global_user_id(abc, path.basename(__file__).replace(".py", ""))

    # person_cross_reference.curie is globally unique (uq_person_xref_curie):
    # an ORCID may already belong to an existing ABC person (e.g. AGR staff who
    # are also SGD colleagues), and two SGD colleagues can share an ORCID.
    # existing_curies tracks every curie already present plus every one added in
    # this run, so any cross-reference whose curie collides is skipped.

    # ---- persons -------------------------------------------------------- #
    colleague_to_person: Dict[int, int] = {}
    pending_privacy: List[Tuple[PersonModel, str]] = []
    created = 0
    skipped_orcid = 0
    for cid in to_create:
        col = colleagues[cid]
        person, orcid_skipped = _create_person(
            abc, cid, col, research_urls, keywords, existing_curies)
        colleague_to_person[cid] = person.person_id
        if not PERSON_HAS_PRIVACY and privacy_for(col) != PRIVACY_PRIVATE:
            pending_privacy.append((person, privacy_for(col)))
        if orcid_skipped:
            skipped_orcid += 1

        created += 1
        if created % COMMIT_BATCH == 0:
            _flush_privacy(abc, pending_privacy)
            pending_privacy.clear()
            abc.commit()
            logger.info("  persons committed: %d / %d", created, len(to_create))

    _flush_privacy(abc, pending_privacy)
    abc.commit()
    logger.info("persons created: %d", created)
    if skipped_orcid:
        logger.info("ORCID cross-references skipped (already exist): %d",
                    skipped_orcid)

    # map every in-scope colleague (new + pre-existing) to a person_id
    for cid in existing_person_map(abc, in_scope).items():
        colleague_to_person.setdefault(*cid)

    # ---- laboratories + memberships ------------------------------------ #
    labs = [pi for pi in lab_members
            if pi in in_scope and pi in colleagues]
    labs_created = 0
    lp_created = 0
    for pi in labs:
        if pi in already_labs:
            laboratory_id = already_labs[pi]
        else:
            col = colleagues[pi]
            lab = LaboratoryModel(
                curie=get_next_laboratory_curie(abc),
                name=f"{display_name_for(col)} Lab",
                institution=[_clean(col.get("institution"))]
                if _clean(col.get("institution")) else None,
                webpage=lab_urls.get(pi) or None,
                city=_clean(col.get("city")),
                state=_clean(col.get("state")),
                postal_code=_clean(col.get("postal_code")),
                country=_clean(col.get("country")),
                street_address=build_street_address(col),
                status="active",
            )
            abc.add(lab)
            abc.flush()
            laboratory_id = lab.laboratory_id
            abc.add(LaboratoryCrossReferenceModel(
                laboratory_id=laboratory_id,
                curie=SGD_LAB_CURIE_FMT.format(pi),
                curie_prefix=SGD_PERSON_PREFIX,
            ))
            labs_created += 1

        # PI membership row
        pi_person = colleague_to_person.get(pi)
        if pi_person and not _lab_person_exists(abc, laboratory_id, pi_person):
            abc.add(LaboratoryPersonModel(
                laboratory_id=laboratory_id,
                person_id=pi_person,
                is_pi=now,
                lab_position=_clean(colleagues[pi].get("job_title"))
                or "Principal Investigator",
            ))
            lp_created += 1

        # member rows
        for member in sorted(lab_members[pi]):
            if member not in in_scope:
                continue
            mp = colleague_to_person.get(member)
            if not mp or _lab_person_exists(abc, laboratory_id, mp):
                continue
            abc.add(LaboratoryPersonModel(
                laboratory_id=laboratory_id,
                person_id=mp,
                lab_position=_clean(colleagues[member].get("job_title")),
            ))
            lp_created += 1

        if (labs_created + 1) % COMMIT_BATCH == 0:
            abc.commit()
            logger.info("  labs committed (running): %d", labs_created)

    abc.commit()
    logger.info("laboratories created: %d", labs_created)
    logger.info("laboratory_person rows created: %d", lp_created)

    _load_collaborators(abc, associate_pairs, colleague_to_person, in_scope)


def _load_collaborators(abc, associate_pairs, colleague_to_person,
                        in_scope) -> None:
    """Load SGD 'Associate' pairs into person_lineage as collaborator_of.

    collaborator_of is symmetric, so each pair is normalized to ascending
    person-id order (matching person_lineage_crud._normalize_pair) and deduped
    against the uq_person_lineage_person_ids_relationship constraint. Pairs are
    skipped when either colleague is out of scope or was not loaded as a person.
    """
    created = 0
    skipped_unresolved = 0
    for c1, c2 in associate_pairs:
        if c1 not in in_scope or c2 not in in_scope:
            skipped_unresolved += 1
            continue
        p1 = colleague_to_person.get(c1)
        p2 = colleague_to_person.get(c2)
        if not p1 or not p2 or p1 == p2:
            skipped_unresolved += 1
            continue
        subject_id, object_id = (p1, p2) if p1 < p2 else (p2, p1)
        if _person_lineage_exists(abc, subject_id, object_id):
            continue
        abc.add(PersonLineageModel(
            person_subject_id=subject_id,
            person_object_id=object_id,
            relationship=COLLABORATOR_OF,
        ))
        created += 1
        if created % COMMIT_BATCH == 0:
            abc.commit()
            logger.info("  collaborator_of committed (running): %d", created)

    abc.commit()
    logger.info("person_lineage (collaborator_of) rows created: %d", created)
    if skipped_unresolved:
        logger.info("Associate pairs skipped (endpoint out of scope / not "
                    "loaded): %d", skipped_unresolved)


def _flush_privacy(abc, pending: List[Tuple[PersonModel, str]]) -> None:
    for person, value in pending:
        abc.execute(
            text("UPDATE person SET privacy = :v WHERE person_id = :id"),
            {"v": value, "id": person.person_id},
        )


def existing_person_map(abc, in_scope: Set[int]) -> Dict[int, int]:
    rows = abc.execute(text(
        "SELECT curie, person_id FROM person_cross_reference "
        "WHERE curie_prefix = :p AND curie LIKE 'SGD:Colleague_%'"
    ), {"p": SGD_PERSON_PREFIX}).fetchall()
    out: Dict[int, int] = {}
    for curie, pid in rows:
        try:
            cid = int(curie.rsplit("_", 1)[-1])
        except ValueError:
            continue
        if cid in in_scope:
            out[cid] = pid
    return out


def _lab_person_exists(abc, laboratory_id: int, person_id: int) -> bool:
    return abc.execute(text(
        "SELECT 1 FROM laboratory_person "
        "WHERE laboratory_id = :l AND person_id = :p LIMIT 1"
    ), {"l": laboratory_id, "p": person_id}).first() is not None


def _person_lineage_exists(abc, subject_id: int, object_id: int) -> bool:
    return abc.execute(text(
        "SELECT 1 FROM person_lineage "
        "WHERE person_subject_id = :s AND person_object_id = :o "
        "AND relationship = :r LIMIT 1"
    ), {"s": subject_id, "o": object_id, "r": COLLABORATOR_OF}
    ).first() is not None


# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--datafile", required=True,
                    help="JSON dump produced by dumpColleague.py")
    ap.add_argument("--commit", action="store_true",
                    help="write to ABC (default is a read-only dry-run)")
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

    abc = create_postgres_session(False)
    try:
        already_persons = existing_person_xrefs(abc)
        already_labs = existing_lab_xrefs(abc)
        existing_curies = existing_person_curies(abc)
        logger.info("ABC already has %d SGD persons, %d SGD labs, "
                    "%d person cross-reference curies",
                    len(already_persons), len(already_labs),
                    len(existing_curies))

        if args.commit:
            commit(abc, colleagues, research_urls, lab_urls, keywords,
                   lab_members, associate_pairs, already_persons, already_labs,
                   existing_curies, args.limit)
        else:
            dry_run(colleagues, research_urls, keywords, lab_members,
                    associate_pairs, already_persons, existing_curies, skipped,
                    args.limit, args.outdir)
    finally:
        abc.close()


if __name__ == "__main__":
    main()
