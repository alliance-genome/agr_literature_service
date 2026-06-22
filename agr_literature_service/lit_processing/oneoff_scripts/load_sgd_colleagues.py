"""
Load SGD colleague data into ABC person / laboratory tables.

Source : SGD ``nex`` schema (read-only, via ``NEX2_URI``)
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
  * Phone numbers are skipped.
  * Idempotent on cross-reference ``SGD:Colleague_<colleague_id>`` (person) and
    ``SGD:Lab_<pi_colleague_id>`` (laboratory): rows already present are skipped.

Reported but not loaded (no clean ABC target):
  * ``colleague_relation`` 'Associate' rows
  * ``colleague_locus`` (colleague <-> gene links)
  * ``colleague.suffix`` / ``profession`` / phones / ``is_beta_tester``

Usage::

    set -a
    source agr_literature_service/.env_cc          # ABC: PSQL_*, ID_MATI_URL, ENV_STATE
    source ../SGDBackend-Nex2/.env_cc              # SGD: NEX2_URI
    set +a

    # read-only dry-run (default) -> prints projected counts, writes TSV previews
    python -m agr_literature_service.lit_processing.oneoff_scripts.load_sgd_colleagues

    # small test against the dev DB (mints real MATI curies for the subset)
    python -m agr_literature_service.lit_processing.oneoff_scripts.load_sgd_colleagues \
        --commit --limit 50

    # full load
    python -m agr_literature_service.lit_processing.oneoff_scripts.load_sgd_colleagues --commit

NOTE: in ``--commit`` mode each person and laboratory consumes one real MATI id
(ENV_STATE != 'test'); MATI's counter does not roll back. Re-runs are safe
because already-loaded colleagues are detected via their SGD cross-reference.
"""

import argparse
import csv
import logging
from datetime import datetime
from os import environ, path
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy import create_engine, text

from agr_literature_service.api.models import (
    PersonModel,
    PersonNameModel,
    PersonEmailModel,
    PersonCrossReferenceModel,
    PersonNoteModel,
    LaboratoryModel,
    LaboratoryPersonModel,
    LaboratoryCrossReferenceModel,
)
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

# does the ORM map person.privacy on this branch? (SCRUM-6157 adds it). The dev
# DB already has the column with a NOT NULL default of 'hide_email', so when the
# model lacks it we set the non-default ('show_all') values with a raw UPDATE.
PERSON_HAS_PRIVACY = hasattr(PersonModel, "privacy")

COMMIT_BATCH = 200


# --------------------------------------------------------------------------- #
# SGD extraction (read-only)
# --------------------------------------------------------------------------- #
def get_sgd_engine():
    uri = environ.get("NEX2_URI")
    if not uri:
        raise SystemExit(
            "NEX2_URI is not set. Source SGDBackend-Nex2/.env_cc first."
        )
    return create_engine(uri)


def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = " ".join(str(value).split())
    return value or None


def extract_colleagues(sgd) -> Dict[int, dict]:
    rows = sgd.execute(text("""
        SELECT colleague_id, display_name, obj_url, orcid,
               first_name, middle_name, last_name, other_last_name,
               job_title, institution,
               address1, address2, address3, city, state, country, postal_code,
               email, display_email, research_interest, colleague_note
        FROM nex.colleague
        ORDER BY colleague_id
    """)).mappings().all()
    return {r["colleague_id"]: dict(r) for r in rows}


def extract_webpages(sgd) -> Tuple[Dict[int, List[str]], Dict[int, List[str]]]:
    """Return (research_summary_urls, lab_urls) keyed by colleague_id."""
    research: Dict[int, List[str]] = {}
    lab: Dict[int, List[str]] = {}
    rows = sgd.execute(text("""
        SELECT colleague_id, url_type, obj_url
        FROM nex.colleague_url
        WHERE coalesce(trim(obj_url), '') <> ''
        ORDER BY colleague_id, url_id
    """)).mappings().all()
    for r in rows:
        target = research if r["url_type"] == "Research summary" else lab
        target.setdefault(r["colleague_id"], []).append(r["obj_url"].strip())
    return research, lab


def extract_keywords(sgd) -> Dict[int, List[str]]:
    keywords: Dict[int, List[str]] = {}
    rows = sgd.execute(text("""
        SELECT ck.colleague_id, k.display_name
        FROM nex.colleague_keyword ck
        JOIN nex.keyword k ON ck.keyword_id = k.keyword_id
        ORDER BY ck.colleague_id
    """)).mappings().all()
    for r in rows:
        kw = _clean(r["display_name"])
        if kw:
            keywords.setdefault(r["colleague_id"], []).append(kw)
    return keywords


def extract_lab_graph(sgd) -> Dict[int, Set[int]]:
    """
    From 'Head of Lab' rows: associate_id is the PI (head), colleague_id is the
    lab member. Returns {pi_colleague_id: {member_colleague_id, ...}}.
    """
    members: Dict[int, Set[int]] = {}
    rows = sgd.execute(text("""
        SELECT colleague_id AS member_id, associate_id AS pi_id
        FROM nex.colleague_relation
        WHERE association_type = 'Head of Lab'
    """)).mappings().all()
    for r in rows:
        members.setdefault(r["pi_id"], set()).add(r["member_id"])
    return members


def count_skipped(sgd) -> Dict[str, int]:
    return {
        "associate_relations": sgd.execute(text(
            "SELECT count(*) FROM nex.colleague_relation "
            "WHERE association_type = 'Associate'"
        )).scalar(),
        "colleague_locus": sgd.execute(text(
            "SELECT count(*) FROM nex.colleague_locus"
        )).scalar(),
    }


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
    if dn:
        return dn
    first = _clean(col.get("first_name")) or ""
    last = _clean(col.get("last_name")) or ""
    return _clean(f"{first} {last}") or f"Colleague {col['colleague_id']}"


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
def dry_run(colleagues, research_urls, keywords, lab_members,
            already_persons, skipped, limit, outdir) -> None:
    cids = sorted(colleagues)
    if limit:
        cids = cids[:limit]
    in_scope = set(cids)
    to_create = [c for c in cids if c not in already_persons]

    n_email = n_public = n_private = n_orcid = n_note = n_bio = 0
    n_secondary_name = 0
    for cid in to_create:
        col = colleagues[cid]
        if _clean(col.get("email")):
            n_email += 1
            if col.get("display_email"):
                n_public += 1
            else:
                n_private += 1
        if orcid_curie(col):
            n_orcid += 1
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
    logger.info("person_cross_reference (ORCID)    : %d", n_orcid)
    logger.info("person_note                       : %d", n_note)
    logger.info("biography_research_interest filled: %d", n_bio)
    logger.info("-" * 64)
    logger.info("laboratory              in scope  : %d", len(labs))
    logger.info("laboratory_person (PI)            : %d", pi_rows)
    logger.info("laboratory_person (member)        : %d", member_rows)
    logger.info("laboratory_person TOTAL           : %d", pi_rows + member_rows)
    logger.info("-" * 64)
    logger.info("SKIPPED (no ABC target):")
    logger.info("  colleague_relation 'Associate'  : %d", skipped["associate_relations"])
    logger.info("  colleague_locus (gene links)    : %d", skipped["colleague_locus"])
    logger.info("  phones / suffix / profession / is_beta_tester: dropped")
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
def commit(abc, colleagues, research_urls, lab_urls, keywords, lab_members,
           already_persons, already_labs, limit) -> None:
    now = datetime.utcnow()
    cids = sorted(colleagues)
    if limit:
        cids = cids[:limit]
    in_scope = set(cids)
    to_create = [c for c in cids if c not in already_persons]

    set_global_user_id(abc, path.basename(__file__).replace(".py", ""))

    # ---- persons -------------------------------------------------------- #
    colleague_to_person: Dict[int, int] = {}
    pending_privacy: List[Tuple[PersonModel, str]] = []
    created = 0
    for cid in to_create:
        col = colleagues[cid]
        privacy = privacy_for(col)

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
            kwargs["privacy"] = privacy
        person = PersonModel(**kwargs)
        abc.add(person)
        abc.flush()
        colleague_to_person[cid] = person.person_id

        if not PERSON_HAS_PRIVACY and privacy != PRIVACY_PRIVATE:
            pending_privacy.append((person, privacy))

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
        abc.add(PersonCrossReferenceModel(
            person_id=person.person_id,
            curie=SGD_PERSON_CURIE_FMT.format(cid),
            curie_prefix=SGD_PERSON_PREFIX,
            pages=[col["obj_url"]] if _clean(col.get("obj_url")) else None,
        ))
        oc = orcid_curie(col)
        if oc:
            abc.add(PersonCrossReferenceModel(
                person_id=person.person_id,
                curie=oc,
                curie_prefix=ORCID_PREFIX,
            ))
        if _clean(col.get("colleague_note")):
            abc.add(PersonNoteModel(
                person_id=person.person_id,
                note=_clean(col.get("colleague_note")),
            ))

        created += 1
        if created % COMMIT_BATCH == 0:
            _flush_privacy(abc, pending_privacy)
            pending_privacy.clear()
            abc.commit()
            logger.info("  persons committed: %d / %d", created, len(to_create))

    _flush_privacy(abc, pending_privacy)
    abc.commit()
    logger.info("persons created: %d", created)

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


# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--commit", action="store_true",
                    help="write to ABC (default is a read-only dry-run)")
    ap.add_argument("--limit", type=int, default=None,
                    help="process only the first N colleagues (testing)")
    ap.add_argument("--outdir", default=".",
                    help="directory for dry-run TSV previews")
    args = ap.parse_args()

    sgd = get_sgd_engine().connect()
    try:
        logger.info("extracting from SGD ...")
        colleagues = extract_colleagues(sgd)
        research_urls, lab_urls = extract_webpages(sgd)
        keywords = extract_keywords(sgd)
        lab_members = extract_lab_graph(sgd)
        skipped = count_skipped(sgd)
        logger.info("SGD: %d colleagues, %d labs (Head-of-Lab PIs)",
                    len(colleagues), len(lab_members))
    finally:
        sgd.close()

    abc = create_postgres_session(False)
    try:
        already_persons = existing_person_xrefs(abc)
        already_labs = existing_lab_xrefs(abc)
        logger.info("ABC already has %d SGD persons, %d SGD labs",
                    len(already_persons), len(already_labs))

        if args.commit:
            commit(abc, colleagues, research_urls, lab_urls, keywords,
                   lab_members, already_persons, already_labs, args.limit)
        else:
            dry_run(colleagues, research_urls, keywords, lab_members,
                    already_persons, skipped, args.limit, args.outdir)
    finally:
        abc.close()


if __name__ == "__main__":
    main()
