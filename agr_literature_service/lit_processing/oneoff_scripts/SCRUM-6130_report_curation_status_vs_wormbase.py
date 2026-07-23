"""SCRUM-6130: report curation_status (mod=WB) rows that are NOT at Caltech.

Starting from every curation_status row that matches the "negated
curation-status-form" signature produced by the SCRUM-6130 backfill:

  curation_status = ATP:0000299
  curation_tag    = ATP:0000226
  mod             = WB

grouped by topic, this compares the WormBase literature DB (e.g. literature-4005)
against the Caltech "validated negative" curation_status pages and reports the
discrepancies.

For each topic the output has:

  * a header line with the ATP curie, the readable topic name (resolved via the
    same helper the API uses, ateam_db_helpers.map_curies_to_names), and the total
    reference count for that topic;
  * a count of references that have no WB cross_reference curie;
  * one line per reference that is KEPT, where a reference is kept when either
      - it has no WB curie, or
      - it has a WB curie but that WBPaper is NOT present on the Caltech page(s)
        for the topic (i.e. it is at 4005 but not at Caltech).
    References whose WBPaper IS present at Caltech are skipped.

Each ATP topic is mapped to one or more Caltech datatype names (WB_TO_ATP below).
The Caltech page for each datatype lists its "validated negative" papers as
8-digit ids in a <textarea name="specific_papers"> block; every id corresponds to
WB:WBPaper<8digits>. All sibling datatype names for a topic are unioned.

Caltech access uses HTTP Basic auth. Credentials are taken from a "user:pass" file
(default ~/.wormbaseweb, override with env WB_WEB_CRED_FILE); if that file is
absent, the script prompts interactively for the username and password. As an
offline alternative, set env WB_CALTECH_CACHE to a JSON file mapping datatype
name -> list of 8-digit ids to skip the live fetch (and the credential) entirely.

Read-only: this script never writes to the literature database.

Run against literature-4005 by loading its env file first, from the repository
root (the directory that contains the agr_literature_service package), e.g.:

    cd /home/azurebrd/git/api_general
    env $(grep -v '^#' agr_literature_service/.env.devserver_4005 | xargs) \
        python agr_literature_service/lit_processing/oneoff_scripts/SCRUM-6130_report_curation_status_vs_wormbase.py

(The filename contains a hyphen, so it cannot be run with `python -m`; run the
file path directly.)

An optional output path may be passed as the first argument (default:
curation_status_wb_not_at_caltech.txt in the current directory).
"""

import getpass
import json
import logging
import os
import re
import sys
import urllib.request
from collections import defaultdict
from urllib.parse import quote

from agr_literature_service.api.crud.ateam_db_helpers import map_curies_to_names
from agr_literature_service.api.models import (
    CrossReferenceModel,
    CurationStatusModel,
    ModModel,
    ReferenceModel,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session


logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


CURATION_STATUS = "ATP:0000299"
CURATION_TAG = "ATP:0000226"
MOD_ABBREVIATION = "WB"
WB_CURIE_PREFIX = "WB"

DEFAULT_OUTPUT_FILE = "curation_status_wb_not_at_caltech.txt"

CRED_FILE = os.environ.get("WB_WEB_CRED_FILE", os.path.expanduser("~/.wormbaseweb"))

# Optional pre-fetched Caltech data: a JSON file mapping datatype name -> list of
# 8-digit WBPaper ids. When set, the script uses it instead of fetching the pages
# live (so no Caltech credential is needed at run time).
CALTECH_CACHE_FILE = os.environ.get("WB_CALTECH_CACHE")

CALTECH_HOST = "https://caltech-curation.textpressolab.com"
CALTECH_URL_TEMPLATE = (
    CALTECH_HOST
    + "/priv/cgi-bin/curation_status.cgi"
    + "?action=listCurationStatisticsPapersPage"
    + "&select_datatypesource=caltech"
    + "&select_curator=two1823"
    + "&method=" + quote("allval neg")
    + "&checkbox_cfp=on&checkbox_afp=on&checkbox_str=on&checkbox_nnc=on&checkbox_svm=on"
    + "&listDatatype={datatype}"
)

# WormBase Caltech datatype name -> ATP topic curie.
WB_TO_ATP = {
    "antibody": "ATP:0000096",
    "otherantibody": "ATP:0000096",
    "catalyticact": "ATP:0000061",
    "chemicals": "ATP:0000278",
    "chemphen": "ATP:0000350",
    "seqfeat": "ATP:0000055",
    "extvariation": "ATP:0000285",
    "othervariation": "ATP:0000285",
    "variation": "ATP:0000285",
    "disease": "ATP:0000011",
    "humdis": "ATP:0000152",
    "humandisease": "ATP:0000152",
    "domanal": "ATP:0000089",
    "envpheno": "ATP:0000351",
    "funccomp": "ATP:0000071",
    "genestudied": "ATP:0000005",
    "additionalexpr": "ATP:0000041",
    "marker": "ATP:0000041",
    "otherexpr": "ATP:0000041",
    "structcorr": "ATP:0000054",
    "geneint": "ATP:0000068",
    "lsrnai": "ATP:0000352",
    "othergenefunc": "ATP:0000060",
    "genesymbol": "ATP:0000048",
    "overexpr": "ATP:0000084",
    "geneprod": "ATP:0000069",
    "genereg": "ATP:0000070",
    "rnai": "ATP:0000082",
    "siteaction": "ATP:0000033",
    "otherspecies": "ATP:0000123",
    "species": "ATP:0000123",
    "newstrains": "ATP:0000027",
    "otherstrain": "ATP:0000027",
    "strain": "ATP:0000027",
    "timeaction": "ATP:0000349",
    "othertransgene": "ATP:0000110",
    "transgene": "ATP:0000110",
    "transporter": "ATP:0000062",
    "seqchange": "ATP:0000056",
    "newmutant": "ATP:0000083",
}


def atp_to_wb_names():
    """Invert WB_TO_ATP: ATP curie -> sorted list of Caltech datatype names."""
    result = defaultdict(list)
    for wb_name, atp in WB_TO_ATP.items():
        result[atp].append(wb_name)
    for atp in result:
        result[atp].sort()
    return result


def collect_rows(db):
    """Return matching curation_status rows joined to their AGRKB curie.

    Each element is a tuple (topic, reference_id, agrkb_curie).
    """
    return (
        db.query(
            CurationStatusModel.topic,
            CurationStatusModel.reference_id,
            ReferenceModel.curie,
        )
        .join(
            ReferenceModel,
            CurationStatusModel.reference_id == ReferenceModel.reference_id,
        )
        .join(
            ModModel,
            CurationStatusModel.mod_id == ModModel.mod_id,
        )
        .filter(
            CurationStatusModel.curation_status == CURATION_STATUS,
            CurationStatusModel.curation_tag == CURATION_TAG,
            ModModel.abbreviation == MOD_ABBREVIATION,
        )
        .order_by(CurationStatusModel.topic, CurationStatusModel.reference_id)
        .all()
    )


def wb_curies_by_reference(db, reference_ids):
    """Map reference_id -> list of non-obsolete WB cross_reference curies."""
    result = defaultdict(list)
    if not reference_ids:
        return result
    rows = (
        db.query(
            CrossReferenceModel.reference_id,
            CrossReferenceModel.curie,
        )
        .filter(
            CrossReferenceModel.reference_id.in_(reference_ids),
            CrossReferenceModel.curie_prefix == WB_CURIE_PREFIX,
            CrossReferenceModel.is_obsolete.is_(False),
        )
        .order_by(CrossReferenceModel.reference_id, CrossReferenceModel.curie)
        .all()
    )
    for reference_id, curie in rows:
        result[reference_id].append(curie)
    return result


def wbpaper_number(curie):
    """Extract the zero-padded 8-digit WBPaper number from a WB curie, or None."""
    match = re.search(r"WBPaper0*(\d+)", curie or "")
    return match.group(1).zfill(8) if match else None


def resolve_credentials():
    """Return (username, password) for Caltech Basic auth.

    Prefer the credential file (default ~/.wormbaseweb, "user:pass" on one line);
    if it is absent or unreadable, prompt interactively for the username and
    password.
    """
    if os.path.exists(CRED_FILE):
        try:
            with open(CRED_FILE) as f:
                cred = f.read().strip()
            if ":" in cred:
                user, password = cred.split(":", 1)
                logger.info(f"using Caltech credentials from {CRED_FILE}")
                return user, password
            logger.warning(f"{CRED_FILE} is not in user:pass form; prompting instead")
        except OSError as e:
            logger.warning(f"could not read {CRED_FILE} ({e}); prompting instead")

    logger.info("Caltech credentials not found in a file; please enter them.")
    user = input("WormBase web username [wormbase]: ").strip() or "wormbase"
    password = getpass.getpass("WormBase web password: ")
    return user, password


def _build_opener():
    user, password = resolve_credentials()
    mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    mgr.add_password(None, CALTECH_HOST + "/", user, password)
    return urllib.request.build_opener(urllib.request.HTTPBasicAuthHandler(mgr))


def fetch_caltech_papers(opener, datatype):
    """Return the set of 8-digit WBPaper ids validated-negative for a datatype."""
    url = CALTECH_URL_TEMPLATE.format(datatype=datatype)
    with opener.open(url, timeout=120) as response:
        html = response.read().decode("latin-1")
    match = re.search(
        r'<textarea[^>]*name="specific_papers">(.*?)</textarea>', html, re.S
    )
    if match is None:
        # A 200 response with no specific_papers textarea means the page is not
        # what we expect (layout change, auth interstitial, renamed field). Treat
        # it as a fetch failure so the topic is marked incomplete rather than
        # silently reporting every reference as "not at Caltech". An empty (but
        # present) textarea is legitimate and yields an empty set.
        raise ValueError(
            f"no specific_papers textarea in Caltech page for '{datatype}'"
        )
    return set(re.findall(r"\b\d{8}\b", match.group(1)))


def caltech_sets_by_topic(topics):
    """For each topic, union the Caltech paper ids across its sibling datatypes.

    Uses the pre-fetched WB_CALTECH_CACHE JSON when set, otherwise fetches the
    pages live via HTTP Basic auth.

    Returns (topic -> set-of-ids, topic -> list-of-datatype-names,
             set-of-topics-with-fetch-errors).
    """
    atp_wb = atp_to_wb_names()
    cache = {}

    if CALTECH_CACHE_FILE:
        logger.info(f"using pre-fetched Caltech cache: {CALTECH_CACHE_FILE}")
        try:
            with open(CALTECH_CACHE_FILE) as f:
                cached = json.load(f)
        except (OSError, ValueError) as e:
            raise SystemExit(
                f"could not load WB_CALTECH_CACHE {CALTECH_CACHE_FILE}: {e}"
            )
        opener = None
    else:
        cached = None
        opener = _build_opener()

    topic_sets = {}
    topic_names = {}
    errored = set()
    for topic in topics:
        names = atp_wb.get(topic, [])
        topic_names[topic] = names
        ids = set()
        for name in names:
            if name not in cache:
                try:
                    if cached is not None:
                        if name not in cached:
                            raise KeyError(f"{name} missing from cache")
                        cache[name] = set(cached[name])
                    else:
                        cache[name] = fetch_caltech_papers(opener, name)
                    logger.info(f"  caltech {name}: {len(cache[name])} papers")
                except Exception as e:
                    logger.error(f"  caltech {name}: FAILED: {e}")
                    cache[name] = None
            if cache[name] is None:
                errored.add(topic)
            else:
                ids |= cache[name]
        topic_sets[topic] = ids
    return topic_sets, topic_names, errored


def report(output_file=DEFAULT_OUTPUT_FILE):
    db = create_postgres_session(False)
    try:
        rows = collect_rows(db)
        logger.info(f"matching curation_status rows (mod={MOD_ABBREVIATION}): {len(rows)}")

        reference_ids = {reference_id for _, reference_id, _ in rows}
        wb_map = wb_curies_by_reference(db, reference_ids)

        topics = sorted({topic for topic, _, _ in rows})
        atp_names = map_curies_to_names("atp", topics)
        logger.info(f"distinct topics: {len(topics)}")

        by_topic = defaultdict(list)
        for topic, reference_id, agrkb_curie in rows:
            by_topic[topic].append((reference_id, agrkb_curie))
    finally:
        db.close()

    logger.info("fetching Caltech validated-negative pages ...")
    caltech_sets, caltech_names, errored = caltech_sets_by_topic(topics)

    total_no_wb = 0
    total_kept_not_caltech = 0
    total_skipped = 0

    with open(output_file, "w") as out:
        for topic in topics:
            entries = by_topic[topic]
            name = atp_names.get(topic, topic)
            count = len(entries)
            noun = "reference" if count == 1 else "references"
            caltech_ids = caltech_sets.get(topic, set())
            datatypes = caltech_names.get(topic, [])

            if not datatypes:
                mapping_note = "  (no Caltech datatype mapping)"
            elif topic in errored:
                mapping_note = f"  (Caltech datatypes: {', '.join(datatypes)} - FETCH INCOMPLETE)"
            else:
                mapping_note = f"  (Caltech datatypes: {', '.join(datatypes)})"

            out.write(f"=== {topic}  {name}  ({count} {noun}) ==={mapping_note}\n")

            # First pass: tally references without a WB curie.
            no_wb = sum(1 for reference_id, _ in entries if not wb_map.get(reference_id))
            total_no_wb += no_wb
            out.write(f"  references without a WB curie: {no_wb}\n")

            for reference_id, agrkb_curie in entries:
                wb_curies = wb_map.get(reference_id, [])
                if not wb_curies:
                    out.write(
                        f"  reference_id={reference_id}  {agrkb_curie}  (no WB curie)\n"
                    )
                    continue
                numbers = [wbpaper_number(c) for c in wb_curies]
                at_caltech = any(n and n in caltech_ids for n in numbers)
                if at_caltech:
                    total_skipped += 1
                    continue
                total_kept_not_caltech += 1
                out.write(
                    f"  reference_id={reference_id}  {agrkb_curie}  "
                    f"{', '.join(wb_curies)}\n"
                )
            out.write("\n")

    logger.info(
        f"done: {len(rows)} rows across {len(topics)} topics; "
        f"{total_no_wb} without WB curie; "
        f"{total_kept_not_caltech} at 4005 but not at Caltech; "
        f"{total_skipped} present at Caltech (skipped). "
        f"wrote {output_file}"
    )
    if errored:
        logger.error(f"WARNING: Caltech fetch incomplete for topics: {sorted(errored)}")


if __name__ == "__main__":
    output_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_FILE
    report(output_path)
