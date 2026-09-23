"""SCRUM-6128: populate curation_status from the Caltech oa_data curation dump.

Reads WormBase's Caltech "oa_data" curation_status export (a public TSV) and, for
every distinct (ATP topic, reference) pair, creates a WB curation_status row. The
script has two run modes, mirroring
SCRUM-6130_populate_curation_status_from_caltech_positive.py:

  --mode populate   (default) INSERT a WB curation_status row for each new
                    (topic, reference) pair. Dry-run unless --commit is given.

  --mode report     Write a read-only, per-ATP-topic report to --output. Never
                    touches the DB.

The source TSV is entry-level (one row per curated entity), so many rows share the
same (topic, reference); the script collapses them to one curation_status row per
(topic, reference):

  * status tie-break: prefer 'curated' (ATP:0000239) over 'curation in progress'
    (ATP:0000237) when a pair has both;
  * among the entries with the winning status, the earliest curator_timestamp
    (the original curation event) supplies the curator and date.

Row mapping (per winning entry):

  topic            <- topic_atp                    (TSV column 5)
  reference_id     <- reference for the WB curie    (TSV column 4, e.g. WB:WBPaper..)
  mod_id           <- WB
  curation_status  <- curation_status_atp           (TSV column 6, per-row)
  curation_tag     =  ATP:0000227   ('curatable')   (constant)
  note             =  NULL
  created_by       <- curator                        (TSV column 9, already WBPerson<n>)
  updated_by       <- curator
  date_created     <- curator_timestamp              (TSV column 10, parsed to UTC)
  date_updated     <- curator_timestamp

SCRUM-6518: each inserted base row also gets one curation_status_source_association
to the tag_source looked up (never created) by source_method='ontology_annotator',
data_provider='WB', secondary_data_provider_id=<WB mod>, and
validation_type='professional_biocurator'. The association carries the same
status/tag/note and the same curator/timestamp as the base row.

The audit fields are set explicitly so the original curator and timestamp are
preserved: AuditedModel.before_insert only fills date/user fields when they are
None, and it auto-creates the referenced created_by/updated_by users.

Idempotent and non-destructive: an existing (topic, reference_id, mod_id) row is
never updated, so re-runs insert nothing new; a pre-existing row that matches the
target value is reported as already-present, and one that differs (e.g. a
validated-negative backfill row, ATP:0000299) as a conflict.

By default every ATP topic in the file is processed; restrict with --topics
(comma-separated) or --topics all.

TSV url defaults to the 20260827 oa_data snapshot; override with env
WB_OADATA_TSV_URL (the /files/pub/ path is public, no credentials).

Run against literature-4005 by loading its env file first, from the repository
root (the directory that contains the agr_literature_service package), e.g.:

    cd /home/azurebrd/git/api_general
    ENV="$(grep -v '^#' agr_literature_service/.env.devserver_4005 | xargs)"
    BASE=agr_literature_service/lit_processing/oneoff_scripts
    SCRIPT=$BASE/SCRUM-6128_populate_curation_status_from_caltech_oadata.py

    # report mode (read-only):
    env $ENV python $SCRIPT --mode report --output oadata_report.txt

    # populate mode (dry-run, then real insert):
    env $ENV python $SCRIPT --mode populate
    env $ENV python $SCRIPT --mode populate --commit

(The filename contains a hyphen, so it cannot be run with `python -m`; run the
file path directly.)
"""

import argparse
import logging
import os
import re
import urllib.request
from collections import defaultdict
from datetime import datetime

import pytz

from agr_literature_service.api.crud.ateam_db_helpers import map_curies_to_names
from agr_literature_service.api.models import (
    CrossReferenceModel,
    CurationStatusModel,
    CurationStatusSourceAssociationModel,
    ModModel,
    ReferenceModel,
    TagSourceModel,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session


logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


CURATED = "ATP:0000239"           # 'curated' (preferred in tie-break)
IN_PROGRESS = "ATP:0000237"       # 'curation in progress'
CURATION_TAG = "ATP:0000227"      # 'curatable'
MOD_ABBREVIATION = "WB"
WB_CURIE_PREFIX = "WB"

# SCRUM-6518: every inserted curation_status row gets one source association to
# the tag_source identified by these parameters (looked up, never created).
# secondary_data_provider_id is the WB mod_id, resolved at run time.
SOURCE_METHOD = "ontology_annotator"
SOURCE_DATA_PROVIDER = "WB"
SOURCE_VALIDATION_TYPE = "professional_biocurator"

STATUS_LABEL = {CURATED: "curated", IN_PROGRESS: "curation-in-progress"}

DEFAULT_TSV_URL = (
    "https://caltech-curation.textpressolab.com/files/pub/kimberly/"
    "20260827_oa_data/curation_status.20260916_142702.entries.tsv"
)
TSV_URL = os.environ.get("WB_OADATA_TSV_URL", DEFAULT_TSV_URL)

DEFAULT_REPORT_FILE = "curation_status_oadata.txt"
BATCH_COMMIT_SIZE = 500


def fetch_tsv_rows():
    """Fetch the TSV live and yield dicts of the columns we need."""
    logger.info(f"fetching {TSV_URL}")
    with urllib.request.urlopen(TSV_URL, timeout=180) as response:
        text = response.read().decode("utf-8")
    lines = text.splitlines()
    rows = []
    for line in lines[1:]:            # skip header
        if not line.strip():
            continue
        f = line.split("\t")
        if len(f) < 11:
            logger.warning(f"skipping malformed row (<11 cols): {line!r}")
            continue
        rows.append({
            "datatype": f[1].strip(),
            "reference": f[3].strip(),
            "topic": f[4].strip(),                 # topic_atp
            "status": f[5].strip(),                # curation_status_atp
            "curator": f[8].strip(),               # curator (col 9)
            "timestamp": f[9].strip(),             # curator_timestamp (col 10)
        })
    logger.info(f"TSV data rows: {len(rows)}")
    return rows


def parse_timestamp(value):
    """Parse a Caltech curator_timestamp into an aware UTC datetime, or None.

    Returns None for a blank value. Handles values like '2004-07-05 21:34:13-07'
    by normalising a trailing +/-HH offset to +/-HH:00 before
    datetime.fromisoformat.
    """
    if not value.strip():
        return None
    normalised = re.sub(r"([+-]\d{2})$", r"\1:00", value.strip())
    dt = datetime.fromisoformat(normalised)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    return dt.astimezone(pytz.UTC)


def collapse_entries(rows, topics):
    """Collapse entry-level rows to one winning entry per (topic, reference).

    Winner: prefer CURATED over IN_PROGRESS; tie-break on earliest timestamp.
    Only rows whose topic is in ``topics`` (a set, or None for all) are kept.
    Returns (best-dict keyed by (topic, reference), filtered_out count).
    """
    best = {}
    filtered_out = 0
    for row in rows:
        topic = row["topic"]
        if topics is not None and topic not in topics:
            filtered_out += 1
            continue
        ts = parse_timestamp(row["timestamp"])
        if ts is None:
            # Blank curator_timestamp (a handful of entries); every (topic,
            # reference) pair has at least one dated entry, so skip these.
            continue
        key = (topic, row["reference"])
        rank = 1 if row["status"] == CURATED else 0
        current = best.get(key)
        if (current is None
                or rank > current["rank"]
                or (rank == current["rank"] and ts < current["ts"])):
            best[key] = {
                "topic": topic,
                "reference": row["reference"],
                "datatype": row["datatype"],
                "status": row["status"],
                "curator": row["curator"],
                "ts": ts,
                "rank": rank,
            }
    return best, filtered_out


def resolve_references(db, curies):
    """Map WB curie -> (reference_id, agrkb_curie) for the given WB:WBPaper curies."""
    result = {}
    curie_list = sorted(curies)
    if not curie_list:
        return result
    batch = 5000
    for i in range(0, len(curie_list), batch):
        chunk = curie_list[i:i + batch]
        rows = (
            db.query(
                CrossReferenceModel.curie,
                CrossReferenceModel.reference_id,
                ReferenceModel.curie,
            )
            .join(
                ReferenceModel,
                CrossReferenceModel.reference_id == ReferenceModel.reference_id,
            )
            .filter(
                CrossReferenceModel.curie_prefix == WB_CURIE_PREFIX,
                CrossReferenceModel.is_obsolete.is_(False),
                CrossReferenceModel.curie.in_(chunk),
            )
            .all()
        )
        for curie, reference_id, agrkb in rows:
            result[curie] = (reference_id, agrkb)
    return result


def resolve_tag_source_id(db, wb_mod_id):
    """Look up (never create) the tag_source for this load's attribution.

    Keyed by source_method / data_provider / secondary_data_provider_id /
    validation_type; expects exactly one match. Raises if 0 or >1 so a missing
    or ambiguous source fails loudly rather than silently mis-attributing.
    """
    matches = (
        db.query(TagSourceModel.tag_source_id,
                 TagSourceModel.source_evidence_assertion)
        .filter(
            TagSourceModel.source_method == SOURCE_METHOD,
            TagSourceModel.data_provider == SOURCE_DATA_PROVIDER,
            TagSourceModel.secondary_data_provider_id == wb_mod_id,
            TagSourceModel.validation_type == SOURCE_VALIDATION_TYPE,
        )
        .all()
    )
    if len(matches) != 1:
        raise RuntimeError(
            f"expected exactly 1 tag_source for source_method={SOURCE_METHOD}, "
            f"data_provider={SOURCE_DATA_PROVIDER}, secondary={wb_mod_id}, "
            f"validation_type={SOURCE_VALIDATION_TYPE}; found {len(matches)}"
        )
    tag_source_id, sea = matches[0]
    logger.info(f"tag_source_id: {tag_source_id} (source_evidence_assertion={sea})")
    return tag_source_id


def classify(db, rows, topics):
    """Collapse + resolve + classify. Returns
    (wb_mod_id, records, not_found_rows, filtered_out).

    records: one dict per resolvable (topic, reference) with reference_id / agrkb /
    status / curator / ts / datatype / classification ('new'|'already-present'|
    'conflict') and existing value.
    """
    wb_mod_id = (
        db.query(ModModel.mod_id)
        .filter(ModModel.abbreviation == MOD_ABBREVIATION)
        .scalar()
    )
    logger.info(f"WB mod_id: {wb_mod_id}")

    best, filtered_out = collapse_entries(rows, topics)
    logger.info(f"distinct (topic, reference) pairs: {len(best)}")

    ref_map = resolve_references(db, {b["reference"] for b in best.values()})
    logger.info(f"references resolved: {len(ref_map)}")

    existing = {}
    for topic, reference_id, status, tag in db.query(
        CurationStatusModel.topic,
        CurationStatusModel.reference_id,
        CurationStatusModel.curation_status,
        CurationStatusModel.curation_tag,
    ).filter(CurationStatusModel.mod_id == wb_mod_id):
        existing[(topic, reference_id)] = (status, tag)

    records = []
    not_found_rows = []
    for entry in best.values():
        curie = entry["reference"]
        if curie not in ref_map:
            not_found_rows.append(entry)
            continue
        reference_id, agrkb = ref_map[curie]
        prior = existing.get((entry["topic"], reference_id))
        if prior is None:
            classification = "new"
        elif prior == (entry["status"], CURATION_TAG):
            classification = "already-present"
        else:
            classification = "conflict"
        records.append({
            **entry,
            "reference_id": reference_id,
            "agrkb": agrkb,
            "classification": classification,
            "existing": prior,
        })
    return wb_mod_id, records, not_found_rows, filtered_out


def run_populate(db, wb_mod_id, tag_source_id, records, not_found_rows,
                 filtered_out, commit):
    """Insert curation_status rows for the 'new' records (only when commit).

    Each new base row also gets one source association to tag_source_id carrying
    the same status/tag/note (SCRUM-6518), with the same curator/timestamp.
    """
    inserted = 0
    for record in records:
        if record["classification"] != "new":
            continue
        if commit:
            curator = record["curator"] or None
            db.add(CurationStatusModel(
                topic=record["topic"],
                reference_id=record["reference_id"],
                mod_id=wb_mod_id,
                curation_status=record["status"],
                curation_tag=CURATION_TAG,
                note=None,
                created_by=curator,
                updated_by=curator,
                date_created=record["ts"],
                date_updated=record["ts"],
                source_associations=[CurationStatusSourceAssociationModel(
                    tag_source_id=tag_source_id,
                    curation_status=record["status"],
                    curation_tag=CURATION_TAG,
                    note=None,
                    created_by=curator,
                    updated_by=curator,
                    date_created=record["ts"],
                    date_updated=record["ts"],
                )],
            ))
            if (inserted + 1) % BATCH_COMMIT_SIZE == 0:
                db.commit()
                logger.info(f"  committed {inserted + 1} so far")
        inserted += 1
    if commit:
        db.commit()

    conflicts = [r for r in records if r["classification"] == "conflict"]
    already = sum(1 for r in records if r["classification"] == "already-present")

    logger.info("")
    logger.info("=== SUMMARY ===")
    logger.info(f"  skipped (topic not selected):     {filtered_out}")
    logger.info(f"  reference not found in DB:         {len(not_found_rows)}")
    logger.info(f"  skipped (already have a value):    {already + len(conflicts)}")
    logger.info(f"    of which CONFLICTS (value != this row): {len(conflicts)}")
    verb = "INSERTED" if commit else "WOULD INSERT"
    logger.info(f"  {verb} (new curation_status rows): {inserted}")
    if not commit:
        logger.info("")
        logger.info("  DRY RUN -- no rows written. Re-run with --commit to insert.")


def run_report(total_rows, records, not_found_rows, filtered_out, topics_display,
               tag_source_id, output_file):
    """Write a read-only detailed report grouped by ATP topic."""
    by_topic = defaultdict(list)
    for record in records:
        by_topic[record["topic"]].append(record)
    names = map_curies_to_names("atp", sorted(by_topic))

    n_new = sum(1 for r in records if r["classification"] == "new")
    n_already = sum(1 for r in records if r["classification"] == "already-present")
    n_conflict = sum(1 for r in records if r["classification"] == "conflict")

    with open(output_file, "w") as out:
        out.write("Caltech oa_data curation_status  vs  literature curation_status (WB)\n")
        out.write(f"source TSV : {TSV_URL}\n")
        out.write("tag        : curation_tag=ATP:0000227 (curatable); status from the file\n")
        out.write("tie-break  : prefer curated (ATP:0000239); earliest timestamp of winner\n")
        out.write(f"tag_source : {tag_source_id} (source_method={SOURCE_METHOD}, "
                  f"validation_type={SOURCE_VALIDATION_TYPE}); attached to each new row\n")
        out.write("mode       : report (read-only; no database writes)\n")
        out.write(f"topics     : {topics_display}\n\n")
        out.write("=== SUMMARY ===\n")
        out.write(f"  TSV data rows (entries)            : {total_rows}\n")
        out.write(f"  skipped (topic not selected)       : {filtered_out}\n")
        out.write(f"  distinct (topic,reference) pairs   : {len(records) + len(not_found_rows)}\n")
        out.write(f"  reference not found in DB          : {len(not_found_rows)}\n")
        out.write(f"  new (would insert)                 : {n_new}\n")
        out.write(f"  already present (same value)       : {n_already}\n")
        out.write(f"  CONFLICT (other existing value)    : {n_conflict}\n\n")

        for atp in sorted(by_topic, key=lambda a: (-len(by_topic[a]), a)):
            entries = by_topic[atp]
            out.write(f"=== {atp}  {names.get(atp, atp)}  ({len(entries)} papers) ===\n")
            for r in sorted(entries, key=lambda x: x["reference"]):
                if r["classification"] == "conflict":
                    status, tag = r["existing"]
                    label = f"CONFLICT existing={status}/{tag}"
                else:
                    label = r["classification"]
                out.write(f"  reference_id={r['reference_id']}  {r['agrkb']}  "
                          f"{r['reference']}  {r['datatype']}  "
                          f"{STATUS_LABEL.get(r['status'], r['status'])}  "
                          f"{r['curator']}  [{label}]\n")
            out.write("\n")

        if not_found_rows:
            out.write(f"=== REFERENCES NOT FOUND IN DB ({len(not_found_rows)}) ===\n")
            for r in not_found_rows:
                out.write(f"  {r['reference']}  {r['datatype']}  topic={r['topic']}\n")

    logger.info(f"new={n_new} already-present={n_already} conflict={n_conflict} "
                f"not_found={len(not_found_rows)}")
    logger.info(f"wrote {output_file}")


def main(mode, commit, output_file, topics):
    topics_display = ", ".join(sorted(topics)) if topics is not None else "all"
    logger.info(f"selected topics: {topics_display}")
    rows = fetch_tsv_rows()
    db = create_postgres_session(False)
    try:
        wb_mod_id, records, not_found_rows, filtered_out = classify(db, rows, topics)
        tag_source_id = resolve_tag_source_id(db, wb_mod_id)
        if mode == "report":
            run_report(len(rows), records, not_found_rows, filtered_out,
                       topics_display, tag_source_id, output_file)
        else:
            run_populate(db, wb_mod_id, tag_source_id, records, not_found_rows,
                         filtered_out, commit)
    except Exception as e:
        db.rollback()
        logger.error(f"error during {mode}, rolled back: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Caltech oa_data -> curation_status (populate or report)."
    )
    parser.add_argument(
        "--mode",
        choices=["populate", "report"],
        default="populate",
        help="populate: insert curation_status rows (dry-run unless --commit); "
             "report: write a read-only detailed report to --output",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="populate mode only: actually insert rows (default: dry-run, no writes)",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_REPORT_FILE,
        help=f"report mode only: output file path (default: {DEFAULT_REPORT_FILE})",
    )
    parser.add_argument(
        "--topics",
        default="all",
        help="comma-separated ATP topics to process, or 'all' for every topic "
             "(default: all)",
    )
    args = parser.parse_args()
    if args.commit and args.mode != "populate":
        parser.error("--commit is only valid with --mode populate")
    if args.topics.strip().lower() == "all":
        topics = None
    else:
        topics = {t.strip() for t in args.topics.split(",") if t.strip()}
        if not topics:
            parser.error("--topics is empty; pass ATP curies or 'all'")
    main(mode=args.mode, commit=args.commit, output_file=args.output, topics=topics)
