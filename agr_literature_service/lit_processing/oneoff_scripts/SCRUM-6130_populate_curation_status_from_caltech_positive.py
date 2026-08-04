"""SCRUM-6130: Caltech curated-positive TSV -> literature curation_status (WB).

Reads WormBase's Caltech "curated positive" export (a public TSV) and, for every
(WBPaper, ATP topic) row, resolves the paper to its literature reference and
compares against the WB curation_status table. The script has two run modes:

  --mode populate   (default) INSERT a WB curation_status row for each new
                    (topic, reference) pair, mirroring the curation event.
                    Dry-run unless --commit is given.

  --mode report     Write a read-only, human-readable report grouped by ATP
                    topic (one line per paper: reference_id, AGRKB, WB:WBPaper,
                    datatype, curator, and status) to --output. Never touches
                    the DB.

Both modes share the same source parsing and classification, so their counts
always agree. Each source (paper, ATP) pair is classified as:

  new              no curation_status row yet -> would be inserted by populate
  already-curated  a row exists with curation_status=ATP:0000239/tag=ATP:0000227
  conflict         a row exists with a DIFFERENT value (e.g. a validated-negative
                   backfill row, ATP:0000299/ATP:0000226) -- reported, never
                   overwritten
  blank-ATP        the source row has no ATP topic (e.g. exprmosaic /
                   geneticmosaic datatypes) -- skipped and listed, never inserted
                   with an empty topic
  not-found        WB:WBPaper<id> has no reference in the DB -- skipped and listed

populate mode row mapping:

  topic            <- atp                         (TSV column 'atp')
  reference_id     <- reference for WB:WBPaper<cur_paper>
  mod_id           <- WB
  curation_status  =  ATP:0000239   ('curated')   (constant)
  curation_tag     =  ATP:0000227   ('curatable') (constant)
  note             <- cur_selcomment / cur_txtcomment joined
  created_by       <- cur_curator
  updated_by       <- cur_curator
  date_created     <- cur_timestamp (parsed, converted to UTC)
  date_updated     <- cur_timestamp

The audit fields are set explicitly so the original curator and timestamp are
preserved: AuditedModel.before_insert only fills date/user fields when they are
None, and it auto-creates the referenced created_by/updated_by users, so a
curator id that is not yet in the users table does not violate the FK.

populate mode is idempotent and non-destructive: an existing (topic,
reference_id, mod_id) row is never updated, so re-runs insert nothing new and
conflicts are reported rather than overwritten. Resolving conflicts is a
separate, deliberate step.

TSV url defaults to the 20260729 snapshot; override with env
WB_CURATED_POSITIVE_TSV_URL (the /files/pub/ path is public, no credentials).

Run against literature-4005 by loading its env file first, from the repository
root (the directory that contains the agr_literature_service package), e.g.:

    cd /home/azurebrd/git/api_general
    ENV="$(grep -v '^#' agr_literature_service/.env.devserver_4005 | xargs)"
    BASE=agr_literature_service/lit_processing/oneoff_scripts
    SCRIPT=$BASE/SCRUM-6130_populate_curation_status_from_caltech_positive.py

    # report mode (read-only):
    env $ENV python $SCRIPT --mode report --output curated_positive_report.txt

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
    ModModel,
    ReferenceModel,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session


logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


CURATION_STATUS = "ATP:0000239"   # 'curated'
CURATION_TAG = "ATP:0000227"      # 'curatable'
MOD_ABBREVIATION = "WB"
WB_CURIE_PREFIX = "WB"

DEFAULT_TSV_URL = (
    "https://caltech-curation.textpressolab.com/files/pub/kimberly/"
    "20260729_curated_positive_oa_blank/"
    "curated_positive_oa_blank.20260729_175018.tsv"
)
TSV_URL = os.environ.get("WB_CURATED_POSITIVE_TSV_URL", DEFAULT_TSV_URL)

DEFAULT_REPORT_FILE = "curated_positive_vs_curation_status.txt"
BATCH_COMMIT_SIZE = 500


def fetch_tsv_rows():
    """Fetch the TSV live and return a list of dicts (one per data row)."""
    logger.info(f"fetching {TSV_URL}")
    with urllib.request.urlopen(TSV_URL, timeout=60) as response:
        text = response.read().decode("utf-8")
    lines = text.splitlines()
    rows = []
    for line in lines[1:]:            # skip header
        if not line.strip():
            continue
        f = line.split("\t")
        if len(f) < 7:
            logger.warning(f"skipping malformed row (<7 cols): {line!r}")
            continue
        rows.append({
            "paper": f[0].strip(),
            "datatype": f[1].strip(),
            "atp": f[2].strip(),
            "curator": f[3].strip(),
            "selcomment": f[4].strip(),
            "txtcomment": f[5].strip(),
            "timestamp": f[6].strip(),
        })
    logger.info(f"TSV data rows: {len(rows)}")
    return rows


def build_note(selcomment, txtcomment):
    """Join the two TSV comment columns; return None when both are blank."""
    parts = [p for p in (selcomment, txtcomment) if p]
    return "; ".join(parts) or None


def parse_timestamp(value):
    """Parse a Caltech cur_timestamp into an aware UTC datetime.

    Handles values like '2014-09-10 11:05:29.968652-07' by normalising a
    trailing +/-HH offset to +/-HH:00 before datetime.fromisoformat.
    """
    normalised = re.sub(r"([+-]\d{2})$", r"\1:00", value.strip())
    dt = datetime.fromisoformat(normalised)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    return dt.astimezone(pytz.UTC)


def resolve_references(db, papers):
    """Map WB:WBPaper<id> -> (reference_id, agrkb_curie) for the given paper ids."""
    curies = ["WB:WBPaper" + p for p in papers]
    result = {}
    if not curies:
        return result
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
            CrossReferenceModel.curie.in_(curies),
        )
        .all()
    )
    for curie, reference_id, agrkb in rows:
        result[curie] = (reference_id, agrkb)
    return result


def classify(db, rows):
    """Resolve + classify every TSV row against WB curation_status.

    Returns (wb_mod_id, records, blank_rows, not_found_rows) where records is a
    list of dicts (one per distinct (atp, reference_id) pair) each carrying the
    source row fields plus wb_curie / reference_id / agrkb / status / existing.
    status is one of 'new', 'already-curated', 'conflict'.
    """
    wb_mod_id = (
        db.query(ModModel.mod_id)
        .filter(ModModel.abbreviation == MOD_ABBREVIATION)
        .scalar()
    )
    logger.info(f"WB mod_id: {wb_mod_id}")

    ref_map = resolve_references(db, {r["paper"] for r in rows})
    logger.info(f"papers resolved to a reference: {len(ref_map)}")

    existing = {}
    for topic, reference_id, status, tag in db.query(
        CurationStatusModel.topic,
        CurationStatusModel.reference_id,
        CurationStatusModel.curation_status,
        CurationStatusModel.curation_tag,
    ).filter(CurationStatusModel.mod_id == wb_mod_id):
        existing[(topic, reference_id)] = (status, tag)

    records = []
    blank_rows = []
    not_found_rows = []
    seen = set()
    for row in rows:
        if not row["atp"]:
            blank_rows.append(row)
            continue
        curie = "WB:WBPaper" + row["paper"]
        if curie not in ref_map:
            not_found_rows.append(row)
            continue
        reference_id, agrkb = ref_map[curie]
        key = (row["atp"], reference_id)
        if key in seen:
            continue
        seen.add(key)
        prior = existing.get(key)
        if prior is None:
            status = "new"
        elif prior == (CURATION_STATUS, CURATION_TAG):
            status = "already-curated"
        else:
            status = "conflict"
        records.append({
            **row,
            "wb_curie": curie,
            "reference_id": reference_id,
            "agrkb": agrkb,
            "status": status,
            "existing": prior,
        })
    return wb_mod_id, records, blank_rows, not_found_rows


def run_populate(db, wb_mod_id, records, blank_rows, not_found_rows, commit):
    """Insert curation_status rows for the 'new' records (only when commit)."""
    inserted = 0
    for record in records:
        if record["status"] != "new":
            continue
        if commit:
            db.add(CurationStatusModel(
                topic=record["atp"],
                reference_id=record["reference_id"],
                mod_id=wb_mod_id,
                curation_status=CURATION_STATUS,
                curation_tag=CURATION_TAG,
                note=build_note(record["selcomment"], record["txtcomment"]),
                created_by=record["curator"] or None,
                updated_by=record["curator"] or None,
                date_created=parse_timestamp(record["timestamp"]),
                date_updated=parse_timestamp(record["timestamp"]),
            ))
            if (inserted + 1) % BATCH_COMMIT_SIZE == 0:
                db.commit()
                logger.info(f"  committed {inserted + 1} so far")
        inserted += 1
    if commit:
        db.commit()

    conflicts = [r for r in records if r["status"] == "conflict"]
    already = sum(1 for r in records if r["status"] == "already-curated")

    logger.info("")
    logger.info("=== SUMMARY ===")
    logger.info(f"  paper not found in DB:            {len(not_found_rows)}")
    logger.info(f"  skipped (blank ATP topic):        {len(blank_rows)}")
    logger.info(f"  skipped (already have a value):   {already + len(conflicts)}")
    logger.info(f"    of which CONFLICTS (value != curated/curatable): {len(conflicts)}")
    verb = "INSERTED" if commit else "WOULD INSERT"
    logger.info(f"  {verb} (new curated rows):        {inserted}")
    if blank_rows:
        datatypes = sorted({r["datatype"] for r in blank_rows})
        logger.info(f"  (blank-ATP datatypes skipped: {', '.join(datatypes)})")
    if conflicts:
        logger.info("")
        logger.info("  conflicts (in TSV as positive, but already valued in DB):")
        logger.info("    %-18s %-22s %-12s %-13s %-13s %s"
                    % ("WBPaper", "AGRKB", "datatype", "atp",
                       "existing_status", "existing_tag"))
        for r in conflicts:
            status, tag = r["existing"]
            logger.info("    %-18s %-22s %-12s %-13s %-13s %s"
                        % (r["wb_curie"], r["agrkb"], r["datatype"], r["atp"],
                           status, tag))
    if not commit:
        logger.info("")
        logger.info("  DRY RUN -- no rows written. Re-run with --commit to insert.")


def run_report(total_rows, records, blank_rows, not_found_rows, output_file):
    """Write a read-only detailed report grouped by ATP topic."""
    by_topic = defaultdict(list)
    for record in records:
        by_topic[record["atp"]].append(record)
    names = map_curies_to_names("atp", sorted(by_topic))

    n_new = sum(1 for r in records if r["status"] == "new")
    n_already = sum(1 for r in records if r["status"] == "already-curated")
    n_conflict = sum(1 for r in records if r["status"] == "conflict")

    with open(output_file, "w") as out:
        out.write("Caltech curated-positive TSV  vs  literature curation_status (WB)\n")
        out.write(f"source TSV : {TSV_URL}\n")
        out.write("assigns    : curation_status=ATP:0000239 (curated), "
                  "curation_tag=ATP:0000227 (curatable)\n")
        out.write("mode       : report (read-only; no database writes)\n\n")
        out.write("=== SUMMARY ===\n")
        out.write(f"  TSV data rows                      : {total_rows}\n")
        out.write(f"  distinct (paper,atp) pairs shown   : {len(records)}\n")
        out.write(f"  papers not found in DB             : {len(not_found_rows)}\n")
        out.write(f"  skipped (blank ATP topic)          : {len(blank_rows)}\n")
        out.write(f"  new (would insert)                 : {n_new}\n")
        out.write(f"  already curated (curated/curatable): {n_already}\n")
        out.write(f"  CONFLICT (other existing value)    : {n_conflict}\n\n")

        for atp in sorted(by_topic, key=lambda a: (-len(by_topic[a]), a)):
            entries = by_topic[atp]
            out.write(f"=== {atp}  {names.get(atp, atp)}  ({len(entries)} papers) ===\n")
            for r in sorted(entries, key=lambda x: x["wb_curie"]):
                if r["status"] == "conflict":
                    status, tag = r["existing"]
                    label = f"CONFLICT existing={status}/{tag}"
                else:
                    label = r["status"]
                out.write(f"  reference_id={r['reference_id']}  {r['agrkb']}  "
                          f"{r['wb_curie']}  {r['datatype']}  {r['curator']}  "
                          f"[{label}]\n")
            out.write("\n")

        if blank_rows:
            out.write(f"=== BLANK-ATP ROWS SKIPPED ({len(blank_rows)}) ===\n")
            for r in blank_rows:
                out.write(f"  WB:WBPaper{r['paper']}  {r['datatype']}  "
                          f"{r['curator']}  {r['timestamp']}\n")
            out.write("\n")
        if not_found_rows:
            out.write(f"=== PAPERS NOT FOUND IN DB ({len(not_found_rows)}) ===\n")
            for r in not_found_rows:
                out.write(f"  WB:WBPaper{r['paper']}  {r['datatype']}  atp={r['atp']}\n")

    logger.info(f"new={n_new} already-curated={n_already} conflict={n_conflict} "
                f"blank={len(blank_rows)} not_found={len(not_found_rows)}")
    logger.info(f"wrote {output_file}")


def main(mode, commit, output_file):
    rows = fetch_tsv_rows()
    db = create_postgres_session(False)
    try:
        wb_mod_id, records, blank_rows, not_found_rows = classify(db, rows)
        if mode == "report":
            run_report(len(rows), records, blank_rows, not_found_rows, output_file)
        else:
            run_populate(db, wb_mod_id, records, blank_rows, not_found_rows, commit)
    except Exception as e:
        db.rollback()
        logger.error(f"error during {mode}, rolled back: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Caltech curated-positive TSV -> curation_status (populate or report)."
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
    args = parser.parse_args()
    if args.commit and args.mode != "populate":
        parser.error("--commit is only valid with --mode populate")
    main(mode=args.mode, commit=args.commit, output_file=args.output)
