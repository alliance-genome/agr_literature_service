"""SCRUM-6130: populate curation_status from the Caltech curated-positive TSV.

Reads WormBase's Caltech "curated positive" export (a public TSV) and, for every
(WBPaper, ATP topic) row, creates a WB curation_status row in the literature DB
mirroring the curation event:

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

Idempotent and non-destructive: a curation_status row already present for a
(topic, reference_id, mod_id) key is left untouched (never updated), so re-runs
insert nothing new AND pre-existing rows -- including any that disagree with this
positive set (e.g. rows previously backfilled as validated-negative,
ATP:0000299/ATP:0000226) -- are reported as conflicts rather than overwritten.
Resolving those conflicts is a separate, deliberate step.

Dry-run by default: the script only reports what it WOULD do. Pass --commit to
actually insert. Read-only against the database unless --commit is given.

TSV url defaults to the 20260729 snapshot; override with env
WB_CURATED_POSITIVE_TSV_URL (the /files/pub/ path is public, no credentials).

Run against literature-4005 by loading its env file first, from the repository
root (the directory that contains the agr_literature_service package), e.g.:

    cd /home/azurebrd/git/api_general
    env $(grep -v '^#' agr_literature_service/.env.devserver_4005 | xargs) \
        python agr_literature_service/lit_processing/oneoff_scripts/SCRUM-6130_populate_curation_status_from_caltech_positive.py

(The filename contains a hyphen, so it cannot be run with `python -m`; run the
file path directly. Add --commit to perform the inserts.)
"""

import argparse
import logging
import os
import re
import urllib.request
from datetime import datetime

import pytz

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


def populate(commit=False):
    rows = fetch_tsv_rows()
    db = create_postgres_session(False)
    try:
        wb_mod_id = (
            db.query(ModModel.mod_id)
            .filter(ModModel.abbreviation == MOD_ABBREVIATION)
            .scalar()
        )
        logger.info(f"WB mod_id: {wb_mod_id}")

        ref_map = resolve_references(db, {r["paper"] for r in rows})
        logger.info(f"papers resolved to a reference: {len(ref_map)}")

        # Existing WB curation_status keys (+ their current value, for conflict
        # reporting), so re-runs stay idempotent and existing rows are untouched.
        existing = {}
        for topic, reference_id, status, tag in db.query(
            CurationStatusModel.topic,
            CurationStatusModel.reference_id,
            CurationStatusModel.curation_status,
            CurationStatusModel.curation_tag,
        ).filter(CurationStatusModel.mod_id == wb_mod_id):
            existing[(topic, reference_id)] = (status, tag)

        inserted = 0
        skipped_existing = 0
        not_found = 0
        blank_atp = 0
        blank_atp_rows = []     # (paper, datatype) rows with no ATP topic in the TSV
        conflicts = []          # existing value != our target (curated/curatable)
        seen_new = set()        # dedup identical (topic, ref) rows within the TSV

        for row in rows:
            if not row["atp"]:
                # No ATP topic in the source row (e.g. exprmosaic / geneticmosaic
                # datatypes); never insert a curation_status row with an empty topic.
                blank_atp += 1
                blank_atp_rows.append((row["paper"], row["datatype"]))
                continue
            curie = "WB:WBPaper" + row["paper"]
            if curie not in ref_map:
                not_found += 1
                logger.warning(f"  paper not found in DB: {curie} (atp {row['atp']})")
                continue
            reference_id, agrkb = ref_map[curie]
            key = (row["atp"], reference_id)

            if key in existing:
                skipped_existing += 1
                cur_status, cur_tag = existing[key]
                if (cur_status, cur_tag) != (CURATION_STATUS, CURATION_TAG):
                    conflicts.append(
                        (curie, agrkb, row["datatype"], row["atp"], cur_status, cur_tag)
                    )
                continue
            if key in seen_new:
                continue
            seen_new.add(key)

            if commit:
                db.add(CurationStatusModel(
                    topic=row["atp"],
                    reference_id=reference_id,
                    mod_id=wb_mod_id,
                    curation_status=CURATION_STATUS,
                    curation_tag=CURATION_TAG,
                    note=build_note(row["selcomment"], row["txtcomment"]),
                    created_by=row["curator"] or None,
                    updated_by=row["curator"] or None,
                    date_created=parse_timestamp(row["timestamp"]),
                    date_updated=parse_timestamp(row["timestamp"]),
                ))
                if (inserted + 1) % BATCH_COMMIT_SIZE == 0:
                    db.commit()
                    logger.info(f"  committed {inserted + 1} so far")
            inserted += 1

        if commit:
            db.commit()

        logger.info("")
        logger.info("=== SUMMARY ===")
        logger.info(f"  paper not found in DB:            {not_found}")
        logger.info(f"  skipped (blank ATP topic):        {blank_atp}")
        logger.info(f"  skipped (already have a value):   {skipped_existing}")
        logger.info(f"    of which CONFLICTS (value != curated/curatable): {len(conflicts)}")
        verb = "INSERTED" if commit else "WOULD INSERT"
        logger.info(f"  {verb} (new curated rows):        {inserted}")
        if blank_atp_rows:
            datatypes = sorted({d for _, d in blank_atp_rows})
            logger.info(f"  (blank-ATP datatypes skipped: {', '.join(datatypes)})")
        if conflicts:
            logger.info("")
            logger.info("  conflicts (in TSV as positive, but already valued in DB):")
            logger.info("    %-18s %-22s %-12s %-13s %-13s %s"
                        % ("WBPaper", "AGRKB", "datatype", "atp",
                           "existing_status", "existing_tag"))
            for curie, agrkb, datatype, atp, status, tag in conflicts:
                logger.info("    %-18s %-22s %-12s %-13s %-13s %s"
                            % (curie, agrkb, datatype, atp, status, tag))
        if not commit:
            logger.info("")
            logger.info("  DRY RUN -- no rows written. Re-run with --commit to insert.")
    except Exception as e:
        db.rollback()
        logger.error(f"error during populate, rolled back: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--commit",
        action="store_true",
        help="actually insert curation_status rows (default: dry-run, no writes)",
    )
    args = parser.parse_args()
    populate(commit=args.commit)
