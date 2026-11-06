"""SCRUM-6670: move FB's "no genetic data" predictions from manual_indexing_tag to topic entity tags.

Until SCRUM-6669 the FB "no genetic data" classifier (job topic ATP:0000207) wrote one
manual_indexing_tag row per paper: curation_tag ATP:0000207 with the model's probability of "no
genetic data" as confidence_score. It now writes a "genetic data" (ATP:0000402) topic tag for every
paper instead, and this script turns each old row into the tag the classifier would write today:

  * score >= 0.5 ("no genetic data"): a negated ATP:0000402 tag, confidence level LOW / MEDIUM /
    HIGH on the classifier's bands (get_confidence_level: 0.667, 0.833);
  * score < 0.5 ("has genetic data"): a non-negated ATP:0000402 tag with confidence level NEG, the
    convention the classifier uses for every negated model's negative calls.

Every tag keeps the row's score and date_created, so the weekly FB export (which only reads tags
created in the last 6 days) does not send them again, and carries the ml_model 60 metadata
(species NCBITaxon:7214, data novelty ATP:0000335, data context ATP:0000405) with source
abc_document_classifier. A reference that already has a classifier ATP:0000402 tag (written by the
SCRUM-6669 classifier) is skipped: that tag is newer. Tags are created with
validate_on_insert=False and each touched reference is revalidated once afterwards.

With --delete-source the FB ATP:0000207 rows are then removed from manual_indexing_tag (the table
stays: other MODs use it) -- only rows whose reference now has a classifier ATP:0000402 tag, and only
after every row has been written to a CSV backup.

Usage (the default is a read-only dry run)::

    python3 agr_literature_service/lit_processing/oneoff_scripts/backfill_fb_no_genetic_data_tets.py
    python3 .../backfill_fb_no_genetic_data_tets.py --commit --limit 20
    python3 .../backfill_fb_no_genetic_data_tets.py --commit
    python3 .../backfill_fb_no_genetic_data_tets.py --delete-source --backup-file /path/rows.csv
"""
import argparse
import csv
import logging
from collections import Counter
from datetime import datetime
from os import path
from typing import Dict, List, Optional, Set

from sqlalchemy import text
from sqlalchemy.orm import Session

# Load models before api.user to avoid a circular import via api.user.
from agr_literature_service.api.models import TopicEntityTagModel  # noqa: F401
from agr_literature_service.api.crud.topic_entity_tag_crud import create_tag, revalidate_all_tags
from agr_literature_service.api.schemas.topic_entity_tag_schemas import TopicEntityTagSchemaPost
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_fb_no_genetic_data_tets")
logger.setLevel(logging.INFO)

MOD_ABBREVIATION = "FB"
NO_GENETIC_DATA_CURATION_TAG = "ATP:0000207"
GENETIC_DATA_TOPIC = "ATP:0000402"
SOURCE_METHOD = "abc_document_classifier"
# ml_model 60's metadata (the production "no genetic data" model since 2026-07-29)
SPECIES = "NCBITaxon:7214"
DATA_NOVELTY = "ATP:0000335"
DATA_CONTEXT = "ATP:0000405"
CREATED_BY = "default_user"
# Rows from before ml_model 60 was created came from version 2 (ml_model 46)
ML_MODEL_V3_ID = 60
ML_MODEL_V2_ID = 46
ML_MODEL_V3_CREATED = datetime(2026, 7, 29)
# Classifier thresholds: a call of "no genetic data" from 0.5, then get_confidence_level's bands
POSITIVE_THRESHOLD = 0.5
LOW_BELOW = 0.667
MEDIUM_BELOW = 0.833

BACKUP_COLUMNS = ["manual_indexing_tag_id", "reference_id", "reference_curie", "confidence_score",
                  "validation_by_biocurator", "note", "date_created", "date_updated"]


def confidence_level(score: float) -> str:
    """The level the classifier gives this score (agr_document_classifier.get_confidence_level)."""
    if score < POSITIVE_THRESHOLD:
        return "NEG"
    if score < LOW_BELOW:
        return "LOW"
    if score < MEDIUM_BELOW:
        return "MEDIUM"
    return "HIGH"


def ml_model_id_for(date_created: datetime) -> int:
    return ML_MODEL_V3_ID if date_created >= ML_MODEL_V3_CREATED else ML_MODEL_V2_ID


def build_payload(row: Dict, source_id: int) -> TopicEntityTagSchemaPost:
    """The ATP:0000402 tag the classifier would write today for this manual_indexing_tag row."""
    score = row["confidence_score"]
    return TopicEntityTagSchemaPost(
        reference_curie=row["reference_curie"],
        topic=GENETIC_DATA_TOPIC,
        species=SPECIES,
        negated=score >= POSITIVE_THRESHOLD,
        data_novelty=DATA_NOVELTY,
        data_context=DATA_CONTEXT,
        confidence_score=score,
        confidence_level=confidence_level(score),
        tag_source_id=source_id,
        ml_model_id=ml_model_id_for(row["date_created"]),
        created_by=CREATED_BY,
        updated_by=CREATED_BY,
        date_created=row["date_created"],
        date_updated=row["date_updated"] or row["date_created"],
        force_insertion=True,
    )


def load_rows(db: Session) -> List[Dict]:
    rows = db.execute(text("""
        SELECT mit.id, mit.reference_id, r.curie, mit.confidence_score, mit.validation_by_biocurator,
               mit.note, mit.date_created, mit.date_updated
        FROM manual_indexing_tag mit
        JOIN mod m ON m.id = mit.mod_id
        JOIN reference r ON r.id = mit.reference_id
        WHERE m.abbreviation = :mod AND mit.curation_tag = :tag
        ORDER BY mit.id
    """), {"mod": MOD_ABBREVIATION, "tag": NO_GENETIC_DATA_CURATION_TAG}).fetchall()
    return [dict(zip(BACKUP_COLUMNS, row)) for row in rows]


def find_source_id(db: Session) -> int:
    ids = [row[0] for row in db.execute(text("""
        SELECT s.id FROM tag_source s JOIN mod m ON m.id = s.secondary_data_provider_id
        WHERE s.source_method = :method AND m.abbreviation = :mod
    """), {"method": SOURCE_METHOD, "mod": MOD_ABBREVIATION}).fetchall()]
    if len(ids) != 1:
        raise SystemExit(f"Expected one {MOD_ABBREVIATION} {SOURCE_METHOD} tag_source, found {ids}")
    return ids[0]


def references_with_classifier_tag(db: Session, source_id: int) -> Set[int]:
    return {row[0] for row in db.execute(text("""
        SELECT DISTINCT reference_id FROM topic_entity_tag
        WHERE topic = :topic AND tag_source_id = :source_id
    """), {"topic": GENETIC_DATA_TOPIC, "source_id": source_id}).fetchall()}


def plan(rows: List[Dict], already_tagged: Set[int]) -> Dict[str, List[Dict]]:
    """Split the rows into what the backfill does with them."""
    todo: Dict[str, List[Dict]] = {"create": [], "skip_tagged": [], "skip_no_score": []}
    for row in rows:
        if row["reference_id"] in already_tagged:
            todo["skip_tagged"].append(row)
        elif row["confidence_score"] is None:
            todo["skip_no_score"].append(row)
        else:
            todo["create"].append(row)
    return todo


def summarize(rows: List[Dict]) -> Counter:
    return Counter(("negated" if r["confidence_score"] >= POSITIVE_THRESHOLD else "non-negated")
                   + " " + confidence_level(r["confidence_score"]) for r in rows)


def write_backup(rows: List[Dict], backup_file: str) -> None:
    with open(backup_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=BACKUP_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Backed up %d rows to %s", len(rows), backup_file)


def create_tags(db: Session, rows: List[Dict], source_id: int) -> Dict[str, int]:
    counts = {"created": 0, "failed": 0, "revalidation_failed": 0}
    touched: List[int] = []
    for n, row in enumerate(rows, 1):
        try:
            create_tag(db, build_payload(row, source_id), validate_on_insert=False)
            counts["created"] += 1
            touched.append(row["reference_id"])
        except Exception as e:  # one row must not abort the rest
            db.rollback()
            counts["failed"] += 1
            logger.error("%s: tag not created: %s", row["reference_curie"], e)
        if n % 500 == 0:
            logger.info("  ...%d/%d", n, len(rows))
    logger.info("Revalidating %d references", len(touched))
    for reference_id in touched:
        try:
            revalidate_all_tags(curie_or_reference_id=str(reference_id), db=db)
        except Exception as e:  # best-effort, as in cleanup_zfin_over_cap_reference_tags
            db.rollback()
            counts["revalidation_failed"] += 1
            logger.warning("Revalidation failed for reference_id=%d: %s", reference_id, e)
    return counts


def delete_source_rows(db: Session, rows: List[Dict], tagged: Set[int]) -> int:
    ids = [row["manual_indexing_tag_id"] for row in rows if row["reference_id"] in tagged]
    if not ids:
        return 0
    deleted = db.execute(text("DELETE FROM manual_indexing_tag WHERE id = ANY(:ids)"),
                         {"ids": ids}).rowcount
    db.commit()
    return deleted


def run(commit: bool, delete_source: bool, limit: Optional[int], backup_file: str) -> Dict[str, int]:
    db = create_postgres_session(False)
    try:
        rows = load_rows(db)
        source_id = find_source_id(db)
        todo = plan(rows, references_with_classifier_tag(db, source_id))
        if limit is not None:
            todo["create"] = todo["create"][:limit]
        logger.info("%d FB %s manual_indexing_tag rows; tag_source_id %d", len(rows),
                    NO_GENETIC_DATA_CURATION_TAG, source_id)
        logger.info("  to create: %d %s", len(todo["create"]), dict(summarize(todo["create"])))
        logger.info("  skipped, reference already has a classifier %s tag: %d",
                    GENETIC_DATA_TOPIC, len(todo["skip_tagged"]))
        logger.info("  skipped, no score: %d", len(todo["skip_no_score"]))
        counts = {"rows": len(rows), "to_create": len(todo["create"]),
                  "skipped_tagged": len(todo["skip_tagged"]),
                  "skipped_no_score": len(todo["skip_no_score"])}
        if not commit and not delete_source:
            logger.info("DRY RUN - nothing written. Re-run with --commit, then --delete-source.")
            return counts

        set_global_user_id(db, path.basename(__file__).replace(".py", ""))
        if commit:
            counts.update(create_tags(db, todo["create"], source_id))
        if delete_source:
            write_backup(rows, backup_file)
            counts["deleted"] = delete_source_rows(db, rows, references_with_classifier_tag(db, source_id))
            counts["left_in_table"] = len(rows) - counts["deleted"]
        logger.info("DONE: %s", counts)
        return counts
    finally:
        db.close()


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--commit", action="store_true", help="create the topic entity tags")
    parser.add_argument("--delete-source", action="store_true",
                        help="back up, then delete the FB ATP:0000207 manual_indexing_tag rows whose "
                             "reference has a classifier ATP:0000402 tag")
    parser.add_argument("--limit", type=int, default=None, help="create at most this many tags")
    parser.add_argument("--backup-file", default=f"SCRUM-6670_manual_indexing_tag_FB_{datetime.now():%Y%m%d_%H%M%S}.csv",
                        help="CSV the rows are written to before --delete-source")
    args = parser.parse_args(argv)
    run(args.commit, args.delete_source, args.limit, args.backup_file)


if __name__ == "__main__":
    main()
