"""SCRUM-5781: Copy classifier embeddings from another ABC environment (e.g. prod)
into THIS one (e.g. dev), WITHOUT re-computing them (no OpenAI spend).

For each target FB reference, download the already-generated ``embedding`` parquet
for the classifier profile from ``--source-api-server`` (prod) via the ABC REST
API, then register it in THIS environment via
``embedding_file_crud.create_or_update``. That writes the ``referencefile`` +
``embedding_file`` rows here and stores the parquet in THIS env's S3 folder
(md5/dedup; access inherited from the source markdown). The S3 bucket is shared
across environments but each env uses its own folder, so the bytes are written
into this env's folder; content-addressing keeps re-runs idempotent.

Targets (``--scope``):

  * ``reclassification`` (FB): references carrying any FB per-data-type
    "classification complete" workflow tag (ATP:0000167 / ATP:0000244 /
    ATP:0000307) or a "no genetic data" manual-indexing tag (ATP:0000207).
  * ``training`` (SCRUM-6143, WB): the MOD's topic-classifier training sets (latest
    dataset version per topic), optionally date-filtered with
    ``--filter-date-before`` -- the same set ``embed_training_set_references``
    embeds on prod, so the two runs line up.

The source markdown (``converted_merged_main``) must already
exist HERE (it does for a dev restored from prod); references missing it, or
missing the embedding on the source env, are skipped and reported.

SAFETY: the default is a READ-ONLY dry run (enumerate + report the dev-side plan,
no source downloads, no writes). Pass ``--commit`` to actually copy.

Usage::

    # source THIS env (dev): PSQL_* (dev DB), AWS_* (shared bucket), COGNITO_*
    set -a; source agr_literature_service/.env.rdsdev; set +a

    # dry run (default): enumerate + report counts, no downloads / no writes
    python -m agr_literature_service.lit_processing.oneoff_scripts.\
copy_classifier_embeddings_from_env --mod FB

    # copy for real (downloads from prod, writes here)
    python -m ...copy_classifier_embeddings_from_env --mod FB --commit

    # WB training sets, after embed_training_set_references ran on prod (SCRUM-6143)
    python -m ...copy_classifier_embeddings_from_env --mod WB --scope training \
        --filter-date-before 2005-01-01 --commit
"""
import argparse
import logging
from io import BytesIO
from typing import List, Optional, Set, Tuple

import requests
from sqlalchemy import text
from sqlalchemy.orm import Session
from starlette.datastructures import UploadFile

# Load models before api.user to avoid a circular import via api.user.
from agr_literature_service.api.models import ReferencefileModel, ReferenceModel  # noqa: F401
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.api.crud import embedding_file_crud
from agr_literature_service.api.schemas.embedding_file_schemas import EmbeddingFileSchemaCreate
from agr_literature_service.lit_processing.oneoff_scripts.embed_training_set_references import (
    TOPIC_CLASSIFIER_TASK_TYPE, enumerate_training_curies, filter_ids_by_date, map_curies_to_ids)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("copy_classifier_embeddings_from_env")
logger.setLevel(logging.INFO)

PROFILE = "classifier_fulltext_paragraph_chunk_refs_excluded_md_cleaned"
VERSION = 1
MODEL_NAME = "openai:text-embedding-3-small"
SOURCE_FILE_CLASS = "converted_merged_main"
DEFAULT_SOURCE_API = "https://literature-rest.alliancegenome.org"

# FB reference-classification "complete" workflow tags (their union = every
# previously classified FB reference) + the "no genetic data" manual-indexing tag.
FB_COMPLETE_TAGS = ("ATP:0000167", "ATP:0000244", "ATP:0000307")
NO_GEN_DATA_TAG = "ATP:0000207"


def reclassification_target_ids(db: Session, mod: str) -> Set[int]:
    """Reference ids carrying a "classification complete" workflow tag or the
    "no genetic data" manual-indexing tag for ``mod`` (the FB reclassification set)."""
    rows = db.execute(text("""
        WITH m AS (SELECT id FROM mod WHERE abbreviation = :mod)
        SELECT reference_id FROM workflow_tag
          WHERE mod_id = (SELECT id FROM m) AND workflow_tag_id = ANY(:tags)
        UNION
        SELECT reference_id FROM manual_indexing_tag
          WHERE mod_id = (SELECT id FROM m) AND curation_tag = :ngd
    """), {"mod": mod, "tags": list(FB_COMPLETE_TAGS), "ngd": NO_GEN_DATA_TAG}).fetchall()
    return {int(r[0]) for r in rows}


def training_target_ids(db: Session, mod: str, filter_date_before: Optional[str]) -> Set[int]:
    """Reference ids in ``mod``'s topic-classifier training sets, enumerated and
    date-filtered exactly as ``embed_training_set_references`` does."""
    curies = sorted(enumerate_training_curies(db, mod, TOPIC_CLASSIFIER_TASK_TYPE))
    ids = list(map_curies_to_ids(db, curies).values())
    if filter_date_before:
        ids = filter_ids_by_date(db, mod, ids, filter_date_before)
    return set(ids)


def enumerate_plan(db: Session, reference_ids: Set[int]) -> List[Tuple[str, int, Optional[int], bool]]:
    """One set-based query returning, per target reference:
    (curie, reference_id, local converted_merged_main source referencefile id or None,
    already-has-a-local-embedding-for-this-profile). Avoids per-reference round trips."""
    if not reference_ids:
        return []
    rows = db.execute(text("""
        SELECT r.curie, r.id,
               (SELECT rf.id FROM referencefile rf
                  WHERE rf.reference_id = r.id AND rf.file_class = :src_class
                  LIMIT 1) AS source_id,
               EXISTS (SELECT 1 FROM embedding_file e
                         WHERE e.reference_id = r.id
                           AND e.profile_name = :prof AND e.version = :ver) AS already
        FROM reference r
        WHERE r.id = ANY(:ids)
        ORDER BY r.curie
    """), {"ids": sorted(reference_ids), "src_class": SOURCE_FILE_CLASS,
           "prof": PROFILE, "ver": VERSION}).fetchall()
    return [(r[0], r[1], r[2], bool(r[3])) for r in rows]


def find_source_embedding_referencefile_id(source_api: str, curie: str, headers: dict) -> Optional[int]:
    resp = requests.get(f"{source_api}/reference/referencefile/show_all/{curie}",
                        headers=headers, timeout=60)
    if resp.status_code != 200:
        return None
    for rf in resp.json():
        if (rf.get("file_class") == "embedding" and rf.get("profile_name") == PROFILE
                and rf.get("version") == VERSION):
            src = rf.get("source") or {}
            if src.get("file_class") == SOURCE_FILE_CLASS:
                return rf.get("referencefile_id")
    return None


def download_source_parquet(source_api: str, referencefile_id: int, headers: dict) -> bytes:
    resp = requests.get(f"{source_api}/reference/referencefile/download_file/{referencefile_id}",
                        headers=headers, timeout=180)
    resp.raise_for_status()
    return resp.content


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mod", default="FB", help="MOD abbreviation (default: FB).")
    parser.add_argument("--source-api-server", default=DEFAULT_SOURCE_API,
                        help=f"ABC REST API to copy embeddings FROM (default: {DEFAULT_SOURCE_API}).")
    parser.add_argument("--scope", choices=["reclassification", "training"], default="reclassification",
                        help="'reclassification' = the FB reclassification set [default]; 'training' = "
                             "the MOD's topic-classifier training sets (SCRUM-6143).")
    parser.add_argument("--filter-date-before", default=None, metavar="YYYY-MM-DD",
                        help="With --scope training: drop references published before this date, "
                             "as embed_training_set_references --filter-date-before does.")
    parser.add_argument("--limit", type=int, default=None, help="Cap number of references.")
    parser.add_argument("--commit", action="store_true",
                        help="Actually download + register. Without it, dry-run (report only).")
    return parser.parse_args(argv)


def _auth_headers() -> dict:
    from agr_cognito_py import get_admin_token
    return {"Authorization": f"Bearer {get_admin_token()}"}


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    db = create_postgres_session(False)
    if args.commit:
        set_global_user_id(db, "copy_classifier_embeddings_from_env")

    if args.scope == "training":
        target_ids = training_target_ids(db, args.mod, args.filter_date_before)
    else:
        target_ids = reclassification_target_ids(db, args.mod)
    plan = enumerate_plan(db, target_ids)
    if args.limit is not None:
        plan = plan[:args.limit]
    logger.info("[%s] %d target references (scope=%s, source=%s)", args.mod, len(plan), args.scope,
                args.source_api_server)

    headers = None
    if args.commit:
        headers = _auth_headers()
        # Fail fast if the source env / auth is wrong: the first target with a
        # local source must resolve an embedding on the source, otherwise every
        # ref would "skip".
        probe = next((curie for curie, _rid, source_id, _done in plan if source_id is not None), None)
        if probe is not None and find_source_embedding_referencefile_id(
                args.source_api_server, probe, headers) is None:
            raise SystemExit(f"Could not find a source embedding for {probe} on "
                             f"{args.source_api_server} (wrong source env, auth, or profile?). Aborting.")

    counts = {"targets": len(plan), "copied": 0, "already": 0,
              "no_local_source": 0, "no_source_embedding": 0, "failed": 0}
    for i, (curie, _reference_id, source_id, is_copied) in enumerate(plan, 1):
        if source_id is None:
            counts["no_local_source"] += 1
            continue
        if is_copied:
            counts["already"] += 1
            continue
        if not args.commit:
            counts["copied"] += 1   # would-copy (dev-side plan; source coverage confirmed separately)
            continue
        src_rf_id = find_source_embedding_referencefile_id(args.source_api_server, curie, headers)
        if src_rf_id is None:
            counts["no_source_embedding"] += 1
            continue
        try:
            parquet = download_source_parquet(args.source_api_server, src_rf_id, headers)
            if not parquet:
                counts["failed"] += 1
                continue
            request = EmbeddingFileSchemaCreate(
                reference_curie=curie, profile_name=PROFILE, version=VERSION,
                model_name=MODEL_NAME, source_referencefile_id=source_id)
            upload = UploadFile(filename=f"embedding_{PROFILE}_v{VERSION}.parquet",
                                file=BytesIO(parquet))
            embedding_file_crud.create_or_update(db, request, upload)
            counts["copied"] += 1
        except Exception as exc:  # noqa: BLE001 - one bad ref must not kill the run
            logger.error("Failed to copy %s: %s", curie, exc)
            db.rollback()
            counts["failed"] += 1
        if i % 200 == 0:
            logger.info("[%s] %d/%d processed: %s", args.mod, i, len(plan), counts)

    verb = "COPIED" if args.commit else "DRY RUN (would copy)"
    logger.info("[%s] DONE %s: %s", args.mod, verb, counts)


if __name__ == "__main__":
    main()
