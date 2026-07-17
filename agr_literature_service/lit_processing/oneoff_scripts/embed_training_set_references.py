"""SCRUM-6144: Backfill classifier embeddings for a MOD's references.

Generates + registers the classifier embeddings (SCRUM-6142) for a set of
references, reusing the exact per-reference machinery the pdf2md conversion job
already runs, so this is a pure backfill of what conversion would have produced.

Flexible by design (so the same script covers the whole epic):

  --mod FB [WB ...]     one or more MOD abbreviations to process
  --scope training      references in the MODs' topic-classifier training sets
                        (latest dataset version per topic)  [default]
          corpus        every reference in the MODs' corpus

Per reference, two cases (matching the ticket):

  * Already converted (has a final ``converted_merged_main`` Markdown): only
    generate + register embeddings, via
    ``generate_classifier_embeddings_for_reference``. This uploads the parquet
    through ``file_upload_single`` (embedding_file_crud), which has NO
    file-conversion workflow-tag gate, so the upload is allowed and the file
    conversion workflow tag is left untouched.
  * Not yet converted: run the conversion first (nXML preferred, PDFX fallback,
    supplements) via ``process_single_reference`` exactly as pdf2md does — that
    call generates the embeddings itself once the Markdown is persisted. Use
    ``--skip-conversion`` to only embed already-converted references.

Idempotent: a source already embedded for this (profile, version) is skipped,
and an already-converted main is never re-converted, so re-runs never re-spend
OpenAI/PDFX.

SAFETY: the default is a READ-ONLY dry run that enumerates the target
references and reports counts (total, already-converted, needing conversion)
WITHOUT calling OpenAI, PDFX, or S3. Pass ``--commit`` to actually generate.

Usage::

    set -a; source agr_literature_service/.env_prod   # PSQL_*, OPENAI_API_KEY,
    set +a                                            # AWS creds, ENV_STATE, COGNITO_*

    # dry run (default): enumerate + report, no writes / no OpenAI spend
    python -m agr_literature_service.lit_processing.oneoff_scripts.\
embed_training_set_references --mod FB

    # small smoke test against the target DB
    python -m ...embed_training_set_references --mod FB --limit 20 --commit

    # full FB training-set backfill
    python -m ...embed_training_set_references --mod FB --commit

    # later phases (same script):
    python -m ...embed_training_set_references --mod WB --commit
    python -m ...embed_training_set_references --mod FB WB --scope corpus --commit
"""
import argparse
import logging
import re
from datetime import datetime
from os import path
from typing import Dict, List, Optional, Set

from sqlalchemy import text
from sqlalchemy.orm import Session

try:
    from dateutil import parser as _dateutil_parser
    _HAVE_DATEUTIL = True
except ImportError:  # pragma: no cover
    _HAVE_DATEUTIL = False

# Load models before api.user to avoid a circular import via api.user.
from agr_literature_service.api.models import ReferencefileModel  # noqa: F401
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("embed_training_set_references")
logger.setLevel(logging.INFO)

# Topic classifiers are the ml_model rows with this task_type (see
# backfill_ml_model_file_classes.py). Their training data lives in
# dataset rows with dataset_type='document' whose data_type is the topic.
TOPIC_CLASSIFIER_TASK_TYPE = "biocuration_topic_classification"
DOCUMENT_DATASET_TYPE = "document"

# Robust parse of a free-form (PubMed-style) reference date. Mirrors the
# classifier trainer's utils.date_utils.parse_reference_date so --filter-date-before
# selects exactly the same references the trainer keeps: missing components
# resolve to the earliest instant, and an unparseable/empty date returns None
# (which the filter treats as "keep").
_YEAR_RE = re.compile(r"\b(1[5-9]\d{2}|20\d{2}|21\d{2})\b")
_MONTH_RANGE_RE = re.compile(r"(?i)\b([a-z]{3,9})\s*[-/]\s*[a-z]{3,9}\b")


def parse_reference_date(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    s = str(date_str).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s[:10], fmt)
        except ValueError:
            pass
    try:
        return datetime.strptime(s[:7], "%Y-%m")
    except ValueError:
        pass
    match = _YEAR_RE.search(s)
    if not match:
        return None
    year = int(match.group(1))
    if _HAVE_DATEUTIL:
        cleaned = _MONTH_RANGE_RE.sub(r"\1", s)
        try:
            return _dateutil_parser.parse(cleaned, default=datetime(year, 1, 1), fuzzy=True)
        except (ValueError, OverflowError, TypeError):
            return datetime(year, 1, 1)
    return datetime(year, 1, 1)


def get_topic_classifier_topics(db: Session, mod: str, task_type: str) -> List[str]:
    """Distinct topics of ``mod``'s topic-classifier ml_models."""
    rows = db.execute(text("""
        SELECT DISTINCT ml.topic
        FROM ml_model ml
        JOIN mod m ON m.mod_id = ml.mod_id
        WHERE m.abbreviation = :mod
          AND ml.task_type = :task_type
          AND ml.topic IS NOT NULL
        ORDER BY ml.topic
    """), {"mod": mod, "task_type": task_type}).fetchall()
    return [r[0] for r in rows]


def get_latest_document_dataset(db: Session, mod: str, topic: str) -> Optional[Dict]:
    """The highest-version ``document`` dataset for (mod, topic), or None."""
    row = db.execute(text("""
        SELECT d.dataset_id, d.version
        FROM dataset d
        JOIN mod m ON m.mod_id = d.mod_id
        WHERE m.abbreviation = :mod
          AND d.data_type = :topic
          AND d.dataset_type = :dtype
        ORDER BY d.version DESC
        LIMIT 1
    """), {"mod": mod, "topic": topic, "dtype": DOCUMENT_DATASET_TYPE}).fetchone()
    if row is None:
        return None
    return {"dataset_id": int(row[0]), "version": int(row[1])}


def get_training_curies_for_dataset(db: Session, dataset_id: int) -> Set[str]:
    """Distinct training-set reference curies in a dataset."""
    rows = db.execute(text("""
        SELECT DISTINCT reference_curie
        FROM dataset_entry
        WHERE dataset_id = :dataset_id AND set_type = 'training'
    """), {"dataset_id": dataset_id}).fetchall()
    return {r[0] for r in rows}


def enumerate_training_curies(db: Session, mod: str, task_type: str) -> Set[str]:
    """Union of training-set curies across the latest document-dataset version
    of every topic that has a topic-classifier ml_model for ``mod``. Logs the
    per-topic breakdown so a dry run shows exactly what will be covered."""
    curies: Set[str] = set()
    topics = get_topic_classifier_topics(db, mod, task_type)
    if not topics:
        logger.warning("[%s] no '%s' ml_models found", mod, task_type)
        return curies
    logger.info("[%s] %d topic-classifier topic(s): %s", mod, len(topics), ", ".join(topics))
    for topic in topics:
        dataset = get_latest_document_dataset(db, mod, topic)
        if dataset is None:
            logger.warning("[%s] topic %s has no '%s' dataset; skipping",
                           mod, topic, DOCUMENT_DATASET_TYPE)
            continue
        topic_curies = get_training_curies_for_dataset(db, dataset["dataset_id"])
        curies |= topic_curies
        logger.info("[%s]   topic %s: dataset_id=%d v%d training_refs=%d",
                    mod, topic, dataset["dataset_id"], dataset["version"], len(topic_curies))
    return curies


def enumerate_corpus_curies(db: Session, mod: str) -> Set[str]:
    """Every reference curie in ``mod``'s corpus."""
    rows = db.execute(text("""
        SELECT r.curie
        FROM mod_corpus_association mca
        JOIN mod m ON m.mod_id = mca.mod_id
        JOIN reference r ON r.reference_id = mca.reference_id
        WHERE m.abbreviation = :mod AND mca.corpus IS TRUE
    """), {"mod": mod}).fetchall()
    return {r[0] for r in rows}


def map_curies_to_ids(db: Session, curies: List[str]) -> Dict[str, int]:
    """Map reference curie -> reference_id (curies absent from reference are
    simply omitted)."""
    result: Dict[str, int] = {}
    if not curies:
        return result
    rows = db.execute(text("""
        SELECT curie, reference_id FROM reference WHERE curie = ANY(:curies)
    """), {"curies": curies}).fetchall()
    for curie, reference_id in rows:
        result[curie] = int(reference_id)
    return result


def get_reference_dates(db: Session, reference_ids: List[int]) -> Dict[int, Optional[str]]:
    """Best publication-date string per reference: date_published, else
    date_published_start (matching the trainer's field precedence; there is no
    ``year`` column locally, so its year-only fallback is moot)."""
    if not reference_ids:
        return {}
    rows = db.execute(text("""
        SELECT reference_id,
               COALESCE(NULLIF(date_published, ''), NULLIF(date_published_start, ''))
        FROM reference WHERE reference_id = ANY(:ids)
    """), {"ids": reference_ids}).fetchall()
    return {int(r[0]): r[1] for r in rows}


def filter_ids_by_date(db: Session, mod: str, reference_ids: List[int],
                       filter_date_before: str) -> List[int]:
    """Keep references published on/after ``filter_date_before`` (YYYY-MM-DD).
    References with an unparseable/missing date are KEPT, matching the classifier
    trainer's behaviour."""
    threshold = datetime.strptime(filter_date_before, "%Y-%m-%d")
    dates = get_reference_dates(db, reference_ids)
    kept, dropped = [], 0
    for rid in reference_ids:
        parsed = parse_reference_date(dates.get(rid))
        if parsed is None or parsed >= threshold:
            kept.append(rid)
        else:
            dropped += 1
    logger.info("[%s] date filter (< %s): kept %d, dropped %d (unparseable/missing kept)",
                mod, filter_date_before, len(kept), dropped)
    return kept


def get_converted_reference_ids(db: Session, reference_ids: List[int]) -> Set[int]:
    """Subset of ``reference_ids`` that already have a final
    ``converted_merged_main`` Markdown (i.e. the main is already converted)."""
    if not reference_ids:
        return set()
    rows = db.execute(text("""
        SELECT DISTINCT reference_id
        FROM referencefile
        WHERE reference_id = ANY(:ids)
          AND file_class = 'converted_merged_main'
          AND file_extension = 'md'
          AND file_publication_status = 'final'
    """), {"ids": reference_ids}).fetchall()
    return {int(r[0]) for r in rows}


def process_mod(db: Session, mod: str, args: argparse.Namespace,
                explicit_curies: Optional[List[str]] = None) -> Dict[str, int]:
    """Enumerate + (unless dry-run) embed one MOD's references. Returns counts.

    When ``explicit_curies`` is given, enumeration is skipped and exactly those
    references are processed (used by ``--reference-curie`` for single-reference
    smoke tests); ``mod`` is then only the conversion context for refs that still
    need converting."""
    if explicit_curies is not None:
        curie_set = set(explicit_curies)
        logger.info("[%s] processing %d explicit reference curie(s)", mod, len(curie_set))
    elif args.scope == "training":
        curie_set = enumerate_training_curies(db, mod, args.task_type)
    else:
        curie_set = enumerate_corpus_curies(db, mod)

    curies = sorted(curie_set)
    curie_to_id = map_curies_to_ids(db, curies)
    missing = [c for c in curies if c not in curie_to_id]
    if missing:
        logger.warning("[%s] %d enumerated curie(s) not found in reference table (skipped)",
                       mod, len(missing))

    ordered_ids = [curie_to_id[c] for c in curies if c in curie_to_id]
    if args.filter_date_before:
        ordered_ids = filter_ids_by_date(db, mod, ordered_ids, args.filter_date_before)
    if args.limit is not None:
        ordered_ids = ordered_ids[:args.limit]
    id_to_curie = {rid: c for c, rid in curie_to_id.items()}

    converted_ids = get_converted_reference_ids(db, ordered_ids)
    to_embed_only = [rid for rid in ordered_ids if rid in converted_ids]
    to_convert = [rid for rid in ordered_ids if rid not in converted_ids]

    logger.info("[%s] scope=%s target refs=%d | already converted=%d | need conversion=%d%s",
                mod, args.scope, len(ordered_ids), len(to_embed_only), len(to_convert),
                " (will be skipped: --skip-conversion)" if args.skip_conversion else "")

    counts = {"targets": len(ordered_ids), "embedded_ok": 0, "embed_skipped": 0,
              "converted_ok": 0, "skipped_unconverted": 0, "resolve_failed": 0, "failed": 0}

    if not args.commit:
        logger.info("[%s] DRY RUN — no embeddings generated. Re-run with --commit to execute.", mod)
        return counts

    # Lazy imports: only needed for the real run, and keep the embedding stack
    # optional for enumeration/dry-run environments.
    from agr_cognito_py import get_admin_token
    from agr_literature_service.lit_processing.embedding.embedding_generation import (
        generate_classifier_embeddings_for_reference,
    )
    from agr_literature_service.lit_processing.pdf2md.pdf2md import (
        process_single_reference, _resolve_workflow_ref_file_info,
    )

    token: Optional[str] = None
    prefer_nxml = not args.force_pdfx
    process_supplements = not args.no_supplements

    for idx, rid in enumerate(ordered_ids, 1):
        curie = id_to_curie.get(rid, str(rid))
        try:
            if rid in converted_ids:
                result = generate_classifier_embeddings_for_reference(db, rid, curie)
                logger.info("[%s] %d/%d embed %s -> %s",
                            mod, idx, len(ordered_ids), curie, result)
                # A {"skipped": reason} result means nothing was embedded (e.g.
                # no source markdown / not a classifier MOD); don't count it as a
                # success so the summary reflects what actually happened.
                if isinstance(result, dict) and "skipped" in result:
                    counts["embed_skipped"] += 1
                else:
                    counts["embedded_ok"] += 1
            elif args.skip_conversion:
                counts["skipped_unconverted"] += 1
                continue
            else:
                ref_file_info, resolve_error = _resolve_workflow_ref_file_info(
                    db=db, ref_id=rid, reference_curie=curie,
                    mod_abbreviation=mod, prefer_nxml=prefer_nxml,
                )
                if ref_file_info is None:
                    logger.warning("[%s] %d/%d %s: %s; skipping",
                                   mod, idx, len(ordered_ids), curie, resolve_error)
                    counts["resolve_failed"] += 1
                    continue
                if token is None:
                    token = get_admin_token()
                success, error_msg = process_single_reference(
                    db, ref_file_info, token,
                    prefer_nxml=prefer_nxml, process_supplements=process_supplements,
                )
                if success:
                    logger.info("[%s] %d/%d convert+embed %s: ok",
                                mod, idx, len(ordered_ids), curie)
                    counts["converted_ok"] += 1
                else:
                    logger.error("[%s] %d/%d convert %s failed: %s",
                                 mod, idx, len(ordered_ids), curie, error_msg)
                    counts["failed"] += 1
        except Exception as exc:  # never let one reference abort the batch
            logger.error("[%s] %d/%d %s: unexpected error: %s",
                         mod, idx, len(ordered_ids), curie, exc)
            counts["failed"] += 1

    logger.info("[%s] DONE: %s", mod, counts)
    return counts


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mod", nargs="+", required=True, metavar="ABBR",
                        help="MOD abbreviation(s) to process (e.g. FB, or FB WB).")
    parser.add_argument("--scope", choices=["training", "corpus"], default="training",
                        help="'training' = topic-classifier training sets (latest dataset "
                             "version per topic) [default]; 'corpus' = whole MOD corpus.")
    parser.add_argument("--reference-curie", nargs="+", default=None, metavar="CURIE",
                        help="Process only these exact reference curie(s), bypassing "
                             "enumeration (for single-reference smoke tests). Requires a "
                             "single --mod (used as the conversion context).")
    parser.add_argument("--reference-curie-file", default=None, metavar="PATH",
                        help="Process exactly the reference curies listed in this file (one "
                             "curie per line; blank lines and '#' comments ignored), bypassing "
                             "enumeration. For embedding a specific target set (e.g. the "
                             "reclassification targets) without a huge command line. Combines "
                             "with --reference-curie; requires a single --mod.")
    parser.add_argument("--task-type", default=TOPIC_CLASSIFIER_TASK_TYPE,
                        help="ml_model task_type for training scope "
                             f"(default: {TOPIC_CLASSIFIER_TASK_TYPE}).")
    parser.add_argument("--filter-date-before", default=None, metavar="YYYY-MM-DD",
                        help="Drop references published before this date, so the backfill "
                             "matches what the classifier trainer keeps (uses date_published, "
                             "then date_published_start; unparseable/missing dates are kept). "
                             "The latest FB/WB training runs used 2005-01-01.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap the number of references per MOD (for smoke tests).")
    parser.add_argument("--commit", action="store_true",
                        help="Actually generate embeddings. Without it, dry-run (report only).")
    parser.add_argument("--skip-conversion", action="store_true",
                        help="Only embed already-converted references; skip refs needing conversion.")
    parser.add_argument("--no-supplements", action="store_true",
                        help="When converting, do not convert supplemental PDFs.")
    parser.add_argument("--force-pdfx", action="store_true",
                        help="When converting, force PDFX for the main file (disable nXML preference).")
    return parser.parse_args(argv)


def load_explicit_curies(args: argparse.Namespace) -> Optional[List[str]]:
    """Merge --reference-curie and --reference-curie-file into one de-duplicated,
    order-preserving list of curies, or None when neither is given."""
    curies: List[str] = list(args.reference_curie) if args.reference_curie else []
    if args.reference_curie_file:
        with open(args.reference_curie_file) as handle:
            for line in handle:
                line = line.strip()
                if line and not line.startswith("#"):
                    curies.append(line)
    if not curies:
        return None
    seen: Set[str] = set()
    return [c for c in curies if not (c in seen or seen.add(c))]


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    explicit_curies = load_explicit_curies(args)
    if explicit_curies is not None and len(args.mod) != 1:
        raise SystemExit("--reference-curie/--reference-curie-file requires exactly one --mod "
                         "(the conversion context)")
    if args.filter_date_before:
        try:
            datetime.strptime(args.filter_date_before, "%Y-%m-%d")
        except ValueError:
            raise SystemExit(f"--filter-date-before must be YYYY-MM-DD, got {args.filter_date_before!r}")
    db = create_postgres_session(False)
    # Only register the automation user on a real run — set_global_user_id
    # writes a users row, so a dry run stays strictly read-only.
    if args.commit:
        # Fail fast: without an OpenAI key the embedding step silently no-ops
        # for every reference (returns {"skipped": "no_api_key"}), which on a
        # long prod run looks like success while embedding nothing.
        from agr_literature_service.api.config import config as _config
        if not _config.OPENAI_API_KEY:
            raise SystemExit(
                "OPENAI_API_KEY is not set in the environment — embedding generation "
                "would no-op for every reference. Add OPENAI_API_KEY to your sourced "
                "env file (e.g. .env.rdsprod) and re-run.")
        set_global_user_id(db, path.basename(__file__).replace(".py", ""))
    try:
        if explicit_curies is not None:
            process_mod(db, args.mod[0], args, explicit_curies=explicit_curies)
            return
        if args.scope == "corpus" and args.commit:
            logger.warning("Corpus scope on --commit can embed a very large number of "
                           "references (OpenAI + PDFX cost). Proceeding.")
        grand: Dict[str, int] = {}
        for mod in args.mod:
            counts = process_mod(db, mod, args)
            for key, value in counts.items():
                grand[key] = grand.get(key, 0) + value
        if len(args.mod) > 1:
            logger.info("TOTAL across %s: %s", ",".join(args.mod), grand)
    finally:
        db.close()


if __name__ == "__main__":
    main()
