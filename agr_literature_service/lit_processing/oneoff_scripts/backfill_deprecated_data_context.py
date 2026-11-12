"""Move every topic_entity_tag off the deprecated data context ATP:0000325 (SCRUM-6815).

The 2026-10-30 ATP release deprecated ATP:0000325 "experimentally studied data" with no
parent and no replacement. Each MOD chose its replacement on SCRUM-6815, and the code
(DATA_CONTEXT_DEFAULTS in topic_entity_tag_crud, the UI and the pipelines) now writes
them; this moves the tags already stored, with the same table:

    MOD          topic-only tag   tag with an entity
    WB, ZFIN     ATP:0000405      ATP:0000406   experimental data / experimentally studied entity
    every other  ATP:0000405      ATP:0000405   experimental data (FB, SGD, MGI, RGD, XB, AGR)

The MOD is the tag source's secondary data provider (required on every source), as in
the defaults.

--include-wb-topic-root also moves WB topic-only tags on the ATP:0000323 root to
ATP:0000405. Kimberly Van Auken confirmed WB topic tags take ATP:0000405 from now on;
whether the 1.25M existing ones move too was still open on SCRUM-6815, so it is off by
default.

Like the FB part of this ticket (2026-11-04) and the SCRUM-5697 backfill, this is batched
raw SQL: no topic_entity_tag_version rows and date_updated is unchanged, since only the
term's id changes, not what the tag says. Every changed id is appended to --id-log
(id,old,new) as it is committed, for an undo. The updates stream through Debezium.

Validation edges depend on data_context, so run the full revalidation sweep afterwards
(GET /topic_entity_tag/revalidate_all_tags/ with delete_all_validation_values_first).

Usage (the default is a read-only dry run)::

    python3 agr_literature_service/lit_processing/oneoff_scripts/backfill_deprecated_data_context.py
    python3 .../backfill_deprecated_data_context.py --commit --limit 1000
    python3 .../backfill_deprecated_data_context.py --commit [--include-wb-topic-root]
"""
import argparse
import csv
import logging
import time
from datetime import datetime
from typing import Dict, List, NamedTuple, Optional

from sqlalchemy import text

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEPRECATED = "ATP:0000325"
DATA_CONTEXT_ROOT = "ATP:0000323"
EXPERIMENTAL_DATA = "ATP:0000405"
EXPERIMENTALLY_STUDIED_ENTITY = "ATP:0000406"
ENTITY_SPLIT_MODS = ("WB", "ZFIN")

HAS_ENTITY = "(tet.entity IS NOT NULL AND tet.entity <> '')"
NO_ENTITY = "(tet.entity IS NULL OR tet.entity = '')"
SPLIT_MOD = "m.abbreviation IN ({})".format(", ".join(f"'{a}'" for a in ENTITY_SPLIT_MODS))

BATCH_SIZE = 5000


class Rule(NamedTuple):
    name: str
    old: str
    new: str
    where: str


RULES: List[Rule] = [
    Rule("WB/ZFIN tags with an entity", DEPRECATED, EXPERIMENTALLY_STUDIED_ENTITY,
         f"{SPLIT_MOD} AND {HAS_ENTITY}"),
    Rule("every other tag", DEPRECATED, EXPERIMENTAL_DATA,
         f"NOT ({SPLIT_MOD} AND {HAS_ENTITY})"),
]
WB_TOPIC_ROOT_RULE = Rule("WB topic-only tags on the root", DATA_CONTEXT_ROOT, EXPERIMENTAL_DATA,
                          f"m.abbreviation = 'WB' AND {NO_ENTITY}")

_FROM = """
    FROM topic_entity_tag tet
    JOIN tag_source ts ON ts.id = tet.tag_source_id
    JOIN mod m ON m.id = ts.secondary_data_provider_id
"""


def rules_for(include_wb_topic_root: bool) -> List[Rule]:
    return RULES + ([WB_TOPIC_ROOT_RULE] if include_wb_topic_root else [])


def counts_by_mod(db, data_context: str) -> Dict[str, Dict[str, int]]:
    """{MOD: {"topic": n, "entity": n}} for the tags on data_context."""
    rows = db.execute(text(f"""
        SELECT m.abbreviation, {HAS_ENTITY}, count(*)
        {_FROM}
        WHERE tet.data_context = :dc
        GROUP BY 1, 2 ORDER BY 1, 2
    """), {"dc": data_context}).fetchall()
    counts: Dict[str, Dict[str, int]] = {}
    for abbreviation, has_entity, count in rows:
        counts.setdefault(abbreviation, {"topic": 0, "entity": 0})["entity" if has_entity else "topic"] = count
    return counts


def report(db, data_context: str) -> int:
    counts = counts_by_mod(db, data_context)
    total = 0
    for abbreviation, kinds in counts.items():
        logger.info("    %-6s topic-only %9d   with entity %9d", abbreviation, kinds["topic"], kinds["entity"])
        total += kinds["topic"] + kinds["entity"]
    logger.info("    total %d tags on %s", total, data_context)
    return total


def count_rule(db, rule: Rule) -> int:
    return db.execute(text(f"SELECT count(*) {_FROM} WHERE tet.data_context = :old AND ({rule.where})"),
                      {"old": rule.old}).scalar() or 0


def apply_rule(db, rule: Rule, writer, batch_size: int = BATCH_SIZE,
               limit: Optional[int] = None, sleep: float = 0.0) -> int:
    """Move the rule's tags in committed batches, logging each id. Returns how many moved."""
    moved = 0
    while limit is None or moved < limit:
        size = batch_size if limit is None else min(batch_size, limit - moved)
        ids = [row[0] for row in db.execute(text(f"""
            UPDATE topic_entity_tag t SET data_context = :new
            WHERE t.id IN (SELECT tet.id {_FROM}
                           WHERE tet.data_context = :old AND ({rule.where})
                           LIMIT :size)
            RETURNING t.id
        """), {"new": rule.new, "old": rule.old, "size": size}).fetchall()]
        if not ids:
            break
        db.commit()
        for tag_id in ids:
            writer.writerow([tag_id, rule.old, rule.new])
        moved += len(ids)
        logger.info("    %s: %d moved", rule.name, moved)
        if sleep:
            time.sleep(sleep)
    return moved


def run(commit: bool, include_wb_topic_root: bool, id_log: str, batch_size: int = BATCH_SIZE,
        limit: Optional[int] = None, sleep: float = 0.0) -> Dict[str, int]:
    db = create_postgres_session(False)
    try:
        logger.info("Tags on the deprecated %s, by MOD:", DEPRECATED)
        report(db, DEPRECATED)
        rules = rules_for(include_wb_topic_root)
        if include_wb_topic_root:
            logger.info("WB tags on the %s root:", DATA_CONTEXT_ROOT)
            report(db, DATA_CONTEXT_ROOT)
        planned = {rule.name: count_rule(db, rule) for rule in rules}
        for rule in rules:
            logger.info("  %s: %d %s -> %s", rule.name, planned[rule.name], rule.old, rule.new)
        if not commit:
            logger.info("DRY RUN - nothing written. Re-run with --commit.")
            return planned

        moved: Dict[str, int] = {}
        remaining = limit
        with open(id_log, "a", newline="") as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["topic_entity_tag_id", "old_data_context", "new_data_context"])
            for rule in rules:
                if remaining is not None and remaining <= 0:
                    break
                moved[rule.name] = apply_rule(db, rule, writer, batch_size, remaining, sleep)
                if remaining is not None:
                    remaining -= moved[rule.name]
                f.flush()
        logger.info("Changed ids are in %s", id_log)
        logger.info("Left on %s:", DEPRECATED)
        moved["left_on_deprecated"] = report(db, DEPRECATED)
        logger.info("DONE: %s", moved)
        logger.info("Next: run the full revalidation sweep (revalidate_all_tags, "
                    "delete_all_validation_values_first) so validation edges follow the new values.")
        return moved
    finally:
        db.close()


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--commit", action="store_true", help="write the changes (default: dry run)")
    parser.add_argument("--include-wb-topic-root", action="store_true",
                        help=f"also move WB topic-only tags on {DATA_CONTEXT_ROOT} to {EXPERIMENTAL_DATA}")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="tags per committed batch")
    parser.add_argument("--limit", type=int, default=None, help="move at most this many tags in total")
    parser.add_argument("--sleep", type=float, default=0.0, help="seconds to pause between batches")
    parser.add_argument("--id-log", default=f"SCRUM-6815_data_context_ids_{datetime.now():%Y%m%d_%H%M%S}.csv",
                        help="CSV every changed id is appended to (id,old,new), for an undo")
    args = parser.parse_args(argv)
    run(args.commit, args.include_wb_topic_root, args.id_log, args.batch_size, args.limit, args.sleep)


if __name__ == "__main__":
    main()
