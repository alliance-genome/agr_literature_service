"""
Set every reference's Alliance category from its MOD reference types,
reference relations and PubMed types (alliance_category_utils).

The PubMed update replaced the category with one from the PubMed types alone,
so papers a MOD typed as a review (and categorised as one at the 2021 load)
became research articles again, and WB comments and letters stayed personal
communications. This script recomputes the category of every reference the
way the loaders now do and changes it where a MOD reference type or a
relation decides it (a change PubMed alone makes is left to the PubMed
update), writing each change to a report:

    curie, old category, new category, MOD reference types, relations, PubMed types

Obsolete references and categories a curator set by hand are left alone. The
changes are made through the ORM as this script, so they are in the history.

    python backfill_alliance_category_from_mod_reference_types.py            # dry run, report only
    python backfill_alliance_category_from_mod_reference_types.py --apply
"""
import argparse
import csv
import logging
from collections import Counter
from os import environ, path
from typing import Dict, List, Tuple

from sqlalchemy import text

from agr_literature_service.api.models import ReferenceModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.utils.alliance_category_utils import \
    RELATION_CATEGORY, category_name, load_category_inputs, mod_reference_type_category, \
    pubmed_types_category, resolve_category
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SCRIPT_USER = path.basename(__file__).replace(".py", "")
BATCH_SIZE = 5000


def find_changes(db, reference_rows) -> List[Tuple]:
    """(reference_id, curie, old, new, inputs, pubmed_types) of each reference
    whose category should change because of its MOD reference types or
    relations. A change PubMed alone makes is left to the PubMed update."""
    inputs = load_category_inputs(db, [row[0] for row in reference_rows])
    pubmed_category_cache: Dict[Tuple[str, ...], object] = {}
    changes = []
    for reference_id, curie, category, pubmed_types in reference_rows:
        key = tuple(pubmed_types or [])
        if key not in pubmed_category_cache:
            pubmed_category_cache[key] = pubmed_types_category(list(key))
        old = category_name(category)
        new = resolve_category(inputs[reference_id], pubmed_category_cache[key], old)
        decided_by = {mod_reference_type_category(inputs[reference_id].mod_reference_types,
                                                  in_pubmed=pubmed_category_cache[key] is not None)}
        decided_by |= {RELATION_CATEGORY.get(relation) for relation in inputs[reference_id].relation_types}
        if new and new != old and new in decided_by:
            changes.append((reference_id, curie, old, new, inputs[reference_id], pubmed_types))
    return changes


def backfill(db, apply: bool, report_file: str, batch_size: int = BATCH_SIZE) -> Counter:
    """Recompute every reference's category; {(old, new): count}."""
    counts: Counter = Counter()
    last_id = 0
    with open(report_file, "w", newline="") as fh:
        report = csv.writer(fh, delimiter="\t")
        report.writerow(["curie", "old_category", "new_category", "mod_reference_types", "relations",
                         "pubmed_types"])
        while True:
            rows = db.execute(text(
                "SELECT id, curie, category::text, pubmed_types FROM reference "
                "WHERE id > :last ORDER BY id LIMIT :n"), {"last": last_id, "n": batch_size}).all()
            if not rows:
                break
            last_id = rows[-1][0]
            changes = find_changes(db, rows)
            for reference_id, curie, old, new, inputs, pubmed_types in changes:
                counts[(old, new)] += 1
                report.writerow([curie, old, new,
                                 "; ".join(f"{mod}:{label}" for mod, label in sorted(inputs.mod_reference_types)),
                                 "; ".join(sorted(inputs.relation_types)), "; ".join(pubmed_types or [])])
                if apply:
                    db.get(ReferenceModel, reference_id).category = new
            if apply:
                db.commit()
            else:
                db.rollback()
            logger.info(f"through reference id {last_id}: {sum(counts.values())} change(s)")
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="update the categories (default is dry run)")
    parser.add_argument("--report", default=None,
                        help="report file (default: alliance_category_backfill.tsv under LOG_PATH or here)")
    args = parser.parse_args()
    report_file = args.report or path.join(environ.get("LOG_PATH", "."), "alliance_category_backfill.tsv")

    db = create_postgres_session(False)
    set_global_user_id(db, SCRIPT_USER)
    try:
        counts = backfill(db, args.apply, report_file)
    finally:
        db.close()
    logger.info(("[APPLY]" if args.apply else "[DRY RUN]") + f" {sum(counts.values())} reference(s) "
                f"{'changed' if args.apply else 'would change'}; report: {report_file}")
    for (old, new), count in counts.most_common():
        logger.info(f"  {old} -> {new}: {count}")


if __name__ == "__main__":
    main()
