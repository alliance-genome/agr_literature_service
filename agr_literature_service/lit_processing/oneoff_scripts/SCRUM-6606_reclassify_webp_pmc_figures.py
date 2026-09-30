#!/usr/bin/env python3
"""
Reclassify PMC webp images that were loaded as 'supplement' (SCRUM-6606).

The PMC Cloud Service article datasets (which replaced the FTP/OA packages in
August 2026) ship figures as .webp instead of the old .jpg master + .gif
thumbnail pairs. classify_pmc_file() did not treat webp as an image extension,
so every webp figure loaded by load_pmc_metadata since the switch landed as
file_class 'supplement'. Those references therefore have image_count = 0
(compute_reference_image_count only counts '%figure%' classes) and show no
images or figure legends, which is what ZFIN reported.

This script re-runs the (now webp-aware) classification for those rows and
updates file_class accordingly - almost always to 'figure'; the name-based
rules still apply, so publisher-labelled thumbnails ('thumb'), inline images
('_ILM<n>') and Taylor & Francis print-B&W duplicates ('_PB' with an '_OC'
twin) get their usual classes. webp has no size-based thumbnail rule (the new
datasets ship one webp per figure, no thumbnail siblings), so file size is not
needed.

Only rows created by load_pmc_metadata are touched: a webp supplement uploaded
by a curator or another loader is not necessarily a figure. Rows whose
file_class was manually edited after load (updated_by set and different from
the loader) are reported and skipped - a curator decision stands.

Updates run through SQL on the live tables, so the existing
referencefile_image_count_update_trigger refreshes reference.image_count
per row; no separate recompute step is needed.

    python SCRUM-6606_reclassify_webp_pmc_figures.py          # dry run
    python SCRUM-6606_reclassify_webp_pmc_figures.py --apply
"""

import argparse
import logging
from collections import Counter
from os import path

from agr_literature_service.api.models import ReferencefileModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    classify_pmc_file
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SCRIPT_USER = path.basename(__file__).replace(".py", "")

LOADER_USER = "load_pmc_metadata"
BATCH_COMMIT_SIZE = 250


def get_sibling_display_names(db, reference_id):
    """Display names of the other loader-created files in the same reference,
    approximating the PMC package the webp arrived in (needed for the
    _PB/_OC color-twin rule)."""
    rows = (db.query(ReferencefileModel.display_name)
            .filter_by(reference_id=reference_id, created_by=LOADER_USER).all())
    return {row.display_name for row in rows}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true",
                        help="commit the reclassification (default is dry run)")
    args = parser.parse_args()

    db = create_postgres_session(False)
    set_global_user_id(db, SCRIPT_USER)
    new_class_counts: Counter = Counter()
    affected_references = set()
    skipped_edited = 0
    since_commit = 0
    try:
        rows = (db.query(ReferencefileModel)
                .filter_by(file_extension="webp", file_class="supplement",
                           created_by=LOADER_USER)
                .order_by(ReferencefileModel.reference_id,
                          ReferencefileModel.referencefile_id).all())
        logger.info(f"{len(rows)} webp supplement row(s) created by {LOADER_USER}")

        siblings_by_reference = {}
        for row in rows:
            if row.updated_by and row.updated_by != LOADER_USER:
                skipped_edited += 1
                logger.info(f"skip referencefile {row.referencefile_id} "
                            f"'{row.display_name}': last edited by {row.updated_by}")
                continue
            if row.reference_id not in siblings_by_reference:
                siblings_by_reference[row.reference_id] = \
                    get_sibling_display_names(db, row.reference_id)
            new_class = classify_pmc_file(
                row.display_name, row.file_extension,
                sibling_display_names=siblings_by_reference[row.reference_id])
            if new_class == "supplement":
                continue
            new_class_counts[new_class] += 1
            affected_references.add(row.reference_id)
            logger.info(f"referencefile {row.referencefile_id} "
                        f"(reference_id {row.reference_id}) '{row.display_name}': "
                        f"supplement -> {new_class}")
            if args.apply:
                row.file_class = new_class
                since_commit += 1
                if since_commit >= BATCH_COMMIT_SIZE:
                    db.commit()
                    since_commit = 0

        reclassified = sum(new_class_counts.values())
        by_class = ", ".join(f"{cls}={n}" for cls, n in sorted(new_class_counts.items()))
        logger.info(f"summary: {reclassified} file(s) reclassified ({by_class or 'none'}) "
                    f"across {len(affected_references)} reference(s); "
                    f"{skipped_edited} skipped as manually edited")
        if args.apply:
            db.commit()
            logger.info("[APPLY] committed")
        else:
            db.rollback()
            logger.info("[DRY RUN] nothing committed")
    finally:
        db.close()


if __name__ == "__main__":
    main()
