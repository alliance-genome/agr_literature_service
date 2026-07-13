"""
One-off cleanup for SCRUM-6095: re-classify Taylor & Francis color/B&W renditions.

Taylor & Francis ship each figure as two renditions: an online-color image
("<figure>_OC") and a print black-and-white image ("<figure>_PB"). Both are
full-size, so the absolute size rule (SCRUM-6281) split them arbitrarily --
hiding ~half of the primary color figures as ``thumbnail`` and leaving
duplicate B&W images in the figure view.

The classifier (file_processing_utils.classify_pmc_file) now identifies these
renditions by name BEFORE the size rules, so classification is size-independent.
This script brings existing data in line by name (no S3 access needed):

  1. ``_OC`` rows mislabeled ``thumbnail`` -> ``figure``   (rescue color figures)
  2. ``_PB`` rows that have an ``_OC`` twin -> ``bw_duplicate``  (demote duplicate)
  3. ``_PB`` rows with NO ``_OC`` twin, currently ``thumbnail`` -> ``figure``
     (it is the sole rendition of the figure)

The ``bw_duplicate`` class does not match ``%figure%``, so those rows drop out
of ``compute_reference_image_count`` -- the curator's figure view then shows
only the color rendition.

By default the script is a dry run: it reports counts but commits nothing.
Pass --apply to perform the updates.

Usage:
    python reclassify_oc_pb_renditions.py            # dry run
    python reclassify_oc_pb_renditions.py --apply    # perform updates
"""
import argparse
import logging
from os import path

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

IMAGE_EXT_FILTER = "lower(file_extension) IN ('jpg', 'jpeg', 'gif', 'tif', 'tiff', 'png')"

# A _PB row has a color twin when the same reference contains the corresponding
# _OC display name (case-insensitive).
HAS_OC_TWIN = (
    "EXISTS (SELECT 1 FROM referencefile oc "
    "        WHERE oc.reference_id = rf.reference_id "
    "          AND lower(oc.display_name) = lower(regexp_replace(rf.display_name, '_PB$', '_OC', 'i')))"
)

# (label, count SQL, update SQL) for each transition. rf is the target row.
TRANSITIONS = [
    (
        "_OC thumbnail -> figure (rescue color)",
        f"SELECT count(*) FROM referencefile rf WHERE rf.file_class = 'thumbnail' "
        f"AND rf.display_name ~* '_OC$' AND {IMAGE_EXT_FILTER}",
        f"UPDATE referencefile rf SET file_class = 'figure' WHERE rf.file_class = 'thumbnail' "
        f"AND rf.display_name ~* '_OC$' AND {IMAGE_EXT_FILTER}",
    ),
    (
        "_PB with _OC twin -> bw_duplicate",
        f"SELECT count(*) FROM referencefile rf WHERE rf.file_class IN ('figure', 'thumbnail') "
        f"AND rf.display_name ~* '_PB$' AND {IMAGE_EXT_FILTER} AND {HAS_OC_TWIN}",
        f"UPDATE referencefile rf SET file_class = 'bw_duplicate' "
        f"WHERE rf.file_class IN ('figure', 'thumbnail') "
        f"AND rf.display_name ~* '_PB$' AND {IMAGE_EXT_FILTER} AND {HAS_OC_TWIN}",
    ),
    (
        "_PB without _OC twin, thumbnail -> figure (sole rendition)",
        f"SELECT count(*) FROM referencefile rf WHERE rf.file_class = 'thumbnail' "
        f"AND rf.display_name ~* '_PB$' AND {IMAGE_EXT_FILTER} AND NOT {HAS_OC_TWIN}",
        f"UPDATE referencefile rf SET file_class = 'figure' WHERE rf.file_class = 'thumbnail' "
        f"AND rf.display_name ~* '_PB$' AND {IMAGE_EXT_FILTER} AND NOT {HAS_OC_TWIN}",
    ),
]


def reclassify_oc_pb(apply_changes: bool) -> None:

    db_session = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, script_name)

    mode = "APPLY" if apply_changes else "DRY-RUN"
    logger.info(f"Running in {mode} mode")

    total = 0
    for label, count_sql, update_sql in TRANSITIONS:
        try:
            n = db_session.execute(text(count_sql)).scalar() or 0
            logger.info(f"{label}: {n}")
            if apply_changes and n:
                db_session.execute(text(update_sql))
                db_session.commit()
            total += n
        except SQLAlchemyError as e:
            logger.error(f"Error on '{label}': {e}")
            db_session.rollback()

    logger.info(f"DONE! mode={mode} total_rows={total}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reclassify Taylor & Francis _OC/_PB color/B&W figure renditions."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Commit the updates. Without this flag the script only reports counts.",
    )
    args = parser.parse_args()
    reclassify_oc_pb(args.apply)
