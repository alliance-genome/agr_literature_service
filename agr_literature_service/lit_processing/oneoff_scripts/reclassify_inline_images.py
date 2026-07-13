"""
One-off cleanup for SCRUM-6095: re-classify publisher inline images.

Some publishers (notably Taylor & Francis) ship inline equation / formatted-text
snippets as small images named ``<article>_ILM<n>`` (e.g. KRNB_A_2685379_ILM0001).
These are neither figures nor thumbnails of figures, so historically they were
mislabeled ``thumbnail`` (they are small enough to fall under the size cutoff).
They now get their own ``inline_image`` file_class, matching the ingest-time
classifier (file_processing_utils.is_inline_image).

The match is anchored to the ``_ILM<digits>`` suffix so it does not catch author
names that merely contain "ilm" (Yilmaz, Egilmez, Tilmann, ...).

By default the script is a dry run: it reports what it would change but commits
nothing. Pass --apply to perform the updates.

Usage:
    python reclassify_inline_images.py             # dry run
    python reclassify_inline_images.py --apply     # perform updates
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

# Image rows whose display name is an anchored inline-image suffix and that are
# currently mislabeled as a figure or thumbnail.
SELECT_CANDIDATES = text(
    "SELECT referencefile_id, display_name, file_extension, file_class "
    "FROM referencefile "
    "WHERE file_class IN ('thumbnail', 'figure') "
    "AND lower(file_extension) IN ('jpg', 'jpeg', 'gif', 'png', 'tif', 'tiff') "
    "AND display_name ~* '_ILM[0-9]+$' "
    "ORDER BY referencefile_id"
)

UPDATE_CLASS = text(
    "UPDATE referencefile "
    "SET file_class = 'inline_image' "
    "WHERE referencefile_id = :referencefile_id"
)


def reclassify_inline_images(apply_changes: bool) -> None:

    db_session = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, script_name)

    mode = "APPLY" if apply_changes else "DRY-RUN"
    logger.info(f"Running in {mode} mode")

    try:
        rows = db_session.execute(SELECT_CANDIDATES).fetchall()
    except SQLAlchemyError as e:
        logger.error(f"Error executing SELECT query: {e}")
        return

    updated = 0
    for referencefile_id, display_name, file_extension, file_class in rows:
        logger.info(f"{file_class} -> inline_image: referencefile_id={referencefile_id} "
                    f"display_name='{display_name}.{file_extension}'")
        if apply_changes:
            # Commit per row so a single failure rolls back only that row and
            # ``updated`` counts committed work exactly.
            try:
                db_session.execute(UPDATE_CLASS, {"referencefile_id": referencefile_id})
                db_session.commit()
                updated += 1
            except SQLAlchemyError as e:
                logger.error(f"Error updating referencefile_id={referencefile_id}: {e}")
                db_session.rollback()

    logger.info(
        f"DONE! mode={mode} matched={len(rows)} "
        f"{'updated' if apply_changes else 'to_update'}={updated if apply_changes else len(rows)}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reclassify publisher inline images ('..._ILM<n>') as 'inline_image'."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Commit the updates. Without this flag the script only reports.",
    )
    args = parser.parse_args()
    reclassify_inline_images(args.apply)
