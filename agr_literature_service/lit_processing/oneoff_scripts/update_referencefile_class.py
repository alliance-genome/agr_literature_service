import logging
from os import path
from sqlalchemy.exc import SQLAlchemyError

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import classify_pmc_file
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

BATCH_COMMIT_SIZE = 200
LIMIT = 500
LOOP_COUNT = 7000


def update_file_class():

    # Update the file_class column in the referencefile table based on the
    # file_name and file_extension.

    db_session = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, script_name)

    for index in range(LOOP_COUNT):
        offset = index * LIMIT
        logger.info(f"offset={offset}")

        try:
            rows = db_session.execute(f"SELECT referencefile_id, display_name, file_extension "
                                      f"FROM referencefile "
                                      f"WHERE file_class = 'supplement' "
                                      f"ORDER BY referencefile_id "
                                      f"LIMIT {LIMIT} "
                                      f"OFFSET {offset}").fetchall()
        except SQLAlchemyError as e:
            logger.info(f"Error executing SQL query: {e}")
            continue

        if not rows:
            continue

        for row in rows:
            referencefile_id, file_name, file_extension = row
            file_class = classify_pmc_file(file_name, file_extension)

            if file_class != 'supplement':
                try:
                    db_session.execute(f"UPDATE referencefile "
                                       f"SET file_class = '{file_class}' "
                                       f"WHERE referencefile_id = {referencefile_id}")
                    logger.info(f"SET file_class to '{file_class}' for display_name = '{file_name}' and file_extension = '{file_extension}'")
                except SQLAlchemyError as e:
                    logger.info(f"Error updating file_class: {e}")

            if not (referencefile_id + 1) % BATCH_COMMIT_SIZE:
                try:
                    db_session.commit()
                except SQLAlchemyError as e:
                    logger.info(f"Error committing changes: {e}")
                    db_session.rollback()

        try:
            db_session.commit()
        except SQLAlchemyError as e:
            logger.info(f"Error committing changes: {e}")
            db_session.rollback()

    logger.info("DONE!")


if __name__ == "__main__":
    update_file_class()
