import logging

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
# from agr_literature_service.api.models import ReferenceModel

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def update_date_published_start_end():

    db_session = create_postgres_session(False)

    limit = 5000
    loop_count = 200
    row_count = 0
    for index in range(loop_count):
        offset = index * limit
        rows = db_session.execute(f"SELECT reference_id, date_published_start, date_published_end "
                                  f"FROM reference "
                                  f"ORDER BY reference_id limit {limit} "
                                  f"offset {offset}").fetchall()
        if len(rows) == 0:
            break
        for x in rows:
            reference_id = x[0]
            date_published_start = x[1]
            date_published_end = x[2]
            if not date_published_start or len(date_published_start) <= 10:
                continue

            row_count += 1
            try:
                db_session.execute(f"UPDATE reference "
                                   f"set date_published_start = '{date_published_start[0:10]}', "
                                   f"date_published_end = '{date_published_end[0:10]}' "
                                   f"WHERE reference_id = {reference_id}")
                if row_count % 300 == 0:
                    db_session.commit()
                logger.info("UPDATE date_published_start and date_published_end for reference_id = " + str(reference_id))
            except Exception as e:
                logger.info("An error occurred when updatng date_published_start and date_published_end for reference_id = " + str(reference_id) + ". error " + str(e))

    db_session.commit()
    db_session.close()


if __name__ == "__main__":

    update_date_published_start_end()
