import logging

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def add_citations():

    db_session = create_postgres_session(False)
    rows = db_session.execute("SELECT reference_id FROM reference where citation_id is null").fetchall()
    count = 0
    for x in rows:
        count += 1
        ref_id = int(x[0])
        db_session.execute(
            "CALL update_citations(:param)",
            {'param': ref_id}
        )
        if count % 250 == 0:
            print(f"count: {count}")
    db_session.commit()
    db_session.close()


if __name__ == "__main__":

    add_citations()
