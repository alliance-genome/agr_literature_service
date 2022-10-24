import logging

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def delete_orcid():

    db_session = create_postgres_session(False)

    # total 2,980,494 rows in cross_reference
    # total   115,397 ORCID in cross_referenc

    limit = 500
    loop_count = 2000

    orcid_list = []

    for index in range(loop_count):
        offset = index * limit
        rs = db_session.execute("SELECT curie FROM cross_reference WHERE curie like 'ORCID:%%' order by curie limit " + str(limit) + " offset " + str(offset))
        rows = rs.fetchall()
        if len(rows) == 0:
            break
        for x in rows:
            orcid = x[0]
            orcid_list.append(x[0])

    i = 0
    for orcid in orcid_list:
        i += 1
        db_session.execute("DELETE FROM cross_reference WHERE curie = '" + orcid + "'")
        logger.info(str(i) + ": Deleting " + orcid + " from cross_reference table.")
        if i % 500 == 0:
            db_session.commit()
            # db_session.rollback()

    db_session.commit()
    # db_session.rollback()


if __name__ == "__main__":

    delete_orcid()
