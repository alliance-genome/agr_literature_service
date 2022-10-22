import logging

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def load_data():

    db_session = create_postgres_session(False)

    # total 2,865,097 rows in cross_reference

    limit = 500
    loop_count = 200000

    reference_id_to_date_created_created_by = {}

    i = 0
    for index in range(loop_count):

        offset = index * limit
        rs = db_session.execute("SELECT reference_id, date_created, created_by FROM reference order by reference_id limit " + str(limit) + " offset " + str(offset))
        rows = rs.fetchall()
        if len(rows) == 0:
            break

        for x in rows:
            i += 1
            reference_id_to_date_created_created_by[x[0]] = (x[1], x[2])
            logger.info(str(i) + ": Retrieving reference_id, date_created, created_by from reference.")

    i = 0
    for index in range(loop_count):
        offset = index * limit
        rs = db_session.execute("SELECT curie, reference_id, curie_prefix FROM cross_reference WHERE reference_id is not null order by reference_id limit " + str(limit) + " offset " + str(offset))
        rows = rs.fetchall()
        if len(rows) == 0:
            break
        for x in rows:
            i += 1
            curie = x[0]
            reference_id = x[1]
            if x[2]:
                continue
            curie_prefix = curie.split(":")[0]
            (date_created, created_by) = reference_id_to_date_created_created_by[reference_id]
            db_session.execute("UPDATE cross_reference SET curie_prefix = '" + curie_prefix + "', date_created = '" + str(date_created) + "', date_updated = '" + str(date_created) + "', created_by = '" + created_by + "', updated_by = '" + created_by + "' WHERE curie = '" + curie + "'")
            logger.info(str(i) + ": upadting cross_reference for reference_id = " + str(reference_id) + ", curie_prefix = " + curie_prefix)

        db_session.commit()
    db_session.commit()


if __name__ == "__main__":

    load_data()
