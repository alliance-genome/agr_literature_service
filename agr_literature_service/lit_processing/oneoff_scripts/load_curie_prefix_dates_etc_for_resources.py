import logging

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def load_data():

    db_session = create_postgres_session(False)

    # total 55466 rows for resources in cross_resource

    limit = 500
    loop_count = 2000

    resource_id_to_date_created_created_by = {}

    i = 0
    for index in range(loop_count):

        offset = index * limit
        rs = db_session.execute("SELECT resource_id, date_created, created_by FROM resource order by resource_id limit " + str(limit) + " offset " + str(offset))
        rows = rs.fetchall()
        if len(rows) == 0:
            break

        for x in rows:
            i += 1
            resource_id_to_date_created_created_by[x[0]] = (x[1], x[2])
            logger.info(str(i) + ": Retrieving resource_id, date_created, created_by from resource.")

    i = 0
    for index in range(loop_count):
        offset = index * limit
        rs = db_session.execute("SELECT curie, resource_id, curie_prefix FROM cross_reference WHERE resource_id is not null order by resource_id limit " + str(limit) + " offset " + str(offset))
        rows = rs.fetchall()
        if len(rows) == 0:
            break
        for x in rows:
            i += 1
            curie = x[0]
            resource_id = x[1]
            if x[2]:
                continue
            curie_prefix = curie.split(":")[0]
            (date_created, created_by) = resource_id_to_date_created_created_by[resource_id]
            db_session.execute("UPDATE cross_reference SET curie_prefix = '" + curie_prefix + "', date_created = '" + str(date_created) + "', date_updated = '" + str(date_created) + "', created_by = '" + created_by + "', updated_by = '" + created_by + "' WHERE curie = '" + curie + "'")
            logger.info(str(i) + " updating cross_reference data for resource_id = " + str(resource_id) + ", curie_prefix = " + curie_prefix)
        db_session.commit()
    db_session.commit()


if __name__ == "__main__":

    load_data()
