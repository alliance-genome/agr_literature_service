import logging
import requests
from os import environ

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_cognito_auth import (
    get_authentication_token,
    generate_headers
)

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def update_curies():

    db_session = create_postgres_session(False)

    limit = 500
    loop_count = 2000

    for index in range(loop_count):

        offset = index * limit
        rs = db_session.execute("SELECT resource_id, curie FROM resource order by resource_id limit " + str(limit) + " offset " + str(offset))
        rows = rs.fetchall()
        if len(rows) == 0:
            break

        curie_count = len(rows)
        (curr_curie, counter) = get_first_curie(curie_count)

        for x in rows:
            resource_id = x[0]
            old_curie = x[1]
            new_curie = curr_curie

            db_session.execute("UPDATE resource set curie = '" + new_curie + "' WHERE resource_id = " + str(resource_id))

            curr_curie = get_next_curie(curr_curie, counter)
            counter += 1
            logger.info(str(resource_id) + " " + old_curie + " " + new_curie)

        db_session.commit()

    db_session.close()


def get_next_curie(curie, counter):

    curie_len = len(curie)
    counter_len = len(str(counter))
    next_curie = curie[0:curie_len - counter_len] + str(counter + 1)
    return next_curie


def get_first_curie(curie_count):

    token = get_authentication_token()
    headers = generate_headers(token)
    headers['subdomain'] = 'resource'

    url = environ['ID_MATI_URL']

    headers['value'] = str(curie_count)
    res = requests.post(url, headers=headers)
    res_json = res.json()
    return (res_json['first']['curie'], res_json['first']['counter'])


if __name__ == "__main__":

    update_curies()
