from agr_literature_service.lit_processing.helper_sqlalchemy import create_postgres_engine, \
    create_postgres_session
from agr_literature_service.api.models import ModReferenceTypeModel
import logging

engine = create_postgres_engine(False)
db_connection = engine.connect()

db_session = create_postgres_session(False)

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

limit = 500
loop_count = 10000

foundType = {}

i = 0
j = 0

for index in range(loop_count):

    offset = index * limit

    rs = db_connection.execute('SELECT mod_reference_type_id, reference_id, reference_type, source FROM mod_reference_type order by reference_id limit ' + str(limit) + ' offset ' + str(offset))

    rows = rs.fetchall()

    if len(rows) == 0:
        break

    for x in rows:
        i = i + 1
        key = (x[1], x[2], x[3])
        mod_reference_type_id = x[0]
        if key in foundType:
            j += 1
            if j % 250 == 0:
                db_session.commit()
            log.info(str(i) + " " + str(j) + " " + str(key))
            try:
                x = db_session.query(ModReferenceTypeModel).filter_by(mod_reference_type_id=mod_reference_type_id).one_or_none()
                if x:
                    db_session.delete(x)
            except Exception as e:
                log.info("Error occurred when deleting " + str(mod_reference_type_id) + " " + str(key) + str(e))
        else:
            foundType[key] = 1

db_session.commit()
db_session.close()
db_connection.close()
engine.dispose()
