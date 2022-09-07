from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_engine, \
    create_postgres_session
from agr_literature_service.api.models import ReferenceModel
import logging
import sys

engine = create_postgres_engine(False)
db_connection = engine.connect()

db_session = create_postgres_session(False)

logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# 21 minutes 50 seconds to update 705534 references on literature-4004 compared to 14.5 hours one by one

# SELECT COUNT(*) FROM reference WHERE reference.date_last_modified_in_pubmed IS NOT NULL
# 870002

# SELECT COUNT(*) FROM cross_reference WHERE curie ~ '^PMID:' AND is_obsolete = false AND reference_id IS NOT NULL
# 721305

# 870002 - 721305 = 148697; an extra 106 have PMID but not dlmip, e.g. 21204361 21413240
# 148803 references to remove dlmip


def update_database(db_session, i, reference_id_list):
    try:
        for r in db_session.query(ReferenceModel).filter(ReferenceModel.reference_id.in_(reference_id_list)).all():
            i += 1
            logger.info(f"{i} reference_id {r.reference_id} curie {r.curie} Remove dlmip {r.date_last_modified_in_pubmed}")
            r.date_last_modified_in_pubmed = None
            db_session.add(r)
        db_session.commit()
    except Exception as e:
        logger.info(f"Error occurred when deleting dlmip from {reference_id_list} : {str(e)}")
    return i


hasPmid = set()
rs = db_connection.execute("SELECT reference_id FROM cross_reference WHERE curie ~ '^PMID:' AND is_obsolete = false AND reference_id IS NOT NULL")
rows = rs.fetchall()
for x in rows:
    hasPmid.add(x[0])

rs = db_connection.execute("SELECT reference_id FROM reference WHERE reference.date_last_modified_in_pubmed IS NOT NULL")
rows = rs.fetchall()
batch_size = 500
# to try a smaller subset
# batch_size = 2
reference_id_list = []
i = 0
for x in rows:
    if x[0] in hasPmid:
        continue
    # to try a smaller subset, keep adding for what is left over but don't add everything
    # if len(reference_id_list) >= batch_size + 2:
    #     continue
    reference_id_list.append(x[0])
    # to try a smaller subset
    # if i >= 5:
    #     continue
    if len(reference_id_list) >= batch_size:
        logger.info("Batch")
        logger.info(reference_id_list)
        i = update_database(db_session, i, reference_id_list)
        reference_id_list = []

if len(reference_id_list) > 0:
    logger.info(f"Remaining batch {reference_id_list}")
    i = update_database(db_session, i, reference_id_list)

db_session.close()
db_connection.close()
engine.dispose()
