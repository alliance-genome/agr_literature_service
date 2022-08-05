from agr_literature_service.lit_processing.helper_sqlalchemy import create_postgres_engine, \
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


# SELECT COUNT(*) FROM reference WHERE reference.date_last_modified_in_pubmed IS NOT NULL
# 870002

# SELECT COUNT(*) FROM cross_reference WHERE curie ~ '^PMID:' AND is_obsolete = false AND reference_id IS NOT NULL
# 721305

# 870002 - 721305 = 148697; an extra 106 have PMID but not dlmip, e.g. 21204361 21413240
# 148803 references to remove dlmip


hasPmid = set()
rs = db_connection.execute("SELECT reference_id FROM cross_reference WHERE curie ~ '^PMID:' AND is_obsolete = false AND reference_id IS NOT NULL")
rows = rs.fetchall()
for x in rows:
    hasPmid.add(x[0])

try_count = 0
remove_count = 0
rs = db_connection.execute("SELECT reference_id FROM reference WHERE reference.date_last_modified_in_pubmed IS NOT NULL")
rows = rs.fetchall()
for x in rows:
    if x[0] not in hasPmid:
        try_count = try_count + 1
        try:
            y = db_session.query(ReferenceModel).filter_by(reference_id=x[0]).one_or_none()
            if y.date_last_modified_in_pubmed:
                remove_count = remove_count + 1
                logger.info(f"Remove dlmip {y.date_last_modified_in_pubmed} from reference_id {x[0]}")
                y.date_last_modified_in_pubmed = None
                db_session.add(y)
                if remove_count % 100 == 0:
                    db_session.commit()
                if remove_count % 1000 == 0:
                    db_connection.close()
# this might help ?
#                     engine.dispose()
#                     engine = create_postgres_engine(False)
                    db_connection = engine.connect()

        except Exception as e:
            logger.info("Error occurred when deleting dlmip from " + str(x[0]) + " " + str(e))
#         if try_count > 1000:
#             break

logger.info(str(try_count) + " references to try to remove dlmip")
logger.info(str(try_count) + " references removed dlmip")


db_session.commit()
db_session.close()
db_connection.close()
engine.dispose()
