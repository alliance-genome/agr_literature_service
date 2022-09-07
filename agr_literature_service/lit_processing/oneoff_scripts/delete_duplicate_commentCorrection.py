from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_engine, \
    create_postgres_session
from agr_literature_service.api.models import ReferenceCommentAndCorrectionModel
import logging

engine = create_postgres_engine(False)
db_connection = engine.connect()

db_session = create_postgres_session(False)

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)


foundRow = {}

rs = db_connection.execute("SELECT reference_comment_and_correction_id, reference_id_from, reference_id_to, reference_comment_and_correction_type FROM reference_comments_and_corrections order by reference_comment_and_correction_id")

rows = rs.fetchall()

for x in rows:
    key = (x[1], x[2], x[3])
    reference_comment_and_correction_id = x[0]
    if key in foundRow:
        log.info(str(x[0]) + " duplicate row: " + str(key))
        # log.info(str(foundRow[key]) + " duplicate row: " + str(key) + " to save")
        try:
            x = db_session.query(ReferenceCommentAndCorrectionModel).filter_by(reference_comment_and_correction_id=reference_comment_and_correction_id).one_or_none()
            if x:
                db_session.delete(x)
        except Exception as e:
            log.info("Error occurred when deleting " + str(reference_comment_and_correction_id) + " " + str(key) + str(e))
    else:
        foundRow[key] = x[0]

db_session.commit()
db_session.close()
db_connection.close()
engine.dispose()
