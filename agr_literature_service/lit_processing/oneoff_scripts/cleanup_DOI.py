from agr_literature_service.api.models import CrossReferenceModel
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
import logging

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

db_session = create_postgres_session(False)

### checked these missing prefix ones are all DOIs
for x in db_session.query(CrossReferenceModel).filter(CrossReferenceModel.curie.notlike('%:%')).all():
    try:
        y = db_session.query(CrossReferenceModel).filter_by(curie='DOI:' + x.curie).one_or_none()
        if y:
            log.info("reference_id=" + str(x.reference_id) + ": Duplicate DOI: " + "bad one: " + x.curie + " good one: " + y.curie + " deleting bad one")
            db_session.delete(x)
        else:
            log.info("reference_id=" + str(x.reference_id) + ": missing prefix DOI: " + x.curie + " adding prefix to this DOI")
            x.curie = 'DOI:' + x.curie
            db_session.add(x)
    except Exception as e:
        log.info("An error occurred when updating DOI for reference_id = " + str(x.reference_id) + " error = " + str(e))

db_session.commit()
db_session.close()
