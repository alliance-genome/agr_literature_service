from agr_literature_service.api.models import CrossReferenceModel
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
import logging

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

db = create_postgres_session(False)

pmids = [
    "PMID:34521819",
    "PMID:24513209",
    "PMID:33453855",
    "PMID:37613209",
    "PMID:37595893",
    "PMID:38519694",
    "PMID:37613119",
    "PMID:38332095",
    "PMID:38838192",
    "PMID:39221967",
    "PMID:37602391",
    "PMID:38377345",
    "PMID:39412685",
    "PMID:38230577",
    "PMID:36534425",
    "PMID:38442891",
    "PMID:39315487",
    "PMID:37613349",
    "PMID:38109637",
    "PMID:35107606",
    "PMID:30874362",
    "PMID:36917933"
]

for pmid in pmids:
    x = db.query(CrossReferenceModel).filter_by(curie=pmid).one_or_none()
    if x:
        x.is_obsolete = True
        db.add(x)
        logger.info(f"Setting {pmid} to obsolete")

db.commit()
db.close()
