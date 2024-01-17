import logging
from os import path
from agr_literature_service.api.models import CrossReferenceModel
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

obsolete_pmids_file = "data/obsolete_pmid.txt"
batch_commit_size = 250


def make_pmids_obsolete():

    db_session = create_postgres_session(False)

    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)
    record = 0
    with open(obsolete_pmids_file) as f:
        for line in f:
            pmid = 'PMID:' + line.strip()
            x = db_session.query(CrossReferenceModel).filter_by(curie=pmid).one_or_none()
            record += 1
            if x:
                try:
                    x.is_obsolete = True
                    logger.info(f"{record} The XREF for {pmid} is set to obsolete")
                except Exception as e:
                    logger.info(f"{record} An error occurred when setting the XREF for {pmid} to obsolete: {e}")
            if record % batch_commit_size == 0:
                db_session.commit()
    db_session.commit()


if __name__ == "__main__":

    make_pmids_obsolete()
