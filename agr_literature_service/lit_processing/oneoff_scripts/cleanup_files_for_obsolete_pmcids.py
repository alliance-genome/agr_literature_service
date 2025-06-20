import logging

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ReferencefileModel, CrossReferenceModel
from agr_literature_service.api.crud.referencefile_utils import remove_from_s3_and_db

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def cleanup_referencefiles():

    db = create_postgres_session(False)

    reffileObjs = db.query(ReferencefileModel).join(
        CrossReferenceModel,
        CrossReferenceModel.reference_id == ReferencefileModel.reference_id
    ).filter(
        CrossReferenceModel.is_obsolete.is_(True),
        CrossReferenceModel.curie_prefix == "PMCID"
    ).all()
    try:
        for reffile_obj in reffileObjs:
            logging.info(f"Deleting {reffile_obj.display_name} for reference_id = {reffile_obj.reference_id}")
            remove_from_s3_and_db(db, reffile_obj)
    except Exception as e:
        logging.error(f"An error occurred when deleting files for obsolete PMCIDs: error={e}")


if __name__ == "__main__":

    cleanup_referencefiles()
