import logging

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ReferenceModel

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def cleanup_duplicate_refs():

    db_session = create_postgres_session(False)

    # copied from jira: SCRUM-3061
    references_to_delete = ['AGRKB:101000000963954',
                            'AGRKB:101000000965605',
                            'AGRKB:101000000963830',
                            'AGRKB:101000000964483',
                            'AGRKB:101000000964485',
                            'AGRKB:101000000965018',
                            'AGRKB:101000000965026',
                            'AGRKB:101000000965028',
                            'AGRKB:101000000965347',
                            'AGRKB:101000000965353',
                            'AGRKB:101000000962166',
                            'AGRKB:101000000962579',
                            'AGRKB:101000000963058',
                            'AGRKB:101000000963477',
                            'AGRKB:101000000962072',
                            'AGRKB:101000000962788']

    for refCurie in references_to_delete:
        x = db_session.query(ReferenceModel).filter_by(curie=refCurie).one_or_none()
        if x:
            try:
                db_session.delete(x)
                logger.info(f"Deleting reference: {refCurie}")
            except Exception as e:
                logger.info(f"An error occurred when deleting reference: {refCurie}. Error={e}")

    db_session.commit()
    # db_session.rollback()
    db_session.close()


if __name__ == "__main__":

    cleanup_duplicate_refs()
