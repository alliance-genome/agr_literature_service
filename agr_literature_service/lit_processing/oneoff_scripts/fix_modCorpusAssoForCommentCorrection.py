from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ModCorpusAssociationModel
import logging

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

infile = "data/mca_update.lst"


def fix_data():

    db_session = create_postgres_session(False)

    f = open(infile)

    i = 0
    for line in f:
        i += 1
        pieces = line.strip().split(' ')
        reference_id = int(pieces[0])
        mod_id = int(pieces[1])
        x = db_session.query(ModCorpusAssociationModel).filter_by(reference_id=reference_id, mod_id=mod_id).one_or_none()
        if x:
            x.mod_corpus_sort_source = "automated_alliance"
            db_session.add(x)
            log.info("Update mod_corpus_sort_source to 'automated_alliance' for reference_id = " + str(reference_id) + " and mod_id = " + str(mod_id))
        if i > 250:
            # db_session.rollback()
            db_session.commit()
            i = 0
    # db_session.rollback()
    db_session.commit()
    db_session.close()


if __name__ == "__main__":

    fix_data()
