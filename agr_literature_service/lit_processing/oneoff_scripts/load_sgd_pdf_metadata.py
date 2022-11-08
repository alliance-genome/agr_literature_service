import logging
from os import path
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ReferencefileModel, ReferencefileModAssociationModel
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

infile = "data/pdf_metadata.txt"

file_class = "main"
file_publication_status = "final"
file_extension = "pdf"
pdf_type = "pdf"
batch_commit_size = 250


def load_data():

    db_session = create_postgres_session(False)

    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)

    rs = db_session.execute("SELECT md5sum FROM referencefile")
    rows = rs.fetchall()
    loaded = {}
    for x in rows:
        loaded[x[0]] = 1

    rs = db_session.execute("SELECT mod_id FROM mod WHERE abbreviation = 'SGD'")
    rows = rs.fetchall()
    mod_id = rows[0][0]

    f = open(infile)

    i = 0
    for line in f:
        pieces = line.strip().split("\t")
        # PMID[tab]reference_id[tab]md5sum
        display_name = pieces[0]
        md5sum = pieces[2]
        if md5sum in loaded:
            continue
        reference_id = int(pieces[1])
        i += 1

        if i % batch_commit_size == 0:
            db_session.commit()

        referencefile_id = insert_referencefile(db_session, display_name,
                                                reference_id, md5sum)
        if referencefile_id:
            insert_referencefile_mod(db_session, display_name,
                                     referencefile_id, mod_id)
        loaded[md5sum] = 1

    f.close()
    db_session.commit()
    db_session.close()


def insert_referencefile_mod(db_session, display_name, referencefile_id, mod_id):

    try:
        x = ReferencefileModAssociationModel(mod_id=mod_id,
                                             referencefile_id=referencefile_id)
        db_session.add(x)
        logger.info("PMID:" + display_name + ": data loaded into Referencefile_mod table")
    except Exception as e:
        logger.info("PMID:" + display_name + ": error loading data into Referencefile_mod table: " + str(e))


def insert_referencefile(db_session, display_name, reference_id, md5sum):

    referencefile_id = None

    try:
        x = ReferencefileModel(display_name=display_name,
                               reference_id=reference_id,
                               md5sum=md5sum,
                               file_class=file_class,
                               file_publication_status=file_publication_status,
                               file_extension=file_extension,
                               pdf_type=pdf_type,
                               is_annotation=False)
        db_session.add(x)
        db_session.flush()
        db_session.refresh(x)
        referencefile_id = x.referencefile_id
        logger.info("PMID:" + display_name + ": main PDF metadata loaded into Referencefile table")
    except Exception as e:
        logger.info("PMID:" + display_name + ": main PDF metadata loading error: " + str(e))

    return referencefile_id


if __name__ == "__main__":

    load_data()
