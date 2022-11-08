import logging
from os import path
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ReferencefileModel, ReferencefileModAssociationModel
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

infile = "data/PMC_OA_metadata.txt"

file_class = "supplement"
file_publication_status = "final"
batch_commit_size = 250


def load_data():

    db_session = create_postgres_session(False)

    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)

    rs = db_session.execute("SELECT referencefile_id, reference_id, md5sum FROM referencefile")
    rows = rs.fetchall()
    referencefile_loaded = {}
    for x in rows:
        referencefile_loaded[x[2]] = (x[0], x[1])

    rs = db_session.execute("SELECT referencefile_id FROM referencefile_mod WHERE mod_id is null")
    rows = rs.fetchall()
    referencefile_mod_loaded = {}
    for x in rows:
        referencefile_mod_loaded[x[0]] = 1

    f = open(infile)

    i = 0
    for line in f:
        pieces = line.strip().split("\t")
        # PMID[tab]PMCID[tab]reference_id[tab]md5sum[tab]file_name_with_suffix
        pmid = pieces[0]
        reference_id = int(pieces[2])
        md5sum = pieces[3]
        file_name_with_suffix = pieces[4]
        referencefile_id = None
        if md5sum in referencefile_loaded:
            (this_referencefile_id, this_reference_id) = referencefile_loaded[md5sum]
            if this_reference_id != reference_id:
                logger.info("PMC_OA file for PMID:" + pmid + ": " + file_name_with_suffix + " matches a file that is associated with a different paper: reference_id = " + str(this_reference_id))
                continue
            referencefile_id = this_referencefile_id
            if referencefile_id in referencefile_mod_loaded:
                continue
        i += 1

        if i % batch_commit_size == 0:
            db_session.commit()
            # db_session.rollback()

        if referencefile_id is None:
            referencefile_id = insert_referencefile(db_session, pmid,
                                                    file_name_with_suffix,
                                                    reference_id, md5sum)

        if referencefile_id:
            insert_referencefile_mod(db_session, pmid, file_name_with_suffix,
                                     referencefile_id)
            referencefile_loaded[md5sum] = (referencefile_id, reference_id)
            referencefile_mod_loaded[referencefile_id] = 1

    f.close()
    db_session.commit()
    # db_session.rollback()
    db_session.close()


def insert_referencefile_mod(db_session, pmid, file_name_with_suffix, referencefile_id):

    try:
        x = ReferencefileModAssociationModel(referencefile_id=referencefile_id)
        db_session.add(x)
        logger.info("PMID:" + pmid + ": PMC file = " + file_name_with_suffix + ": loaded into Referencefile_mod table")
    except Exception as e:
        logger.info("PMID:" + pmid + ": PMC file = " + file_name_with_suffix + ": an error occurred when loading data into Referencefile_modtable: " + str(e))


def insert_referencefile(db_session, pmid, file_name_with_suffix, reference_id, md5sum):

    file_extension = file_name_with_suffix.split(".")[-1].lower()
    display_name = file_name_with_suffix.replace("." + file_extension, "")
    pdf_type = None
    if file_extension == 'pdf':
        pdf_type = 'pdf'

    logger.info(file_name_with_suffix + " | " + display_name + " | " + file_extension + " | " + str(pdf_type))

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
        logger.info("PMID:" + pmid + ": PMC file = " + file_name_with_suffix + ": loaded into Referencefile table")
    except Exception as e:
        logger.info("PMID:" + pmid + ": PMC file = " + file_name_with_suffix + " an error occurred when loading data into Referencefile table. error: " + str(e))

    return referencefile_id


if __name__ == "__main__":

    load_data()
