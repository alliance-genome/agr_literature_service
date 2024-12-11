import logging
from os import path
from sqlalchemy import text

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.lit_processing.utils.db_read_utils import get_mod_abbreviations
# from agr_literature_service.api.crud.workflow_tag_crud import transition_to_workflow_status
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

batch_size_for_commit = 250
file_uploaded_tag_atp_id = "ATP:0000134"  # file uploaded
file_upload_in_progress_tag_atp_id = "ATP:0000139"
file_needed_tag_atp_id = "ATP:0000141"


def start_backfill_fileupload_workflowTag(mod):

    db = create_postgres_session(False)
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, scriptNm)

    row = db.execute(text("SELECT mod_id FROM mod WHERE abbreviation = '{mod}'")).fetchone()
    mod_id = int(row[0])

    logger.info(f"Backfilling/updating file upload workflow tags for {mod}:")
    check_and_backfill_workflowTags(db, mod_id, mod)


def check_and_backfill_workflowTags(db, mod_id, mod):

    logger.info(f"Retrieving file upload Workflow tags for {mod}:")
    reference_id_to_wfts = get_fileupload_workflowTags(db, mod_id)

    logger.info(f"Retrieving file upload status for {mod}:")
    (reference_id_to_main_pdf_uploaded, reference_id_to_file_uploaded) = get_fileupload_status(db, mod_id)

    logger.info(f"Retrieving in-corpus references for {mod}:")

    sql_query_str = """
        SELECT reference_id
        FROM   mod_corpus_association
        WHERE  mod_id = :mod_id
        AND    corpus = True
    """
    sql_query = text(sql_query_str)
    rows = db.execute(sql_query, {
        'mod_id': mod_id
    })

    logger.info(f"Checking and updating the file upload workflow tags for {mod}:")

    for row in rows:
        reference_id = row[0]
        wfts = reference_id_to_wfts.get(reference_id, set())
        if reference_id in reference_id_to_main_pdf_uploaded:
            ## files uploaded
            check_and_set_correct_WFT(db, mod, mod_id, reference_id, file_uploaded_tag_atp_id, wfts)
        elif reference_id in reference_id_to_file_uploaded:
            ## file upload in progress
            check_and_set_correct_WFT(db, mod, mod_id, reference_id, file_upload_in_progress_tag_atp_id, wfts)
        else:
            ## file needed
            check_and_set_correct_WFT(db, mod, mod_id, reference_id, file_needed_tag_atp_id, wfts)

    db.rollback()
    # db.commit()


def check_and_set_correct_WFT(db, mod, mod_id, reference_id, requried_tag_id, wfts):

    if requried_tag_id in wfts:
        if len(wfts) > 1:
            remove_extra_wfts(db, mod, mod_id, reference_id, requried_tag_id)
            logger.info(f"{mod} reference_id = {reference_id} requires {requried_tag_id}, but has multiple WFT: {wfts}")
    else:
        if len(wfts) == 0:
            logger.info(f"{mod} reference_id = {reference_id} requires {requried_tag_id}, no WFT in database")
            insert_new_wft(db, mod, mod_id, reference_id, requried_tag_id)
        elif len(wfts) == 1:
            logger.info(f"{mod} reference_id = {reference_id} requires {requried_tag_id}, but has {wfts}")
            update_wft(db, mod, mod_id, reference_id, requried_tag_id)
        else:
            logger.info(f"{mod} reference_id = {reference_id} requires {requried_tag_id}, but has multiple WFT: {wfts}")
            insert_new_wft(db, mod, mod_id, reference_id, requried_tag_id)
            remove_extra_wfts(db, mod, mod_id, reference_id, requried_tag_id)


def insert_new_wft(db, mod, mod_id, reference_id, requried_tag_id):

    # insert row if mod_id = mod_id, reference_id=reference_id, workflow_tag_id = requried_tag_id
    return


def update_wft(db, mod, mod_id, reference_id, requried_tag_id):

    # update row if mod_id = mod_id, reference_id=reference_id, workflow_tag_id in [three file upload WFTs]
    return


def remove_extra_wfts(db, mod, reference_id, requried_tag_id):

    # remove row(s) if mod_id = mod_id, reference_id=reference_id, workflow_tag_id in [two non requried_tag_id]
    return


def get_fileupload_status(db, mod_id):

    sql_query_str = """
        SELECT rf.reference_id, rf.file_class, rf.file_publication_status, rf.pdf_type
        FROM referencefile rf
        JOIN referencefile_mod rfm ON rf.referencefile_id = rfm.referencefile_id
        WHERE rfm.mod_id = :mod_id
           OR rfm.mod_id IS NULL
    """
    sql_query = text(sql_query_str)
    rows = db.execute(sql_query, {
        'mod_id': mod_id
    })

    reference_id_to_main_pdf_uploaded = {}
    reference_id_to_file_uploaded = {}
    for row in rows:
        reference_id = row[0]
        file_class = row[1]
        file_status = row[2]
        pdf_type = row[3]
        reference_id_to_file_uploaded[reference_id] = 1
        if file_class == 'main' and file_status == 'final' and pdf_type == 'pdf':
            reference_id_to_main_pdf_uploaded[reference_id] = 1

    return (reference_id_to_main_pdf_uploaded, reference_id_to_file_uploaded)


def get_fileupload_workflowTags(db, mod_id):

    wft_ids = [file_needed_tag_atp_id, file_upload_in_progress_tag_atp_id, file_uploaded_tag_atp_id]

    sql_query_str = """
        SELECT reference_id, workflow_tag_id
        FROM   workflow_tag
        WHERE mod_id = :mod_id
        AND workflow_tag_id in :wft_ids
    """
    sql_query = text(sql_query_str)
    rows = db.execute(sql_query, {
        'mod_id': mod_id,
        'workflow_tag_id': wft_ids
    })

    reference_id_to_wfts = {}
    for row in rows:
        reference_id = row[0]
        wft_id = row[1]
        wfts = set()
        if reference_id in reference_id_to_wfts:
            wfts = reference_id_to_wfts[reference_id]
        wfts.add(wft_id)
        reference_id_to_wfts[reference_id] = wfts

    return reference_id_to_wfts


if __name__ == "__main__":

    for mod in get_mod_abbreviations():
        check_and_backfill_workflowTags(mod)
