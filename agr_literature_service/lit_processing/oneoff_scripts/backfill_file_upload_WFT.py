import logging
from os import path
from sqlalchemy import text, bindparam

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.lit_processing.utils.db_read_utils import get_mod_abbreviations
from agr_literature_service.api.crud.workflow_tag_crud import transition_to_workflow_status, \
    get_workflow_tags_from_process
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

batch_size_for_commit = 250
file_uploaded_tag_atp_id = "ATP:0000134"
file_upload_in_progress_tag_atp_id = "ATP:0000139"
file_needed_tag_atp_id = "ATP:0000141"


def start_backfill_fileupload_workflowTag(mod):

    db = create_postgres_session(False)
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, scriptNm)

    logger.info(f"Getting mod_id for {mod}:")
    row = db.execute(text("SELECT mod_id FROM mod WHERE abbreviation = :mod"),
                     {"mod": mod}).fetchone()
    mod_id = int(row[0])

    # logger.info(f"Backfilling/updating file upload workflow tags for {mod}:")
    # check_and_backfill_workflowTags(db, mod_id, mod)

    logger.info(f"Cleaning up text conversion WFT etc for papers without a main PDF for {mod}:")
    cleanup_text_conversion_and_other_workflow_tags(db, mod_id, mod)


def cleanup_text_conversion_and_other_workflow_tags(db, mod_id, mod):

    logger.info(f"Retrieving papers with a main pdf for {mod}:")
    reference_ids_with_main_pdf = get_references_with_main_pdf(db, mod_id, mod)

    all_text_related_workflow_tags = get_all_text_conversion_classification_extraction_wfts(db)

    sql_query_str = """
        SELECT reference_id, workflow_tag_id
        FROM   workflow_tag
        WHERE  mod_id = :mod_id
        AND    workflow_tag_id IN :all_text_related_workflow_tags
    """
    sql_query = text(sql_query_str)
    rows = db.execute(sql_query, {
        'mod_id': mod_id,
        'all_text_related_workflow_tags': tuple(all_text_related_workflow_tags)
    })

    for row in rows:
        reference_id = row[0]
        workflow_tag_id = row[1]
        if reference_id not in reference_ids_with_main_pdf:
            logger.info(f"{mod} {reference_id} {workflow_tag_id}: Removing unwanted WFT")
            sql_query = text("""
            DELETE FROM workflow_tag
            WHERE reference_id = :reference_id
            AND mod_id = :mod_id
            AND workflow_tag_id = :workflow_tag_id
            """)
            db.execute(sql_query, {
                'reference_id': reference_id,
                'mod_id': mod_id,
                'workflow_tag_id': workflow_tag_id
            })
            db.commit()


def get_all_text_conversion_classification_extraction_wfts(db):

    all_text_conversion_wft = get_workflow_tags_from_process("ATP:0000161")
    all_ref_classification_wft = get_workflow_tags_from_process("ATP:0000165")
    all_entity_extraction_wft = get_workflow_tags_from_process("ATP:0000172")
    return all_text_conversion_wft + all_ref_classification_wft + all_entity_extraction_wft


def get_references_with_main_pdf(db, mod_id, mod):

    sql_query_str = """
        SELECT reference_id
        FROM   workflow_tag
        WHERE  mod_id = :mod_id
        AND    workflow_tag_id = :workflow_tag_id
    """
    sql_query = text(sql_query_str)
    rows = db.execute(sql_query, {
        'mod_id': mod_id,
        'workflow_tag_id': file_uploaded_tag_atp_id
    })
    return {row[0] for row in rows}


def check_and_backfill_workflowTags(db, mod_id, mod):

    logger.info(f"Retrieving file upload Workflow tags for {mod}:")
    reference_id_to_wft = get_fileupload_workflowTags(db, mod_id)

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

    count = 0
    for row in rows:
        reference_id = row[0]
        wft = reference_id_to_wft.get(reference_id)
        if reference_id in reference_id_to_main_pdf_uploaded:
            ## files uploaded
            if wft != file_uploaded_tag_atp_id:
                count += 1
                check_and_set_correct_WFT(db, mod, mod_id, reference_id, file_uploaded_tag_atp_id, wft)
        elif reference_id in reference_id_to_file_uploaded:
            ## file upload in progress
            if wft != file_upload_in_progress_tag_atp_id:
                count += 1
                check_and_set_correct_WFT(db, mod, mod_id, reference_id, file_upload_in_progress_tag_atp_id, wft)
        else:
            ## file needed
            if wft != file_needed_tag_atp_id:
                count += 1
                check_and_set_correct_WFT(db, mod, mod_id, reference_id, file_needed_tag_atp_id, wft)
        if count % batch_size_for_commit == 0:
            db.commit()
    db.commit()


def check_and_set_correct_WFT(db, mod, mod_id, reference_id, requried_tag_id, wft):

    try:
        if wft is None and requried_tag_id != file_needed_tag_atp_id:
            transition_to_workflow_status(db, str(reference_id), mod, file_needed_tag_atp_id)
        transition_to_workflow_status(db, str(reference_id), mod, requried_tag_id)
        logger.info(f"Transitioning file_upload workflow_tag from {wft} to {requried_tag_id} for reference_id = {reference_id}, mod={mod}")
    except Exception as e:
        logger.info(f"An error occurred when transitioning file_upload workflow_tag {wft} to {requried_tag_id} for mod={mod}, reference_id={reference_id}. error={e}")
        db.rollback()


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
        if file_class == 'main' and file_status == 'final' and (pdf_type == 'pdf' or not pdf_type):
            reference_id_to_main_pdf_uploaded[reference_id] = 1

    return (reference_id_to_main_pdf_uploaded, reference_id_to_file_uploaded)


def get_fileupload_workflowTags(db, mod_id):

    wft_ids = [
        file_needed_tag_atp_id,
        file_upload_in_progress_tag_atp_id,
        file_uploaded_tag_atp_id
    ]

    sql_query_str = """
        SELECT reference_id, workflow_tag_id
        FROM workflow_tag
        WHERE mod_id = :mod_id
          AND workflow_tag_id IN :wft_ids
    """
    sql_query = text(sql_query_str).bindparams(
        bindparam('wft_ids', expanding=True)
    )

    rows = db.execute(sql_query, {
        'mod_id': mod_id,
        'wft_ids': wft_ids
    })

    reference_id_to_wft = {row.reference_id: row.workflow_tag_id for row in rows}

    return reference_id_to_wft


if __name__ == "__main__":

    for mod in get_mod_abbreviations():
        start_backfill_fileupload_workflowTag(mod)
