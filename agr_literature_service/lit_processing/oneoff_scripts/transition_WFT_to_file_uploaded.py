import logging
from os import path
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.api.crud.workflow_tag_crud import transition_to_workflow_status
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

batch_size_for_commit = 250
file_uploaded_tag_atp_id = "ATP:0000134"  # file uploaded


def add_file_uploaded_workflow():

    db_session = create_postgres_session(False)
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)

    rows = db_session.execute("SELECT mod_id, abbreviation FROM mod").fetchall()
    mod_id_to_mod = {row['mod_id']: row['abbreviation'] for row in rows}

    rows = db_session.execute(f"SELECT mod_id, reference_id "
                              f"FROM workflow_tag "
                              f"WHERE workflow_tag_id = '{file_uploaded_tag_atp_id}'").fetchall()
    mod_reference_with_pdf_uploaded = {(x['mod_id'], x['reference_id']) for x in rows}

    rows = db_session.execute("SELECT mod_id, reference_id "
                              "FROM mod_corpus_association "
                              "WHERE corpus is True").fetchall()
    mod_reference_in_corpus = {(x['mod_id'], x['reference_id']) for x in rows}

    rows = db_session.execute("SELECT rfm.mod_id, rf.reference_id "
                              "FROM referencefile rf, referencefile_mod rfm "
                              "WHERE rf.referencefile_id = rfm.referencefile_id "
                              "AND rf.file_class = 'main' "
                              "AND rf.file_publication_status = 'final' "
                              "AND rf.file_extension = 'pdf'").fetchall()
    count = 0
    processed = set()
    for x in rows:
        mod_id = x['mod_id']
        reference_id = x['reference_id']
        mod_ids = [mod_id]
        if mod_id is None:
            mod_ids = [1, 2, 3, 4, 5, 6, 7]
        for mod_id in mod_ids:
            if (mod_id, reference_id) not in mod_reference_in_corpus:
                continue
            if (mod_id, reference_id) in mod_reference_with_pdf_uploaded:
                continue
            if (mod_id, reference_id) in processed:
                continue
            mod = mod_id_to_mod[mod_id]
            count += 1
            try:
                transition_to_workflow_status(db_session, str(reference_id), mod, file_uploaded_tag_atp_id)
                logger.info(f"{count} Transitioning file_upload workflow_tag to 'file_uploaded' for reference_id = {reference_id}, mod={mod}")
            except Exception as e:
                logger.error(f"An error occurred when transitioning file_upload workflow_tag to 'file_uploaded' for mod={mod}, reference_id={reference_id}. error={e}")
                db_session.rollback()
                return
            if count % batch_size_for_commit == 0:
                db_session.commit()
            processed.add((mod_id, reference_id))

    db_session.commit()


if __name__ == "__main__":

    add_file_uploaded_workflow()
