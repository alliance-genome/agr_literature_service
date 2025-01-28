from sqlalchemy import text
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
import logging

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

db_session = create_postgres_session(False)

rows = db_session.execute(text("SELECT reference_workflow_tag_id "
                               "FROM workflow_tag "
                               "WHERE mod_id = 2 "
                               "AND workflow_tag_id = 'ATP:0000139' "
                               "AND reference_id in ( "
                               "select reference_id from referencefile "
                               "where file_publication_status = 'final' "
                               "and file_class = 'main' "
                               "and pdf_type != 'pdf' "
                               "and pdf_type is not null)")).fetchall()
count = 0
for row in rows:
    count += 1
    db_session.execute(text(f"UPDATE workflow_tag "
                            f"SET workflow_tag_id = 'ATP:0000134' "
                            f"WHERE reference_workflow_tag_id = {row[0]}"))
    logger.info(f"{count} setting workflow_tag_id = 'ATP:0000134' for reference_workflow_tag_id = {row[0]}")
    if count % 250 == 0:
        db_session.commit()

db_session.commit()
db_session.close()
