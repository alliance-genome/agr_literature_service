import logging.config
from os import path
from sqlalchemy import text

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import WorkflowTagModel
from agr_literature_service.api.user import set_global_user_id

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


def get_from_database(db_session):
    rows = db_session.execute(text("""
        SELECT DISTINCT ON (cr.reference_id)
               cr.curie, cr.reference_id, cr.is_obsolete,
               r.curie AS reference_curie
        FROM cross_reference cr
        JOIN reference r ON cr.reference_id = r.reference_id
        WHERE cr.curie_prefix = 'WB'
          AND cr.is_obsolete = FALSE
          AND EXISTS (
              SELECT 1
              FROM workflow_tag wt1
              JOIN mod m1 ON wt1.mod_id = m1.mod_id
              WHERE wt1.reference_id = r.reference_id
                AND m1.abbreviation = 'WB'
                AND wt1.workflow_tag_id = 'ATP:0000163'
          )
          AND NOT EXISTS (
              SELECT 1
              FROM workflow_tag wt2
              JOIN mod m2 ON wt2.mod_id = m2.mod_id
              WHERE wt2.reference_id = r.reference_id
                AND m2.abbreviation = 'WB'
                AND wt2.workflow_tag_id IN (
                    'ATP:0000173', 'ATP:0000174', 'ATP:0000190', 'ATP:0000187'
                )
          )
        ORDER BY cr.reference_id
    """)).fetchall()
    atp_tags = ['ATP:0000221', 'ATP:0000175', 'ATP:0000173', 'ATP:0000220', 'ATP:0000206', 'ATP:0000272', 'ATP:0000269']
    batch_counter = 0
    batch_size = 250
    for x in rows:
        wb_wbpaper_id = x[0]
        agr_reference_id = x[1]
        for wb_atp in atp_tags:
            batch_counter += 1
            if batch_counter % batch_size == 0:
                batch_counter = 0
                # UNCOMMENT TO POPULATE
                # db_session.commit()
            logger.info(f"INSERT {agr_reference_id} {wb_wbpaper_id} is NOT in entity extraction needed, needs new value {wb_atp}")
            try:
                x = WorkflowTagModel(reference_id=agr_reference_id,
                                     mod_id=2,
                                     workflow_tag_id=wb_atp)
                db_session.add(x)
            except Exception as e:
                logger.info("An error occurred when adding workflog_tag row for reference_id = " + str(agr_reference_id) + " and atp value = " + wb_atp + " " + str(e))
    # UNCOMMENT TO POPULATE
    # db_session.commit()


if __name__ == "__main__":
    db_session = create_postgres_session(False)
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)
    get_from_database(db_session)
