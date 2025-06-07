import logging.config
import urllib
from os import path
from sqlalchemy import text

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import WorkflowTagModel
from agr_literature_service.api.user import set_global_user_id

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


# When running this script, it will do what it can and output messages about what it couldn't do, and someone will fix those things manually.
# Run as-is, then grep that there are no ERROR entries in the output.  grep that all UPDATE entries make sense, at the point of writing the
# script, all entries are only INSERT.  Then uncomment the lines with db_session add/comment to update the database.


def get_from_database(db_session):
    wb_xref_to_reference_id = {}
    agrkb_to_atp = {}
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
    batch_counter = 0
    batch_size = 250
#     for x in rows:
    for x in rows[:2]:
        logger.info(f"reference_id {x[0]}\t{x[1]}\t{x[2]}\t{x[3]}")
        agr_reference_id = x[0]
        wb_wbpaper_id = x[1]
        wb_atp = 'ATP:0000221'
        batch_counter += 1
        if batch_counter % batch_size == 0:
            batch_counter = 0
            # UNCOMMENT TO POPULATE
            # db_session.commit()
        logger.info(f"INSERT {agr_reference_id} {wb_wbpaper_id} is NOT in agrkb_to_atp, needs new value {wb_atp}")
        try:
            x = WorkflowTagModel(reference_id=agr_reference_id,
                                 mod_id=2,
                                 workflow_tag_id=wb_atp)
            db_session.add(x)
        except Exception as e:
            logger.info("An error occurred when adding workflog_tag row for reference_id = " + str(agr_reference_id) + " and atp value = " + wb_atp + " " + str(e))
    # UNCOMMENT TO POPULATE
    # db_session.commit()

#     rows = db_session.execute(text("SELECT reference_id, curie, is_obsolete FROM cross_reference "
#                                    "WHERE curie_prefix = 'WB'")).fetchall()
#     for x in rows:
#         logger.info(f"reference_id {x[0]}\t{x[1]}\t{x[2]}")
#         wb_xref_to_reference_id[x[1]] = [x[0], x[2]]
#     rows = db_session.execute(text("SELECT reference.reference_id, reference.curie, workflow_tag.workflow_tag_id, workflow_tag.reference_workflow_tag_id "
#                                    "FROM reference, workflow_tag "
#                                    "WHERE reference.reference_id = workflow_tag.reference_id "
#                                    "AND workflow_tag.workflow_tag_id IN ( 'ATP:0000103', 'ATP:0000104', 'ATP:0000106' ) "
#                                    "ORDER BY workflow_tag.date_updated")).fetchall()
#     for x in rows:
#         agrkb_to_atp[x[0]] = [x[1], x[2], x[3]]
    return wb_xref_to_reference_id, agrkb_to_atp


def OLD_get_from_database(db_session):
    wb_xref_to_reference_id = {}
    agrkb_to_atp = {}
    rows = db_session.execute("SELECT reference_id, curie, is_obsolete FROM cross_reference "
                              "WHERE curie_prefix = 'WB'").fetchall()
    for x in rows:
        # logger.info(f"reference_id {x[0]}\t{x[1]}\t{x[2]}")
        wb_xref_to_reference_id[x[1]] = [x[0], x[2]]
    rows = db_session.execute("SELECT reference.reference_id, reference.curie, workflow_tag.workflow_tag_id, workflow_tag.reference_workflow_tag_id "
                              "FROM reference, workflow_tag "
                              "WHERE reference.reference_id = workflow_tag.reference_id "
                              "AND workflow_tag.workflow_tag_id IN ( 'ATP:0000103', 'ATP:0000104', 'ATP:0000106' ) "
                              "ORDER BY workflow_tag.date_updated").fetchall()
    for x in rows:
        agrkb_to_atp[x[0]] = [x[1], x[2], x[3]]
    return wb_xref_to_reference_id, agrkb_to_atp


def OLD_process_wormbase_data(wb_xref_to_reference_id, agrkb_to_atp, db_session):
    # to set database user as "populate_wormbase_workflow_reftype" instead of "default_user"
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)

    url = 'https://tazendra.caltech.edu/~postgres/agr/lit/wb_curatability_reference_type.tsv'
    f = urllib.request.urlopen(url)
    wormbase_stuff = f.read().decode('utf-8')
    wormbase_stuff_array = wormbase_stuff.strip().split("\n")
    logger.info("processing wormbase data")
    # counter = 0
    # max_count = 3
    batch_counter = 0
    batch_size = 250
    for line in wormbase_stuff_array:
        # counter += 1
        # if counter > max_count:
        #     break
        # has_errors = False
        wb_data = line.split("\t")
        wb_wbpaper_id = wb_data[0]
        wb_atp = wb_data[1]
        # logger.info(f"line {line} valid {wb_wbpaper_id} merged {wb_atp}")
        if wb_wbpaper_id in wb_xref_to_reference_id:
            [agr_reference_id, obs] = wb_xref_to_reference_id[wb_wbpaper_id]
            if obs is True:
                # has_errors = True
                logger.info(f"ERROR wb_wbpaper_id {wb_wbpaper_id} is_obsolete for {agr_reference_id}, needs {wb_atp}")
            # logger.info(f"wb_wbpaper_id {wb_wbpaper_id} is in wb_xref_to_reference_id")
            else:
                if agr_reference_id in agrkb_to_atp:
                    [agrkb, agr_atp, ref_wf_tag_id] = agrkb_to_atp[agr_reference_id]
                    if agr_atp != wb_atp:
                        logger.info(f"UPDATE wb_wbpaper_id {wb_wbpaper_id} is {agr_atp} for {agr_reference_id}/{agrkb}, needs {wb_atp}, update {ref_wf_tag_id}")
                        workflow_tag_db_obj = db_session.query(WorkflowTagModel).filter(WorkflowTagModel.reference_workflow_tag_id == ref_wf_tag_id).first()
                        workflow_tag_db_obj.workflow_tag_id = wb_atp
                        db_session.add(workflow_tag_db_obj)
                    else:
                        logger.info(f"NO ACTION wb_wbpaper_id {wb_wbpaper_id} is {agr_atp} for {agr_reference_id}/{agrkb}, needs {wb_atp}, no update {ref_wf_tag_id}")
                else:
                    logger.info(f"INSERT {agr_reference_id} {wb_wbpaper_id} is NOT in agrkb_to_atp, needs new value {wb_atp}")
                    try:
                        x = WorkflowTagModel(reference_id=agr_reference_id,
                                             workflow_tag_id=wb_atp)
                        db_session.add(x)
                    except Exception as e:
                        logger.info("An error occurred when adding workflog_tag row for reference_id = " + str(agr_reference_id) + " and atp value = " + wb_atp + " " + str(e))
                batch_counter += 1
                if batch_counter % batch_size == 0:
                    batch_counter = 0
                    # UNCOMMENT TO POPULATE
                    # db_session.commit()
        else:
            # has_errors = True
            logger.info(f"ERROR wb_wbpaper_id {wb_wbpaper_id} is NOT in wb_xref_to_reference_id, needs new value {wb_atp}")
    # UNCOMMENT TO POPULATE
    # db_session.commit()


if __name__ == "__main__":
    db_session = create_postgres_session(False)
    wb_xref_to_reference_id, agrkb_to_atp = get_from_database(db_session)
#     wb_xref_to_reference_id, agrkb_to_atp = OLD_get_from_database(db_session)
#     OLD_process_wormbase_data(wb_xref_to_reference_id, agrkb_to_atp, db_session)
