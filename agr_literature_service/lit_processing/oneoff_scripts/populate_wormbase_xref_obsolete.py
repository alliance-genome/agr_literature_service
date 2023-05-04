import logging.config
import urllib
from os import path

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import CrossReferenceModel
from agr_literature_service.api.user import set_global_user_id

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


# When running this script, it will do what it can and output messages about what it couldn't do, and someone will fix those things manually.


def get_from_database(db_session):
    wb_xref_to_reference_id = {}
    rows = db_session.execute("SELECT reference_id, curie, is_obsolete FROM cross_reference "
                              "WHERE curie_prefix = 'WB'").fetchall()
    for x in rows:
        # logger.info(f"reference_id {x[0]}\t{x[1]}\t{x[2]}")
        wb_xref_to_reference_id[x[1]] = [x[0], x[2]]
    return wb_xref_to_reference_id


def process_wormbase_data(wb_xref_to_reference_id, db_session):
    # to set database user as "populate_wormbase_xref_obsolete" instead of "default_user"
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)

    url = 'https://tazendra.caltech.edu/~postgres/agr/lit/merged_papers.tsv'
    f = urllib.request.urlopen(url)
    wormbase_stuff = f.read().decode('utf-8')
    wormbase_stuff_array = wormbase_stuff.strip().split("\n")
    logger.info("processing wormbase data")
    for line in wormbase_stuff_array:
        has_errors = False
        wb_valid_merged = line.split("\t")
        wb_valid = wb_valid_merged[0]
        wb_merged = wb_valid_merged[1]
        # logger.info(f"line {line} valid {wb_valid} merged {wb_merged}")
        if wb_valid in wb_xref_to_reference_id:
            [agrkb, obs] = wb_xref_to_reference_id[wb_valid]
            if obs is True:
                has_errors = True
                logger.info(f"wb_valid {wb_valid} is_obsolete for {agrkb}, needs {wb_merged}")
            # logger.info(f"wb_valid {wb_valid} is in wb_xref_to_reference_id")
        else:
            has_errors = True
            logger.info(f"wb_valid {wb_valid} is NOT in wb_xref_to_reference_id, needs obsolete {wb_merged}")
        if wb_merged in wb_xref_to_reference_id:
            has_errors = True
#             logger.info(f"wb_merged {wb_merged} is already {agrkb}, should be in {wb_valid}")
        # else:
        #     logger.info(f"wb_merged {wb_merged} is correctly NOT in wb_xref_to_reference_id")
        if has_errors is False:
            reference_id = wb_xref_to_reference_id[wb_valid][0]
            logger.info(f"Add {wb_merged} obsolete to {reference_id}")
            try:
                x = CrossReferenceModel(reference_id=reference_id,
                                        curie_prefix=wb_merged.split(':')[0],
                                        curie=wb_merged,
                                        is_obsolete=True,
                                        pages=['reference'])
                db_session.add(x)
                logger.info("The cross_reference row for reference_id = " + str(reference_id) + " and curie = " + wb_merged + " has been added into database.")
            except Exception as e:
                logger.info("An error occurred when adding cross_reference row for reference_id = " + str(reference_id) + " and curie = " + wb_merged + " " + str(e))
    db_session.commit()

# test database afterward for something that should have been created with is_obsolete = True  and pages = {reference}
# SELECT * FROM cross_reference WHERE curie = 'WB:WBPaper00046376'


if __name__ == "__main__":
    db_session = create_postgres_session(False)
    wb_xref_to_reference_id = get_from_database(db_session)
    process_wormbase_data(wb_xref_to_reference_id, db_session)
