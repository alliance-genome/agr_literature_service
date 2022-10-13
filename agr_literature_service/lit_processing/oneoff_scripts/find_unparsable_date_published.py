from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.utils.date_utils import parse_date
import logging
import sys
# from datetime import datetime
# from agr_literature_service.api.models import ReferenceModel


logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format='%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',
                    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def get_xrefs(curie):
    xref_results = db_session.execute(f"SELECT curie FROM cross_reference WHERE reference_id IN ( SELECT reference_id FROM reference WHERE curie = '{curie}')")
    xrefs = xref_results.fetchall()
    return ", ".join([xref[0] for xref in xrefs])


def query_data():
    # rs = db_session.execute("SELECT curie, date_published FROM reference WHERE date_published IS NOT NULL AND date_published !~ '^\d\d\d\d-\d\d-\d\d$' AND date_published ~ '[a-zA-Z]' AND date_published != 'Unknown' ORDER BY curie")  # process text dates that are not Unknown
    # rs = db_session.execute("SELECT curie, date_published FROM reference WHERE date_published IS NOT NULL AND date_published !~ '^\d\d\d\d-\d\d-\d\d$' AND date_published ~ '[a-zA-Z]' ORDER BY curie")  # process text dates
    # rs = db_session.execute("SELECT curie, date_published FROM reference WHERE date_published IS NOT NULL AND date_published !~ '^\d\d\d\d-\d\d-\d\d$' AND date_published !~ '[a-zA-Z]' ORDER BY curie")  # process non-text dates
    # rs = db_session.execute("SELECT curie, date_published FROM reference WHERE date_published IS NOT NULL ORDER BY curie LIMIT 5")  # process sample

    # rs = db_session.execute("SELECT reference_id, curie, date_published FROM reference WHERE date_published IS NOT NULL AND date_published_start IS NULL AND date_published_end IS NULL ORDER BY curie LIMIT 10000")  # process sample

    rs = db_session.execute("SELECT reference_id, curie, date_published FROM reference WHERE date_published IS NOT NULL AND date_published_start IS NULL AND date_published_end IS NULL ORDER BY curie")  # process everything

    # data returns like
    # [('AGR:AGR-Reference-0000000001', '2013-04-19'), ('AGR:AGR-Reference-0000000002', '1975-01-01'), ('AGR:AGR-Reference-0000000003', '1996-05-15'), ('AGR:AGR-Reference-0000000004', '1988-07-01'), ('AGR:AGR-Reference-0000000005', '2007-01-01')]

    rows = rs.fetchall()
    return rows


def find_data_mappings(db_session):
    rows = query_data()
    i = 0
    for row in rows:
        reference_id = row[0]
        curie = row[1]
        date_string = row[2]
        # logger.info(f"{date_string}\t{curie}")
        date_range, error_message = parse_date(date_string, False)
        if date_range:
            update_db_dates(db_session, reference_id, date_range[0], date_range[1])
            logger.info(f"{reference_id}\t{date_string}\t{date_range}\t{date_range[0]}\t{date_range[1]}\t{curie}")
            # converting to datetime object is unncessary
            # start_date_object = datetime.strptime(date_range[0], '%Y-%m-%d').date()
            # end_date_object = datetime.strptime(date_range[1], '%Y-%m-%d').date()
            # update_db_dates(db_session, reference_id, start_date_object, end_date_object)
            # logger.info(f"{reference_id}\t{date_string}\t{date_range}\t{start_date_object}\t{end_date_object}\t{curie}")
            i += 1
            if i % 250 == 0:
                db_session.commit()
        else:
            xrefs = get_xrefs(curie)
            logger.info(f"{date_string}\tnot_found\t{curie}\t{xrefs}")
        if error_message:
            logger.info(error_message)
    db_session.commit()


def update_db_dates(db_session, reference_id, start_date_string, end_date_string):
    try:
        # 0.8 sec to process 1k entries with raw sql  12 min 33 sec for about 906358 references
        db_session.execute(f"UPDATE reference SET date_published_start = '{start_date_string}', date_published_end = '{end_date_string}' WHERE reference_id = '{reference_id}'")
        logger.info(f"UPDATE reference SET date_published_start = '{start_date_string}', date_published_end = '{end_date_string}' WHERE reference_id = '{reference_id}'")

        # 2 min 20 sec to process 1k entries with sql alchemy
        # x = db_session.query(ReferenceModel).filter_by(reference_id=reference_id).one_or_none()
        # if x is None:
        #     return
        # x.date_published_start = start_date_string
        # x.date_published_end = end_date_string
        # logger.info(f"Updated date published for reference_id = {str(reference_id)}")

    except Exception as e:
        logger.info(f"An error occurred when updating date published for for reference_id = {str(reference_id)} error: {str(e)}")


if __name__ == "__main__":

    db_session = create_postgres_session(False)
    find_data_mappings(db_session)
    db_session.close()
