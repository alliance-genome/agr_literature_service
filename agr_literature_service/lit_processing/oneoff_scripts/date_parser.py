from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.utils.date_utils import parse_date
import logging
import sys
# import re

# from calendar import monthrange
from datetime import datetime


logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format='%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',
                    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

db_session = create_postgres_session(False)


def get_xrefs(curie):
    xref_results = db_session.execute(f"SELECT curie FROM cross_reference WHERE reference_id IN ( SELECT reference_id FROM reference WHERE curie = '{curie}')")
    xrefs = xref_results.fetchall()
    return ", ".join([xref[0] for xref in xrefs])


# rs = db_session.execute("SELECT curie, date_published FROM reference WHERE date_published IS NOT NULL AND date_published !~ '^\d\d\d\d-\d\d-\d\d$' AND date_published ~ '[a-zA-Z]' AND date_published != 'Unknown' ORDER BY curie")
# rs = db_session.execute("SELECT curie, date_published FROM reference WHERE date_published IS NOT NULL AND date_published !~ '^\d\d\d\d-\d\d-\d\d$' AND date_published ~ '[a-zA-Z]' ORDER BY curie")
# rs = db_session.execute("SELECT curie, date_published FROM reference WHERE date_published IS NOT NULL AND date_published !~ '^\d\d\d\d-\d\d-\d\d$' AND date_published !~ '[a-zA-Z]' ORDER BY curie")  # process non-text dates
# rs = db_session.execute("SELECT curie, date_published FROM reference WHERE date_published IS NOT NULL ORDER BY curie LIMIT 5")  # process sample

rs = db_session.execute("SELECT curie, date_published FROM reference WHERE date_published IS NOT NULL ORDER BY curie")  # process everything
# data returns like
# [('AGR:AGR-Reference-0000000001', '2013-04-19'), ('AGR:AGR-Reference-0000000002', '1975-01-01'), ('AGR:AGR-Reference-0000000003', '1996-05-15'), ('AGR:AGR-Reference-0000000004', '1988-07-01'), ('AGR:AGR-Reference-0000000005', '2007-01-01')]

rows = rs.fetchall()
for row in rows:
    curie = row[0]
    date_string = row[1]
    # logger.info(f"{date_string}\t{curie}")
    date_range, error_message = parse_date(row[1], False)
    if date_range:
        start_date_object = datetime.strptime(date_range[0], '%Y-%m-%d').date()
        end_date_object = datetime.strptime(date_range[1], '%Y-%m-%d').date()
        logger.info(f"{date_string}\t{date_range}\t{start_date_object}\t{end_date_object}\t{curie}")
    else:
        xrefs = get_xrefs(curie)
        logger.info(f"{date_string}\tnot_found\t{curie}\t{xrefs}")
    if error_message:
        logger.info(error_message)


db_session.close()
