from sqlalchemy import text
import logging
from agr_literature_service.lit_processing.utils.report_utils import send_report
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

db_session = create_postgres_session(False)


query = """
    SELECT r.curie, count(c.reference_id) AS count
      FROM reference r
      LEFT JOIN cross_reference c ON c.reference_id = r.reference_id
    GROUP BY r.curie
    ORDER BY count;
"""
logger.info("Getting data from the database...")

rows = db_session.execute(text(query)).fetchall()

message = ''
for x in rows:
    # rows are order by count so as soon as count has a value we can stop
    # as all the rest have cross reference counts.
    if x[1]:
        break
    message += f"\t{x[0]}\n"

# if messages is  NOT '' then we need to send a report.
if message:
    subject = "references with no cross references report."
    send_report(subject, message)
