import logging
from sqlalchemy import text

# from agr_literature_service.lit_processing.utils.report_utils import send_report
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

db_session = create_postgres_session(False)

# literature_subset=# \d  mod_corpus_association_version
#                        Table "public.mod_corpus_association_version"
#            Column           |            Type             | Collation | Nullable | Default
# ----------------------------+-----------------------------+-----------+----------+---------
#  mod_corpus_association_id  | integer                     |           | not null |
#  reference_id               | integer                     |           |          |
#  mod_id                     | integer                     |           |          |
#  corpus                     | boolean                     |           |          |
#  mod_corpus_sort_source     | modcorpussortsourcetype     |           |          |
#  date_updated               | timestamp without time zone |           |          |
#  date_created               | timestamp without time zone |           |          |
#  transaction_id             | bigint                      |           | not null |
#  end_transaction_id         | bigint                      |           |          |
#  operation_type             | smallint                    |           | not null |
#  reference_id_mod           | boolean                     |           | not null | false
#  mod_id_mod                 | boolean                     |           | not null | false
#  corpus_mod                 | boolean                     |           | not null | false
#  mod_corpus_sort_source_mod | boolean                     |           | not null | false
#  date_updated_mod           | boolean                     |           | not null | false
#  date_created_mod           | boolean                     |           | not null | false
#  created_by                 | character varying           |           |          |
#  created_by_mod             | boolean                     |           | not null | false
#  updated_by                 | character varying           |           |          |
#  updated_by_mod             | boolean                     |           | not null | false
query = """
SELECT r.curie, m.date_updated, m.corpus, m.mod_id
from mod_corpus_association_version mv,
     mod_corpus_association m,
     reference r
where r.reference_id = m.reference_id and
      m.mod_corpus_association_id = mv.mod_corpus_association_id and
      mv.corpus_mod = 't' and --  corpus has changed
      (m.corpus = 'f' OR m.corpus is NULL) and  -- corpus value is false or null
      mv.corpus = 't' and -- old corpus is true
      m.date_updated > NOW() - INTERVAL '24 HOURS';"""

logger.info("Getting data from the database...")

rows = db_session.execute(text(query)).fetchall()

message = ''
for x in rows:
    print(x)
