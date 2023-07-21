import logging
from collections import OrderedDict
# # type: ignore from typing import OrderedDict, Any

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

db_session = create_postgres_session(False)

# create dict for storing results by mod_id
# and get dict to convert mod_id to abbreviation
mod_query = """
SELECT mod_id, abbreviation
  FROM mod;
"""
rows = db_session.execute(mod_query).fetchall()
mod_refs = OrderedDict()  # type: ignore
mod_id_to_mod = {}
for x in rows:
    mod_refs[x[0]] = OrderedDict()
    mod_id_to_mod[x[0]] = x[1]

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
SELECT r.curie, mv.date_updated, m.corpus, m.mod_id
from mod_corpus_association_version mv,
     mod_corpus_association m,
     reference r
where r.reference_id = m.reference_id and
      m.mod_corpus_association_id = mv.mod_corpus_association_id and
      mv.corpus_mod = 't' and --  corpus has changed
      (m.corpus = 'f' OR m.corpus is NULL) and  -- corpus value is false or null
       mv.corpus = 't'  -- old corpus is true
       ORDER BY mv.date_updated desc;  -- newest first """
#      m.date_updated > NOW() - INTERVAL '24 HOURS';"""

logger.info("Getting data from the database...")

rows = db_session.execute(query).fetchall()

message = ''

for x in rows:
    # x -> [0] r.curie  [1] mv.date_updated  [2] m.corpus  [3] m.mod_id
    if x[0] not in mod_refs[x[3]]:
        mod_refs[x[3]][x[0]] = x

for mod_key in mod_refs.keys():
    print(f"First 10 changes for mod {mod_id_to_mod[mod_key]}")
    count = 0
    for curie_key in reversed(mod_refs[mod_key].keys()):
        count += 1
        (_, date_updated, corpus, _) = mod_refs[mod_key][curie_key]
        if count > 10:
            break
        change = "removed from corpus"
        if corpus is None:
            change = "under review"
        print(f"{curie_key} was changed to {change} on {date_updated}")
    print()
