"""
Get transitions from table and print.

i.e.
   python3 table_to_human_readable_transitions.py > new_filename

"""
import logging
from sqlalchemy import text
from sqlalchemy.orm import Session
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_engine, \
    create_postgres_session
from agr_literature_service.api.crud.ateam_db_helpers import atp_get_name


logger = logging.getLogger(__name__)


def print_wtt(db: Session):  # noqa
    try:
        query = r"""
        select workflow_tag_topic_id, workflow_tag, topic
          from workflow_tag_topic"""

        wtts = db.execute(text(query)).mappings().fetchall()
        start = '{'
        end = '}'

        for wtt in wtts:
            if 1:
                print(f"""
        {start}'id': "{wtt['workflow_tag_topic_id']}",
               'atp_tag':  "{wtt['workflow_tag']}",,
               'tag': "{atp_get_name(wtt['workflow_tag'])}",
               'atp_topic':  "{wtt['topic']}",,
               'topic': "{atp_get_name(wtt['topic'])}"{end},""")
    except Exception as e:
        logger.error(e)
        print(f"Error: {e}")
        exit(-1)


if __name__ == "__main__":
    engine = create_postgres_engine(False)
    db_connection = engine.connect()
    db_session: Session = create_postgres_session(False)

    print_wtt(db_session)
