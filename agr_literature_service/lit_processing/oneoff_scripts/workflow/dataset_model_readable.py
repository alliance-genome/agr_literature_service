"""
Get transitions from table and print.

i.e.
   python3 table_to_human_readable_transitions.py > new_filename

"""
import logging
import argparse
from sqlalchemy import text
from sqlalchemy.orm import Session
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_engine, \
    create_postgres_session
from agr_literature_service.api.crud.ateam_db_helpers import atp_get_name


logger = logging.getLogger(__name__)

helptext = r"example: python3 dataset_model_readable -m FB"
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description=helptext)
parser.add_argument('-m', '--mod_abbr', help='list dataset/model info for a specific mod', type=str, required=True, default=None)
args = parser.parse_args()

def print_data(db: Session, mod_abbr):  # noqa
    dataset_value_count = {}
    try:
        query = """
        Select dataset_id as dataset_id, classification_value as value, count(*) as count
        from dataset_entry group by (dataset_id, classification_value)"""
        dvcs = db.execute(text(query)).mappings().fetchall()
        for dvc in dvcs:
            if dvc['dataset_id'] not in dataset_value_count:
                dataset_value_count[dvc['dataset_id']] = {}
            dataset_value_count[dvc['dataset_id']][dvc['value']] = dvc['count']
    except Exception as e:
        logger.error(e)
        print(f"Error: {e}")
        exit(-1)
    try:
        query = f"""
        select d.dataset_id as dataset_id, d.title as dataset_title, d.data_type as dataset_type,
               ml.model_type as model_type, ml.topic as tet_value, w.workflow_tag as wft,
               ml.species, ml.negated, ml.novel_topic_data, ml.production
          from dataset d, mod m, ml_model ml
          left join workflow_tag_topic w on ml.topic = w.topic
          where d.dataset_id = ml.dataset_id
                and ml.mod_id = m.mod_id
                and d.mod_id = m.mod_id
                and m.abbreviation = '{mod_abbr}'"""

        mds = db.execute(text(query)).mappings().fetchall()

        for md in mds:
            tet_name = "None"
            if md['wft']:
                tet_name = atp_get_name(md['wft'])
            print(f"""\n{md['dataset_id']}
    'dataset title': "{md['dataset_title']}",
    'atp_dataset_tag':  "{md['dataset_type']}",,
    'dataset_tag': "{atp_get_name(md['dataset_type'])}",
    'model_type':  "{md['model_type']}",
    'atp_tet_topic': "{md['tet_value']}",
    'tet_topic': "{atp_get_name(md['tet_value'])}",
    'workflow tag topic start atp': "{md['wft']}",
    'workflow tag topic start name': "{tet_name}",
    'species':  "{md['species']}",
    "negated": "{md['negated']}",
    "novel topic data": "{md['novel_topic_data']}",
    "production": "{md['production']}" """)
            for key, value in dataset_value_count[md['dataset_id']].items():
                print(f"    class:{key} count: {value}")

    except Exception as e:
        logger.error(e)
        print(f"Error: {e}")
        exit(-1)


if __name__ == "__main__":
    engine = create_postgres_engine(False)
    db_connection = engine.connect()
    db_session: Session = create_postgres_session(False)

    print_data(db_session, args.mod_abbr)
