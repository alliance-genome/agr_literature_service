from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from os import environ
from agr_literature_service.api.models import (
    WorkflowTransitionModel,
    WorkflowTagTopicModel, MLModel
)


def add_sequence_data(db_subset_session):
    # Need to set the initial seq values else we cannot add any more entries,
    # which might be needed for testing the api.

    # get data needed for adding new values
    # following query returns something like:-
    # author                             | author_id                           | author_author_id_seq
    # citation                           | citation_id                         | citation_citation_id_seq
    # copyright_license                  | copyright_license_id                | copyright_license_copyright_license_id_seq
    # ........

    query = r"""SELECT t.oid::regclass AS table_name,
       a.attname AS column_name,
       s.relname AS sequence_name
    FROM pg_class AS t
    JOIN pg_attribute AS a
      ON a.attrelid = t.oid
    JOIN pg_depend AS d
      ON d.refobjid = t.oid
         AND d.refobjsubid = a.attnum
    JOIN pg_class AS s
      ON s.oid = d.objid
    WHERE d.classid = 'pg_catalog.pg_class'::regclass
        AND d.refclassid = 'pg_catalog.pg_class'::regclass
        AND d.deptype IN ('i', 'a')
        AND t.relkind IN ('r', 'P')
        AND s.relkind = 'S'
  """
    # print(query)
    rows = db_subset_session.execute(text(query)).fetchall()
    # print(rows)
    for x in rows:

        if x[0] not in ('workflow_transition'):
            continue
        # first get the current max value.
        max_query = f"SELECT MAX({x[1]}) from {x[0]}"
        max_val = db_subset_session.execute(text(max_query)).fetchall()[0][0]
        # and set seq to that  +1 and or 0 if none found
        if max_val:
            print(f"setting max value to {max_val + 1} for {x[2]}")
            com = f"ALTER SEQUENCE {x[2]} RESTART WITH {max_val+1}"
            db_subset_session.execute(text(com))
        else:
            print(f"setting max value to 0 for {x[2]} as none found")
            com = f"ALTER SEQUENCE {x[2]} RESTART WITH 1"
            db_subset_session.execute(text(com))

def create_postgres_engine(production=False):
    """Connect to database.
    """
    if production:
        USER = environ.get('PROD_USERNAME', None)
        PASSWORD = environ.get('PROD_PASSWORD', None)
        SERVER = environ.get('PROD_HOST', None)
        PORT = environ.get('PROD_PORT', '5432')
        DB = environ.get('PROD_DATABASE', 'literature')
    else:
        USER = environ.get('STAGE_USERNAME', None)
        PASSWORD = environ.get('STAGE_PASSWORD', None)
        SERVER = environ.get('STAGE_HOST', None)
        PORT = environ.get('STAGE_PORT', '5432')
        DB = environ.get('STAGE_DATABASE', 'literature')

    # Create our SQL Alchemy engine from our environmental variables.
    engine_var = 'postgresql://' + USER + ":" + PASSWORD + '@' + SERVER + ':' + PORT + '/' + DB
    engine = create_engine(engine_var)

    return engine

def main():
    prod_eng = create_postgres_engine(True)
    Session = sessionmaker(bind=prod_eng, autoflush=False, autocommit=False)
    prod_db = Session()

    dev_eng = create_postgres_engine(False)
    Session = sessionmaker(bind=dev_eng, autoflush=False, autocommit=False)
    dev_db = Session()

    query = "SELECT count(*) as count FROM workflow_transition"
    counts = prod_db.execute(text(query)).fetchall()
    for count in counts:
        print(f"prod count = {count[0]}")

    counts = dev_db.execute(text(query)).fetchall()
    for count in counts:
        print(f"dev count = {count[0]}")

    #  'workflow_transition': workflow_transition
    query = "delete from workflow_transition"
    dev_db.execute(text(query))
    wts = prod_db.query(WorkflowTransitionModel).all()
    for wt in wts:
        dev_db.merge(wt)
    dev_db.commit()

    #exit()
    #query = "delete from workflow_tag_topic"
    #dev_db.execute(text(query))

    #wtts = prod_db.query(WorkflowTagTopicModel).all()
    #for wtt in wtts:
    #    dev_db.merge(wtt)
    #dev_db.commit()

    add_sequence_data(dev_db)

def models_copy_NOT():
    prod_eng = create_postgres_engine(True)
    Session = sessionmaker(bind=prod_eng, autoflush=False, autocommit=False)
    prod_db = Session()

    dev_eng = create_postgres_engine(False)
    Session = sessionmaker(bind=dev_eng, autoflush=False, autocommit=False)
    dev_db = Session()

    rows = prod_db.execute(text("SELECT m.* "
                           "FROM dataset m "
                           "WHERE m.mod_id = 1")).mappings().fetchall()
    topic_to_dataset = {}
    for row in rows:
        row_dict = dict(row)
        print(row_dict)
        query = f"""INSERT INTO dataset 
        (title, mod_id, data_type, dataset_type, version, description, frozen, date_created)
        VALUES ('{row_dict['title']}', {row_dict['mod_id']}, '{row_dict['data_type']}',
        '{row_dict['dataset_type']}', {row_dict['version']}, '{row_dict['description']}', {row_dict['frozen']}, NOW()) 
        RETURNING dataset_id"""
        print(query)
        value = dev_db.execute(text(query)).fetchone()[0]
        print(F"Value: {value}")
        topic_to_dataset[row_dict['data_type']] = value

    rows = prod_db.execute(text("SELECT m.* "
                           "FROM ml_model m "
                           "WHERE m.mod_id = 1")).mappings().fetchall()
    for row in rows:
        row_dict = dict(row)
        print(row_dict)
        print(row_dict['topic'])
        query = f"""INSERT INTO ml_model 
        (task_type, model_type, file_extension, mod_id, topic, version_num, precision, recall, f1_score,
         parameters, dataset_id, production, negated, novel_topic_data)
         VALUES ('{row_dict['task_type']}', '{row_dict['model_type']}', '{row_dict['file_extension']}', 
                  {row_dict['mod_id']}, '{row_dict['topic']}', {row_dict['version_num']}, 
                  {row_dict['precision']}, {row_dict['recall']}, {row_dict['f1_score']},
                  'BOB', {topic_to_dataset[row_dict['topic']]},
                  't', 't', 'f')"""
        print(query)
        dev_db.execute(text(query))

    dev_db.commit()
    add_sequence_data(dev_db)

    dev_db.commit()


if __name__ == "__main__":
    main()