from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, subqueryload
from sqlalchemy import func
from os import environ, system
from typing import List

import argparse
from random import randint

from agr_literature_service.api.models import (
    UserModel,
    ModModel,
    ReferenceModel,
    ResourceModel,
    ReferencetypeModel,
    ModReferencetypeAssociationModel,
    CopyrightLicenseModel,
    TopicEntityTagSourceModel,
    WorkflowTransitionModel,
    DatasetModel,
    MLModel
)

# NOTE: Order is important here.
# i.e. ModModel have to be loaded before ModReferencetypeAssociationModel
# as it references it.
data_needed_before_reference = [
    UserModel,
    ModModel,
    ReferencetypeModel,
    ModReferencetypeAssociationModel,
    CopyrightLicenseModel,
    TopicEntityTagSourceModel,
    WorkflowTransitionModel,
    DatasetModel,
    MLModel
]

# curies in curie order. Please retain this order to
# make them easier to find.
test_reference_curies = [
    'AGRKB:101000000656561',
    'AGRKB:101000000650977',
    'AGRKB:101000000661443',
    'AGRKB:101000000679676',
    'AGRKB:101000000654341',
    'AGRKB:101000000589922',
    'AGRKB:101000000863609',
    'AGRKB:101000000946299',
    'AGRKB:101000000594062',
    'AGRKB:101000000872479',
    'AGRKB:101000000865324',
    'AGRKB:101000000255429',  # needed to test merges of papers
    'AGRKB:101000000390275',
    'AGRKB:101000000466335',
    'AGRKB:101000000014457'   # end of merge test examples
]

trigger_list = ['reference', 'resource', 'author', 'cross_reference']

ALL_OUTPUT = 2  # For verbosity

helptext = r"""
    env PSQL_XXX used for Target subset db details.
    env ORIG_XXX used for source reference db details.
    If ORIG_XXX is defined use that for source db details.
    If ORIG_XXX NOT defined then PSQL_XXX details will be used.
    In most cases ORIG_XXX will not be needed but this is incase
    the target and source databases are not on the same server.

"""
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description=helptext)
parser.add_argument('-l', '--limit', help='Limit to x references', type=int, default=2000, required=False)
parser.add_argument('-v', '--verbose', help="verbosity, minumum=0, headlines=1, full=2,", type=int, default=1, required=False)
parser.add_argument('-d', '--dump_dir', help='Directory to dump schemas and dbs to', type=str, required=True)
parser.add_argument('-f', '--full_orig_dump', help='Dump the original database in full as well', type=bool, default=False, required=False)
parser.add_argument('-s', '--subset_dump', help='Dump the new subset database.', type=bool, default=False, required=False)
parser.add_argument('-r', '--randomise', help='Randomise the references added', type=bool, default=False, required=False)
parser.add_argument('-n', '--name_subset', help='Set the new subset database name.', type=str, default="literature_subset", required=False)
args = parser.parse_args()

num_of_refs = args.limit
verbose = args.verbose
dump_dir = args.dump_dir
full_orig_dump = args.full_orig_dump
subset_dump = args.subset_dump
randomise = args.randomise
subset_name = args.name_subset

ref_ids: List = []


def dump_schema(user, password, server, port, db):
    global dump_dir, full_orig_dump, verbose
    if verbose:
        print(f"Dumping schema for {db} in {dump_dir} for server {server}")

    # Dump full original database if requested.
    filename = f"{dump_dir}/{server}-literature.sql"
    if full_orig_dump:
        com = f"pg_dump -Fc --clean -n public -h {server} -p {port} -U {user} -W {db} > {filename}"
        print(com)
        system(f"PGPASSWORD={password} {com}")

    # Dump schema for original database.
    filename = f"{dump_dir}/literature_schema.sql"
    com = f"pg_dump -Fc --schema-only --clean -n public -h {server} -p {port} -U {user} -d {db} > {filename}"
    print(com)
    system(f"PGPASSWORD={password} {com}")


def load_schema(user, password, server, port, db):
    global dump_dir, verbose
    if verbose:
        print(f"Deleting old db {db} on server {server}")
    com = f'psql -h {server} -p {port} -U {user} -c "DROP DATABASE IF EXISTS {db};"'
    if verbose:
        print(com)
    system(f"PGPASSWORD={password} {com}")

    if verbose:
        print(f"Create new database {db} on server {server}")
    com = f'psql -h {server} -p {port} -U {user} -c "CREATE DATABASE {db};"'
    if verbose:
        print(com)
    system(f"PGPASSWORD={password} {com}")

    if verbose:
        print(f"Load schema for {db}")
    filename = f"{dump_dir}/literature_schema.sql"
    com = f"pg_restore -h {server} -n public -U {user} -p {port} -d {db} < {filename}"
    if verbose:
        print(com)
    system(f"PGPASSWORD={password} {com}")


def dump_subset():
    global verbose, dump_dir
    user = environ.get('PSQL_USERNAME', 'postgres')
    password = environ.get('PSQL_PASSWORD', 'postgres')
    server = environ.get('PSQL_HOST', 'localhost')
    port = environ.get('PSQL_PORT', '5432')
    db = subset_name
    if verbose:
        print(f"Dumping database {db} to {dump_dir}")
    filename = f"{dump_dir}/literature_subset.sql"
    com = f"pg_dump -Fc --clean -n public -h {server} -p {port} -U {user} -d {db} > {filename}"
    if verbose:
        print(com)
    system(f"PGPASSWORD={password} {com}")


def create_postgres_engine(source=False):
    global verbose
    """Connect to database.
    If ORIG_XXX is defined use that for source db details.
    If ORIG_XXX not defined then use PSQL_XXX.
    In most cases ORIG_XXX will not be needed but this is incase
    the target and source databases are not on the same server.
    """
    db_type = "Source"
    if source:
        USER = environ.get('ORIG_USERNAME', None)
        if not USER:
            USER = environ.get('PSQL_USERNAME', 'postgres')
        PASSWORD = environ.get('ORIG_PASSWORD', None)
        if not PASSWORD:
            PASSWORD = environ.get('PSQL_PASSWORD', 'postgres')
        SERVER = environ.get('ORIG_HOST', None)
        if not SERVER:
            SERVER = environ.get('PSQL_HOST', 'localhost')
        PORT = environ.get('ORIG_PORT', None)
        if not PORT:
            PORT = environ.get('PSQL_PORT', '5432')
        DB = environ.get('ORIG_DATABASE', 'literature')
        dump_schema(USER, PASSWORD, SERVER, PORT, DB)
    else:
        USER = environ.get('PSQL_USERNAME', 'postgres')
        PASSWORD = environ.get('PSQL_PASSWORD', 'postgres')
        SERVER = environ.get('PSQL_HOST', 'localhost')
        PORT = environ.get('PSQL_PORT', '5432')
        DB = environ.get('PSQL_DATABASE', 'literature_subset')
        db_type = "Target"
        load_schema(USER, PASSWORD, SERVER, PORT, DB)

    # Create our SQL Alchemy engine from our environmental variables.
    engine_var = 'postgresql://' + USER + ":" + PASSWORD + '@' + SERVER + ':' + PORT + '/' + DB

    if verbose:
        print(f"SQL {db_type}: {engine_var}")
    engine = create_engine(engine_var)

    return engine


def create_postgres_session(source=False):

    engine = create_postgres_engine(source)

    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()

    return session


def get_ref_ids(db_orig_session):
    global num_of_refs
    if randomise:
        bob = db_orig_session.query(func.max(ReferenceModel.reference_id)).scalar()
        count = 0
        ref_list = []
        max_ref = int(bob)
        if verbose:
            print(f"MAX ref_id is: {max_ref}")
        while count < num_of_refs:
            count += 1
            ref_rand = randint(1, bob)
            ref_list.append(ref_rand)
        if verbose:
            print(ref_list)
        return ref_list
    else:
        return range(1, num_of_refs)


def add_references(db_orig_session, db_subset_session):
    global verbose
    global ref_ids
    if verbose:
        print(f"Adding {num_of_refs} references")
    refs = db_orig_session.query(ReferenceModel).options(
        subqueryload(ReferenceModel.cross_reference),
        subqueryload(ReferenceModel.obsolete_reference),
        subqueryload(ReferenceModel.mod_referencetypes),
        subqueryload(ReferenceModel.mod_corpus_association),
        subqueryload(ReferenceModel.mesh_term),
        subqueryload(ReferenceModel.author),
        subqueryload(ReferenceModel.referencefiles),
        subqueryload(ReferenceModel.topic_entity_tags),
        subqueryload(ReferenceModel.workflow_tag),
        subqueryload(ReferenceModel.citation)
    ).filter(ReferenceModel.reference_id.in_(ref_ids))
    count = 0
    for ref in refs:
        if verbose == ALL_OUTPUT:
            print(f"Adding {ref.curie} {ref.reference_id}")
        db_subset_session.merge(ref)
        count += 1
        print(f"Adding reference {count}..")
        if count % 50 == 0:
            db_subset_session.commit()
    if verbose:
        print(f"Added {count} records for References.")
    return count


def add_specific_test_references(db_orig_session, db_subset_session):
    global verbose, test_reference_curies

    if verbose:
        print("Adding SET test references")
    refs = db_orig_session.query(ReferenceModel).options(
        subqueryload(ReferenceModel.cross_reference),
        subqueryload(ReferenceModel.obsolete_reference),
        subqueryload(ReferenceModel.mod_referencetypes),
        subqueryload(ReferenceModel.mod_corpus_association),
        subqueryload(ReferenceModel.mesh_term),
        subqueryload(ReferenceModel.author),
        subqueryload(ReferenceModel.referencefiles),
        subqueryload(ReferenceModel.topic_entity_tags),
        subqueryload(ReferenceModel.workflow_tag),
        subqueryload(ReferenceModel.citation)
    ).filter(ReferenceModel.curie.in_(test_reference_curies))
    count = 0
    for ref in refs:
        if verbose == ALL_OUTPUT:
            print(f"Adding {ref.curie} {ref.reference_id}")
        db_subset_session.merge(ref)
        count += 1
    if verbose:
        print(f"Added {count} records for Test Set references.")


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


def add_alembic(db_orig_session, db_subset_session):
    # Add alembic_version
    alembic_rows = db_orig_session.execute(text("SELECT version_num from alembic_version"))
    version = ''
    for alembic_row in alembic_rows:
        version = alembic_row[0]
    if not version:
        print("ERROR: Could not find version_num for alembic_version table")
    else:
        db_subset_session.execute(text(f"INSERT into alembic_version (version_num) VALUES ('{version}');"))
    db_subset_session.commit()
    db_subset_session.close()


def trigger_settings(db_session, state="DISABLE"):
    global trigger_list
    for table in trigger_list:
        db_session.execute(text(f'ALTER TABLE {table} {state} TRIGGER all;'))
    db_session.commit()


def add_full_table_data(db_orig_session, db_subset_session, model):
    records = db_orig_session.query(model)
    count = 0
    for record in records:
        count += 1
        if verbose == ALL_OUTPUT:
            print(f"Adding record {record}")
        db_subset_session.merge(record)
    if verbose:
        print(f"Added {count} records for {model.__name__}.")
    db_subset_session.commit()
    db_subset_session.close()


def load_data_needed_before_reference(db_orig_session, db_subset_session):
    global data_needed_before_reference
    for modclass in data_needed_before_reference:
        add_full_table_data(db_orig_session, db_subset_session, modclass)


def load_resources(db_orig_session, db_subset_session):
    """
    Too many resources to load so only load those needed by
    filtering on the reference_id.
    Note: no commit here, we load the references first as
          resources may take a little while too.
    """
    global test_reference_curies
    global ref_ids
    resources = db_orig_session.query(ResourceModel).join(ReferenceModel).filter(ReferenceModel.reference_id.in_(ref_ids))
    count = 0
    loaded = []
    for res in resources:
        count += 1
        if verbose == ALL_OUTPUT:
            print(f"Adding {res}")
        db_subset_session.merge(res)
        loaded.append(res.resource_id)

    # We also need to load the resources for the test set
    ress = db_orig_session.query(ResourceModel).join(ReferenceModel)\
        .filter(ReferenceModel.curie.in_(test_reference_curies))
    for res in ress:
        if verbose == ALL_OUTPUT:
            print(f"Adding {res}")
        if res.resource_id not in loaded:
            db_subset_session.merge(res)
            count += 1
    if verbose:
        print(f"Added {count} Resources")


def start():
    global test_reference_curies
    global ref_ids
    db_orig_session = create_postgres_session(source=True)
    ref_ids = get_ref_ids(db_orig_session)
    db_subset_session = create_postgres_session()

    # remove the triggers while loading.
    trigger_settings(db_subset_session, state="DISABLE")

    load_data_needed_before_reference(db_orig_session, db_subset_session)

    load_resources(db_orig_session, db_subset_session)

    # add the references, one set by a count and other from preset list.
    ref_count = add_references(db_orig_session, db_subset_session)
    add_specific_test_references(db_orig_session, db_subset_session)
    print("Be patient the commit can take a wee while.")
    db_subset_session.commit()

    # Add alembic_version
    add_alembic(db_orig_session, db_subset_session)

    # Sanity checks
    okay = True
    tables = ['reference', 'citation']  # add other tables?
    for table_name in tables:
        count_rows = db_subset_session.execute(text(f"SELECT count(1) from {table_name}"))
        count = 0
        for count_row in count_rows:
            count = count_row[0]
        if not count:
            print(f"ERROR: No records found for table  {table_name}")
            okay = False
        theoretical_count = len(test_reference_curies) + ref_count
        if count != theoretical_count:
            print(f"ERROR: {count} records found for table  {table_name} but was expecting {theoretical_count}")
            okay = False

    # Add the triggers back.
    trigger_settings(db_subset_session, state="ENABLE")

    # add sequence data. What value to use next in a sequence
    add_sequence_data(db_subset_session)

    # for what ever reason need this:
    db_subset_session.execute(text("REFRESH MATERIALIZED VIEW _view"))
    db_subset_session.commit()
    if subset_dump:
        dump_subset()

    if not okay:
        exit(-1)


start()
