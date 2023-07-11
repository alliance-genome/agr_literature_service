from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, subqueryload
from os import environ, system

import argparse

from agr_literature_service.api.models import (
    UserModel,
    ModModel,
    ReferenceModel,
    ResourceModel,
    ReferencetypeModel,
    ModReferencetypeAssociationModel,
    CopyrightLicenseModel

)

# NOTE: Order is important here.
# i.e. ModModel have to be loaded before ModReferencetypeAssociationModel
# as it references it.
data_needed_before_reference = [
    UserModel,
    ModModel,
    ReferencetypeModel,
    ModReferencetypeAssociationModel,
    CopyrightLicenseModel
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

seq_list = [
    "author_author_id_seq",
    "cross_reference_id_seq",
    "editor_editor_id_seq",
    "mesh_detail_mesh_detail_id_seq",
    "mod_corpus_association_mod_corpus_association_id_seq",
    "mod_mod_id_seq",
    "mod_referencetype_mod_referencetype_id_seq",
    "mod_taxon_mod_taxon_id_seq",
    "obsolete_reference_curie_obsolete_id_seq",
    "reference_comments_and_correc_reference_comment_and_correct_seq",
    "reference_mod_md5sum_reference_mod_md5sum_id_seq",
    "reference_mod_referencetype_reference_mod_referencetype_id_seq",
    "reference_reference_id_seq",
    "referencefile_mod_referencefile_mod_id_seq",
    "referencefile_referencefile_id_seq",
    "referencetype_referencetype_id_seq",
    "resource_descriptor_pages_resource_descriptor_pages_id_seq",
    "resource_descriptors_resource_descriptor_id_seq",
    "resource_resource_id_seq",
    "topic_entity_tag_topic_entity_tag_id_seq",
    "transaction_id_seq",
    "workflow_tag_reference_workflow_tag_id_seq",
    "copyright_license_copyright_license_id_seq",
    "citation_citation_id_seq"
]

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
args = parser.parse_args()

num_of_refs = args.limit
verbose = args.verbose
dump_dir = args.dump_dir
full_orig_dump = args.full_orig_dump
subset_dump = args.subset_dump


def dump_schema(verbose, user, password, server, port, db):
    global dump_dir, full_orig_dump
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


def load_schema(verbose, user, password, server, port, db):
    global dump_dir
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
    db = environ.get('PSQL_DATABASE', 'literature_subset')
    if verbose:
        print(f"Dumping database {db} to {dump_dir}")
    filename = f"{dump_dir}/literature_subset.sql"
    com = f"pg_dump -Fc --clean -n public -h {server} -p {port} -U {user} -d {db} > {filename}"
    if verbose:
        print(com)
    system(f"PGPASSWORD={password} {com}")


def create_postgres_engine(verbose, source=False):

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
        dump_schema(verbose, USER, PASSWORD, SERVER, PORT, DB)
    else:
        USER = environ.get('PSQL_USERNAME', 'postgres')
        PASSWORD = environ.get('PSQL_PASSWORD', 'postgres')
        SERVER = environ.get('PSQL_HOST', 'localhost')
        PORT = environ.get('PSQL_PORT', '5432')
        DB = environ.get('PSQL_DATABASE', 'literature_subset')
        db_type = "Target"
        load_schema(verbose, USER, PASSWORD, SERVER, PORT, DB)

    # Create our SQL Alchemy engine from our environmental variables.
    engine_var = 'postgresql://' + USER + ":" + PASSWORD + '@' + SERVER + ':' + PORT + '/' + DB

    if verbose:
        print(f"SQL {db_type}: {engine_var}")
    engine = create_engine(engine_var)

    return engine


def create_postgres_session(verbose, source=False):

    engine = create_postgres_engine(verbose, source)

    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()

    return session


def add_references(db_orig_session, db_subset_session):
    global verbose
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
    ).filter(ReferenceModel.reference_id <= num_of_refs)
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
    global seq_list
    for seq in seq_list:
        com = f"ALTER SEQUENCE {seq} RESTART WITH {1000000}"
        db_subset_session.execute(com)
    db_subset_session.commit()


def add_alembic(db_orig_session, db_subset_session):
    # Add alembic_version
    alembic_rows = db_orig_session.execute("SELECT version_num from alembic_version")
    version = ''
    for alembic_row in alembic_rows:
        version = alembic_row[0]
    if not version:
        print("ERROR: Could not find version_num for alembic_version table")
    else:
        db_subset_session.execute(f"INSERT into alembic_version (version_num) VALUES ('{version}');")
    db_subset_session.commit()
    db_subset_session.close()


def trigger_settings(db_session, state="DISABLE"):
    global trigger_list
    for table in trigger_list:
        db_session.execute(f'ALTER TABLE {table} {state} TRIGGER all;')
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
    resources = db_orig_session.query(ResourceModel).join(ReferenceModel).filter(ReferenceModel.reference_id <= num_of_refs)
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
    db_orig_session = create_postgres_session(True, source=True)

    db_subset_session = create_postgres_session(True)

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

    # add sequence data. What value to use next in a sequence
    add_sequence_data(db_subset_session)

    # Sanity checks
    okay = True
    tables = ['reference', 'citation']  # add other tables?
    for table_name in tables:
        count_rows = db_subset_session.execute(f"SELECT count(1) from {table_name}")
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

    # for what ever reason need this:
    db_subset_session.execute("REFRESH MATERIALIZED VIEW _view")
    db_subset_session.commit()
    if subset_dump:
        dump_subset()

    if not okay:
        exit(-1)


start()
