from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, subqueryload
from os import environ

from agr_literature_service.api.models import (
    ModModel,
    UserModel,
    ReferenceModel,
    ResourceModel,
    CopyrightLicenseModel, ModReferencetypeAssociationModel,
    ReferencetypeModel
)

num_of_ref = 2000
orig_db = 'literature-test'
subset_db = 'literature_subset'


def create_postgres_engine(verbose, db):

    """Connect to database."""
    USER = environ.get('PSQL_USERNAME', 'postgres')
    PASSWORD = environ.get('PSQL_PASSWORD', 'postgres')
    SERVER = environ.get('PSQL_HOST', 'localhost')
    PORT = environ.get('PSQL_PORT', '5433')

    DB = db

    # Create our SQL Alchemy engine from our environmental variables.
    engine_var = 'postgresql://' + USER + ":" + PASSWORD + '@' + SERVER + ':' + PORT + '/' + DB
    engine = create_engine(engine_var)
    if True:
        print('Using server: {}'.format(SERVER))
        print('Using database: {}'.format(DB))
        print(engine_var)

    return engine


def create_postgres_session(verbose, db):

    engine = create_postgres_engine(verbose, db)

    # Session = sessionmaker(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()

    return session


def start():
    db_orig_session = create_postgres_session(False, orig_db)

    db_subset_session = create_postgres_session(False, subset_db)

    # Uncomment if not starting from a fresh db
    # db_subset_session.execute("DELETE FROM cross_reference")
    # db_subset_session.execute("DELETE FROM resource")
    # db_subset_session.execute("DELETE FROM reference")
    # db_subset_session.execute("DELETE FROM users")
    # db_subset_session.execute("DELETE FROM mod")

    users = db_orig_session.query(UserModel)
    for user in users:
        # db_subset_session.add(user)
        print(f"Adding user {user.id} {user.email}")
        db_subset_session.merge(user)
    db_subset_session.commit()
    db_subset_session.close()

    mods = db_orig_session.query(ModModel)
    for mod in mods:
        print(f"Adding mod {mod}")
        db_subset_session.merge(mod)
    db_subset_session.commit()
    db_subset_session.close()

    db_subset_session = create_postgres_session(False, subset_db)

    resources = db_orig_session.query(ResourceModel).join(ReferenceModel).filter(ReferenceModel.reference_id <= num_of_ref)
    for res in resources:
        print(f"Adding {res}")
        db_subset_session.merge(res)
    db_subset_session.commit()

    db_subset_session.close()

    copyrightlicenses = db_orig_session.query(CopyrightLicenseModel).join(ReferenceModel).filter(
        ReferenceModel.reference_id <= num_of_ref)
    for copy in copyrightlicenses:
        print(f"Adding {copy}")
        db_subset_session.merge(copy)
    db_subset_session.commit()
    db_subset_session.close()

    referencetypes = db_orig_session.query(ReferencetypeModel).all()
    for referencetype in referencetypes:
        print(f"Adding {referencetype}")
        db_subset_session.merge(referencetype)
    db_subset_session.commit()
    db_subset_session.close()

    mod_referencetypes = db_orig_session.query(ModReferencetypeAssociationModel).all()
    for mod_referencetype in mod_referencetypes:
        print(f"Adding {mod_referencetype}")
        db_subset_session.merge(mod_referencetype)
    db_subset_session.commit()
    db_subset_session.close()

    refs = db_orig_session.query(ReferenceModel).options(
        subqueryload(ReferenceModel.cross_reference),
        subqueryload(ReferenceModel.obsolete_reference),
        subqueryload(ReferenceModel.mod_referencetypes),
        subqueryload(ReferenceModel.mod_corpus_association),
        subqueryload(ReferenceModel.mesh_term),
        subqueryload(ReferenceModel.author),
        subqueryload(ReferenceModel.referencefiles),
        subqueryload(ReferenceModel.topic_entity_tags),
        subqueryload(ReferenceModel.workflow_tag)
    ).filter(ReferenceModel.reference_id <= num_of_ref)
    for ref in refs:
        print(f"Adding {ref}")
        db_subset_session.merge(ref)
    print("Be patient the commit can take a wee while.")
    db_subset_session.commit()

    # Need to set the initial seq values else we cannot add any more entries,
    # which might be needed for testing the api.
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
        "topic_entity_tag_prop_topic_entity_tag_prop_id_seq",
        "topic_entity_tag_topic_entity_tag_id_seq",
        "transaction_id_seq",
        "workflow_tag_reference_workflow_tag_id_seq",
        "copyright_license_copyright_license_id_seq"
    ]
    for seq in seq_list:
        com = f"ALTER SEQUENCE {seq} RESTART WITH {num_of_ref+1000}"
        db_subset_session.execute(com)
    db_subset_session.commit()


start()
