from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from os import environ

from agr_literature_service.api.models import (
    AuthorModel,
    ModModel,
    UserModel,
    ReferenceModel,
    ResourceModel
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

    # Uncommnet if not starting from a fesh db
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

    refs = db_orig_session.query(ReferenceModel).filter(ReferenceModel.reference_id <= num_of_ref)
    for ref in refs:
        print(f"Adding {ref}")
        db_subset_session.merge(ref)
    print("Be patient the commit can take a wee while.")
    db_subset_session.commit()

start()