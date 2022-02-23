from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from os import environ

from literature.crud import reference_crud


def create_postgres_session(verbose):
    """Connect to database."""
    USER = environ.get('PSQL_USERNAME', 'postgres')
    PASSWORD = environ.get('PSQL_PASSWORD', 'postgres')
    SERVER = environ.get('HOST', 'localhost')
    PORT = environ.get('PSQL_PORT', '5432')

    DB = environ.get('PSQL_DATABASE', 'literature')

    if verbose:
        print('Using server: {}'.format(SERVER))
        print('Using database: {}'.format(DB))

    # Create our SQL Alchemy engine from our environmental variables.
    engine_var = 'postgresql://' + USER + ":" + PASSWORD + '@' + SERVER + ':' + PORT + '/' + DB
    engine = create_engine(engine_var)

    Session = sessionmaker(bind=engine)
    session = Session()

    return session


def use_method(curies, max_number, count_start=0, verbose=False):
    count = 0
    session = create_postgres_session(verbose)
    while(count <= max_number):
        ref = reference_crud.show(session, curies[count + count_start])
        if verbose:
            if count <= 5:
                print(ref["curie"])
        count += 1
    return count_start + count
