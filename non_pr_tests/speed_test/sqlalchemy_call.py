from os import environ
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from literature.models import ReferenceModel


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


def use_alchemy(curies, max_number, count_start=0, verbose=False):
    count = 0
    session = create_postgres_session(verbose)
    while(count <= max_number):
        reference = session.query(ReferenceModel).\
            filter(ReferenceModel.curie == curies[count + count_start]).one_or_none()
        if verbose:
            if count <= 5:
                print(reference.curie)
        count += 1
    session.close()
    return count + count_start


def batch_alchemy(curies, batch_size, count_start=0, verbose=False):
    session = create_postgres_session(verbose)
    batch_list = curies[count_start:(count_start + batch_size)]
    refs = session.query(ReferenceModel).\
        filter(ReferenceModel.curie.in_(batch_list)).all()
    session.close()
    # Make sure we have all the data, Store in a dict similare to 
    # what would be used in the code.
    new_dict = {item.curie: item for item in refs}
    # print("dict type is {}".format(type(new_dict)))
    # del refs
    if verbose:
        # print(batch_list[:5])
        for agr in batch_list[:5]:
            print(new_dict[agr])
        for agr in batch_list[:-5]:
            print(new_dict[agr])

    return count_start + batch_size
