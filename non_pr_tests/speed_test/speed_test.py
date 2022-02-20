##############################################################################
# Speed tests.
#
# add python path to fins literature stuff.
# PYTHONPATH=~/alliance/agr_literature_service/backend/app
#
# Generate a sample of curies to use:-
#    psql -d literature -U postgres -d literature < curie_sample_gen.sql \
#        > sample_curies.txt
#    Edit the sample_curies.txt and remove the header and footers to leave just
#    the list of curies.
#
# Also try loading the whole set into memory - sql and pydantic
#
################################################################################
import datetime
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from os import environ, path
import requests
import json
from literature.models import ReferenceModel
from literature.crud import reference_crud

api_port = environ.get('API_PORT', '8080') 
api_server = environ.get('API_SERVER', 'localhost')
file_name = "./sample_curies.txt"

# increase each test run else the references may be cached which wiil
# skew results.
caching_avoid = 300


def create_postgres_session():
    """Connect to database."""
    USER = environ.get('PSQL_USERNAME', 'postgres')
    PASSWORD = environ.get('PSQL_PASSWORD', 'postgres')
    SERVER = environ.get('HOST', 'localhost')
    PORT = environ.get('PSQL_PORT', '5432')

    DB = environ.get('PSQL_DATABASE', 'literature')

    print('Using server: {}'.format(SERVER))
    print('Using database: {}'.format(DB))
    print(USER)
    print(PASSWORD)
    print(PORT)
    print(DB)
    # Create our SQL Alchemy engine from our environmental variables.
    engine_var = 'postgresql://' + USER + ":" + PASSWORD + '@' + SERVER + ':' + PORT + '/' + DB
    engine = create_engine(engine_var)

    Session = sessionmaker(bind=engine)
    session = Session()

    return session


def load_agr_curies():
    curies = []
    count = 0
    if path.isfile(file_name):
        with open(file_name, 'r') as read_fh:
            for line in read_fh:
                curies.append(line.strip())
                count += 1
    else:
        print("Could not load file {}".format(file_name))
    print("Read {} curies form file.".format(count))
    return curies


def use_alchemy(session, curies, max_number):
    count = 0
    while(count <= max_number):
        reference = session.query(ReferenceModel).filter(ReferenceModel.curie == curies[count + caching_avoid]).one_or_none()
        # reference_crud.show(curies[count], Depends(get_db))
        if count <= 5:
            print(reference.curie)
        count += 1


def use_method(session, curies, max_number):
    count = 0
    while(count <= max_number):
        ref = reference_crud.show(session, curies[count + caching_avoid])
        # print(dir(ref))
        if count <= 5:
            # print(ref)
            print(ref["curie"])
        count += 1


#### methods that do the work
def use_curl(curies, max_number, translate=True):

    count = 0
    while(count <= max_number):
        url = 'http://' + api_server + ':' + api_port + '/reference/' + curies[count + caching_avoid]
        # logger.info("get AGR reference info from database %s", url)
        get_return = requests.get(url)
        if translate:
            json.loads(get_return.text)
        if count < 5:
            print(url)
            # print(db_entry)
        count += 1


# change the MAX_SAMPLE_SIZE based on timings, we need a sensible amout but not too many
MAX_SAMPLE_SIZE = 10
curies = load_agr_curies()

# Use the url to curl no conversion of json
start_time = datetime.datetime.now()
use_curl(curies, MAX_SAMPLE_SIZE, translate=False)
end_time = datetime.datetime.now()
diff = end_time - start_time
print("Time to curl {} entries was  {}".format(MAX_SAMPLE_SIZE, diff))

# use curl
caching_avoid += MAX_SAMPLE_SIZE + 10
start_time = datetime.datetime.now()
use_curl(curies, MAX_SAMPLE_SIZE, translate=True)
end_time = datetime.datetime.now()
diff = end_time - start_time
print("Time to curl {} entries was  {}".format(MAX_SAMPLE_SIZE, diff))

caching_avoid += MAX_SAMPLE_SIZE + 500
session = create_postgres_session()
start_time = datetime.datetime.now()
use_alchemy(session, curies, MAX_SAMPLE_SIZE)
end_time = datetime.datetime.now()
diff = end_time - start_time
print("Time to sqlalchemy {} entries was  {}".format(MAX_SAMPLE_SIZE, diff))

caching_avoid += MAX_SAMPLE_SIZE + 500
start_time = datetime.datetime.now()
use_method(session, curies, MAX_SAMPLE_SIZE)
end_time = datetime.datetime.now()
diff = end_time - start_time
print("Time to reference_crud.show {} entries was  {}".format(MAX_SAMPLE_SIZE, diff))
