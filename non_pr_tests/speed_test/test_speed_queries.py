##############################################################################
# Speed tests.
#
# add python path to fins literature stuff.
# PYTHONPATH=~/alliance/agr_literature_service/backend/app
#
#
# create database if it does not exist or to remove postgres caching.
# pg_restore --clean --if-exists -d literature -h postgres -U postgres -p 5432 < literature-4003.pg.dump.20211118
#
# start the app so we can curl:-
# python3 backend/app/main.py
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

from os import environ, path

from curl_call import use_curl
from direct_method_call import use_method
from sqlalchemy_call import use_alchemy, batch_alchemy
from direct_sql_call import sql_direct, batch_sql_direct

verbose = False
api_port = environ.get('API_PORT', '8080')
api_server = environ.get('API_SERVER', 'localhost')
file_name = "./sample_curies.txt"

# increase each test run else the references may be cached which wiil
# skew results.
start_index = 350
MAX_SAMPLE_SIZE = 100


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
    print("Read {} curies from file.".format(count))
    return curies


curies = load_agr_curies()

# Use the url to curl no conversion of json
start_time = datetime.datetime.now()
start_index = use_curl(curies, MAX_SAMPLE_SIZE, translate=False, count_start=start_index, verbose=verbose)
end_time = datetime.datetime.now()
diff = end_time - start_time
print("Time to curl (Translate=F) {} entries was  {}".format(MAX_SAMPLE_SIZE, diff))

# use curl
start_time = datetime.datetime.now()
start_index = use_curl(curies, MAX_SAMPLE_SIZE, translate=True, count_start=start_index, verbose=verbose)
end_time = datetime.datetime.now()
diff = end_time - start_time
print("Time to (Translate=T) curl {} entries was  {}".format(MAX_SAMPLE_SIZE, diff))


start_time = datetime.datetime.now()
start_index = use_alchemy(curies, MAX_SAMPLE_SIZE, count_start=start_index, verbose=verbose)
end_time = datetime.datetime.now()
diff = end_time - start_time
print("Time to sqlalchemy {} entries was  {}".format(MAX_SAMPLE_SIZE, diff))

start_time = datetime.datetime.now()
start_index = use_method(curies, MAX_SAMPLE_SIZE, count_start=start_index, verbose=verbose)
end_time = datetime.datetime.now()
diff = end_time - start_time
print("Time to reference_crud.show {} entries was  {}".format(MAX_SAMPLE_SIZE, diff))

start_time = datetime.datetime.now()
start_index = sql_direct(curies, MAX_SAMPLE_SIZE, count_start=start_index, verbose=verbose)
end_time = datetime.datetime.now()
diff = end_time - start_time
print("Time to direct sql {} entries was  {}".format(MAX_SAMPLE_SIZE, diff))

# curl and direct method call have no batch so lets just do sqlalchemy and direct sql.

for batch_size in [10, 100, 500, 1000, 10000, 100000]:
    start_time = datetime.datetime.now()
    start_index = batch_alchemy(curies, batch_size, count_start=start_index, verbose=verbose)
    end_time = datetime.datetime.now()
    diff = end_time - start_time
    print("Time for BATCH alchemy with {} entries was  {}".format(batch_size, diff))

    start_time = datetime.datetime.now()
    start_index = batch_sql_direct(curies, batch_size, count_start=start_index, verbose=verbose)
    end_time = datetime.datetime.now()
    diff = end_time - start_time
    print("Time for BATCH direct sql with {} entries was  {}".format(batch_size, diff))
