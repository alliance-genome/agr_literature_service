from os import environ
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from agr_literature_service.api.crud import reference_crud
from agr_literature_service.api.models import ReferenceModel
import datetime


def create_postgres_session():
    """Connect to database."""
    USER = environ.get('PSQL_USERNAME', 'postgres')
    PASSWORD = environ.get('PSQL_PASSWORD', 'postgres')
    SERVER = environ.get('PSQL_HOST', 'localhost')
    try:
        PORT = environ.get('PSQL_PORT', 5432)
    except KeyError:
        PORT = '5432'

    DB = environ.get('PSQL_DATABASE', 'literature')

    # Create our SQL Alchemy engine from our environmental variables.
    engine_var = 'postgresql://' + USER + ":" + PASSWORD + '@' + SERVER + ':' + PORT + '/' + DB
    engine = create_engine(engine_var)

    Session = sessionmaker(bind=engine)
    session = Session()

    return session


def main():
    db = create_postgres_session()
    print("Starting {}".format(datetime.datetime.now()))
    outfile = open("citation_update.txt", 'a')
    seen = {}
    i = 0
    last_seen = 0
    offset = 0
    batch_size = 100

    descending = db.query(ReferenceModel).order_by(ReferenceModel.reference_id.desc())
    last_ref = descending.first()
    print("Last referecne_id is : {}".format(last_ref.reference_id))
    okay = True
    while last_seen <= last_ref.reference_id and okay:
        ref_query = db.query(ReferenceModel).order_by(ReferenceModel.reference_id).offset(offset).limit(batch_size).all()
        okay = False
        for reference in ref_query:
            citation = reference_crud.get_citation_from_obj(db, reference)
            # Double quote the quotes else won't load.
            citation = citation.replace("'", "''")
            sql_string = 'UPDATE reference SET citation = \'{}\' WHERE reference_id = {};\n'.\
                         format(citation, reference.reference_id)
            outfile.write(sql_string)
            if reference.curie in seen:
                print("{} Done already??".format(reference.curie))
                exit(-1)
            seen[reference.curie] = 1
            offset = offset + 1
            i = i + 1
            if i > batch_size:
                i = 0
                print("{} {}".format(last_seen, datetime.datetime.now()))
            last_seen = reference.reference_id
            okay = True
    print("Last reference processed was {}".format(last_seen))


# if __name__ == "main":
main()
