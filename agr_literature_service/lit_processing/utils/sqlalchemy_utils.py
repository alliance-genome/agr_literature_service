from agr_literature_service.lit_processing.utils.generic_utils import split_identifier

from os import environ

from agr_literature_service.api.models import ReferenceModel, CrossReferenceModel, ResourceModel

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def create_postgres_engine(verbose):

    """Connect to database."""
    USER = environ.get('PSQL_USERNAME', 'postgres')
    PASSWORD = environ.get('PSQL_PASSWORD', 'postgres')
    SERVER = environ.get('PSQL_HOST', 'localhost')
    PORT = environ.get('PSQL_PORT', '5432')
    DB = environ.get('PSQL_DATABASE', 'literature')

    # Create our SQL Alchemy engine from our environmental variables.
    engine_var = 'postgresql://' + USER + ":" + PASSWORD + '@' + SERVER + ':' + PORT + '/' + DB
    # future=True is recommended for 2.0-style behavior
    engine = create_engine(engine_var, future=True)
    if verbose:
        print('Using server: {}'.format(SERVER))
        print('Using database: {}'.format(DB))
        print(engine_var)

    return engine


def create_postgres_session(verbose):

    engine = create_postgres_engine(verbose)

    # SQLAlchemy 2.0 recommends using 'autocommit=False' explicitly in sessionmaker
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session = Session()

    return session


def sqlalchemy_load_ref_xref(datatype):
    ref_xref_valid = dict()
    ref_xref_obsolete = dict()
    xref_ref = dict()
    # db_session = next(get_db())
    db_session = create_postgres_session(False)

    query = None
    if datatype == 'reference':
        # 14 seconds to load all xref through sqlalchemy
        query = db_session.query(
            ReferenceModel.curie,
            CrossReferenceModel.curie,
            CrossReferenceModel.is_obsolete
        ).join(
            ReferenceModel.cross_reference
        ).filter(
            CrossReferenceModel.reference_id.isnot(None)
        )

    elif datatype == 'resource':
        query = db_session.query(
            ResourceModel.curie,
            CrossReferenceModel.curie,
            CrossReferenceModel.is_obsolete
        ).join(
            ResourceModel.cross_reference
        ).filter(
            CrossReferenceModel.resource_id.isnot(None)
        )

    if query is not None:
        results = query.all()
        for result in results:
            # print(result)
            agr = result[0]
            xref = result[1].replace("DOI:10.1037//", "DOI:10.1037/")
            is_obsolete = result[2]
            prefix, identifier, separator = split_identifier(xref, True)
            if is_obsolete is False:
                if agr not in ref_xref_valid:
                    ref_xref_valid[agr] = dict()
                ref_xref_valid[agr][prefix] = identifier
                if prefix not in xref_ref:
                    xref_ref[prefix] = dict()
                if identifier not in xref_ref[prefix]:
                    xref_ref[prefix][identifier] = agr
            else:
                if agr not in ref_xref_obsolete:
                    ref_xref_obsolete[agr] = dict()
                # a reference and prefix can still have multiple obsolete values
                if prefix not in ref_xref_obsolete[agr]:
                    ref_xref_obsolete[agr][prefix] = set()
                if identifier not in ref_xref_obsolete[agr][prefix]:
                    ref_xref_obsolete[agr][prefix].add(identifier.lower())

    return xref_ref, ref_xref_valid, ref_xref_obsolete
