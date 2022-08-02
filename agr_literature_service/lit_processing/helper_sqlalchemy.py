from agr_literature_service.lit_processing.helper_file_processing import split_identifier
from os import environ
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
    engine = create_engine(engine_var)
    if verbose:
        print('Using server: {}'.format(SERVER))
        print('Using database: {}'.format(DB))
        print(engine_var)

    return engine


def create_postgres_session(verbose):

    engine = create_postgres_engine(verbose)

    Session = sessionmaker(bind=engine)
    session = Session()

    return session


def sqlalchemy_load_ref_xref(datatype, mod=None):
    ref_xref_valid = dict()
    ref_xref_obsolete = dict()
    xref_ref = dict()

    engine = create_postgres_engine(False)
    db_connection = engine.connect()

    rows = None
    if datatype == 'reference':
        if mod is not None:
            rs = db_connection.execute("SELECT r.curie, cr.curie, cr.is_obsolete FROM reference r, cross_reference cr, mod_corpus_association mca, mod m WHERE r.reference_id = cr.reference_id and r.reference_id = mca.reference_id and mca.mod_id = m.mod_id and m.abbreviation = '" + mod + "'")

            rows = rs.fetchall()
        else:
            rs = db_connection.execute("SELECT r.curie, cr.curie, cr.is_obsolete FROM reference r, cross_reference cr WHERE r.reference_id = cr.reference_id")

            rows = rs.fetchall()

    elif datatype == 'resource':
        rs = db_connection.execute("SELECT r.curie, cr.curie, cr.is_obsolete FROM resource r, cross_reference cr WHERE r.resource_id = cr.resource_id")

        rows = rs.fetchall()

    if rows is not None:

        for result in rows:

            agr = result[0]
            xref = result[1]
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
