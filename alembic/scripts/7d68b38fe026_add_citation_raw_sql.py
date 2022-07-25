from agr_literature_service.lit_processing.helper_sqlalchemy import create_postgres_engine
from datetime import datetime
import re

datafile = "update_citations_from_raw_sql.txt"


def generate_all_citations():

    print(datetime.now(), " getting journal data...")

    resource_id_to_journal_name = get_journal_data()

    print(datetime.now(), " getting author data...")

    reference_id_to_authors = get_author_data()

    print(datetime.now(), " getting reference data...")

    engine = create_postgres_engine(False)
    db_connection = engine.connect()

    fw = open(datafile, "w")

    per_db_connection = 20000
    limit = 500

    i = 0
    j = 0

    for index in range(2000):

        if j > per_db_connection:
            db_connection.close()
            engine.dispose()
            engine = create_postgres_engine(False)
            db_connection = engine.connect()
            j = 0

        offset = index * limit
        print(datetime.now(), "offset = ", offset)

        rs = db_connection.execute('select reference_id, date_published, title, volume, issue_name, page_range, resource_id from reference order by reference_id limit ' + str(limit) + ' offset ' + str(offset))
        rows = rs.fetchall()
        if len(rows) == 0:
            break
        for x in rows:
            i += 1
            j += 1
            if i % 100 == 0:
                print(i, "reference:", x[0])
            reference_id = x[0]
            citation = generate_one_citation(reference_id_to_authors.get(reference_id, []),
                                             x[1], x[2], x[3], x[4], x[5],
                                             resource_id_to_journal_name.get(x[6], ''))
            citation = citation.replace("'", "''")
            sql_string = 'UPDATE reference SET citation = \'{}\' WHERE reference_id = {};\n'.\
                         format(citation, reference_id)
            fw.write(sql_string)

    fw.close()
    db_connection.close()
    engine.dispose()

    print(datetime.now(), "DONE!!")


def get_author_data():

    engine = create_postgres_engine(False)
    db_connection = engine.connect()

    reference_id_to_authors = {}

    i = 0
    author_limit = 500000
    for index in range(100):
        offset = index * author_limit
        rs = db_connection.execute('select a.reference_id, a.name from author a order by a.reference_id, a.order limit ' + str(author_limit) + ' offset ' + str(offset))
        rows = rs.fetchall()
        if len(rows) == 0:
            break
        for x in rows:
            i += 1
            if i % 1000 == 0:
                print(i, "author:", x[1])
            data = []
            reference_id = x[0]
            if reference_id in reference_id_to_authors:
                data = reference_id_to_authors[reference_id]
            if x[1]:
                data.append(x[1])
            reference_id_to_authors[reference_id] = data

    db_connection.close()
    engine.dispose()

    return reference_id_to_authors


def get_journal_data():

    engine = create_postgres_engine(False)
    db_connection = engine.connect()

    resource_id_to_journal_name = {}

    rs = db_connection.execute('select resource_id, title from resource')
    rows = rs.fetchall()
    i = 0
    for x in rows:
        i += 1
        print(i, "resource:", x[1])
        resource_id_to_journal_name[x[0]] = x[1]

    db_connection.close()
    engine.dispose()

    return resource_id_to_journal_name


def generate_one_citation(authors, date_published, title, volume, issue_name, page_range, journal):

    year = ''
    if date_published:
        year_re_result = re.search(r"(\d{4})", date_published)
        if year_re_result:
            year = year_re_result.group(1)

    title = title or ''
    if not re.search('[.]$', title):
        title = title + '.'

    authorNames = "; ".join(authors)

    citation = "{}, ({}) {} {} {} ({}): {}".\
        format(authorNames, year, title,
               journal, volume or '', issue_name or '', page_range or '')

    return citation


if __name__ == "__main__":

    generate_all_citations()
