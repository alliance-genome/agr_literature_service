import psycopg2
from os import environ


def establish_db_connection():
    """Establishes the database connection.
       Required libraries: psycopg2, datetime.
       Output: a database connection."""
    USER = environ.get('PSQL_USERNAME', 'postgres')
    PASSWORD = environ.get('PSQL_PASSWORD', 'postgres')
    SERVER = environ.get('HOST', 'localhost')
    # PORT = environ.get('PSQL_PORT', '5432')
    DB = environ.get('PSQL_DATABASE', 'literature')
    conn_string = "host=%s dbname=%s user=%s password='%s'" % (SERVER, DB, USER, PASSWORD)
    db_connection = psycopg2.connect(conn_string)

    return db_connection


def sql_direct(curies, max_number, count_start=0, verbose=False):
    count = 0
    conn = establish_db_connection()
    cursor = conn.cursor()
    query = ('SELECT r.* '
             'from public.references r '
             'where r.curie = %s')
    while(count <= max_number):
        curie = curies[count + count_start]
        cursor.execute(query, (curie,))
        ref = cursor.fetchall()
        if verbose:
            if count <= 5:
                print(ref)
        count += 1
    cursor.close()
    conn.close()
    return count_start + count


def batch_sql_direct(curies, batch_size, count_start=0, verbose=False):
    batch_list = curies[count_start:(count_start + batch_size)]
    conn = establish_db_connection()
    cursor = conn.cursor()
    list_str = "'" + "', '".join(batch_list) + "'"
    query = ('SELECT r.* '
             'from public.references r '
             'where r.curie in ({})').format(list_str)
    list_str = "'" + "', '".join(batch_list) + "'"

    cursor.execute(query)
    refs = cursor.fetchall()

    # Make sure we have all the data, Store in a dict similare to
    # what would be used in the code.
    # print(refs[0])
    new_dict = {item[1]: item for item in refs}

    cursor.close()
    conn.close()
    if verbose:
        for agr in batch_list[:5]:
            print(new_dict[agr])
        for agr in batch_list[:-5]:
            print(new_dict[agr])

    return count_start + batch_size
