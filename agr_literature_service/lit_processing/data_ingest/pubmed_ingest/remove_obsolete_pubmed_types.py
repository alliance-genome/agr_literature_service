import logging
from sqlalchemy import text
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def cleanup_data():  # pragma: no cover

    db = create_postgres_session(False)

    logger.info("Querying cross_reference table...")

    sql_query = text("""
    SELECT reference_id, is_obsolete
    FROM cross_reference
    WHERE curie_prefix = 'PMID'
    """)

    rows = db.execute(sql_query).fetchall()
    reference_id_to_is_obsolete = {row[0]: row[1] for row in rows}

    logger.info("Querying reference table and deleting obsolete pubmed_types...")

    sql_query = text("""
    SELECT reference_id, curie, title, pubmed_types
    FROM reference
    WHERE pubmed_types::text != '{}'
    AND pubmed_types is not NULL
    """)

    rows = db.execute(sql_query).fetchall()
    row_count = 0
    for row in rows:
        reference_id = row[0]
        curie = row[1]
        title = row[2]
        pubmed_types = row[3]
        if reference_id not in reference_id_to_is_obsolete:
            logger.info(f"reference_id = {reference_id} has no PMID. {curie} {pubmed_types}")
            cleanup_this_paper(db, reference_id, curie, title)
        elif reference_id_to_is_obsolete[row[0]] is True:
            logger.info(f"reference_id = {reference_id} has an obsolete PMID. {curie} {pubmed_types}")
            remove_pubmed_types(db, reference_id, curie)
        row_count += 1
        if row_count % 100:
            # db.rollback()
            db.commit()

    # db.rollback()
    db.commit()
    db.close()


def cleanup_this_paper(db, reference_id, curie, title):

    sql_query = text("""
    SELECT mod_id
    FROM mod_corpus_association
    WHERE reference_id = :reference_id
    AND corpus = TRUE
    """)

    rows = db.execute(sql_query, {"reference_id": reference_id}).fetchall()

    if len(rows) > 0:
        return

    sql_query = text("""
    SELECT reference_id
    FROM reference
    WHERE UPPER(title) = :title
    """)

    rows = db.execute(sql_query, {"title": title.upper()}).fetchall()

    if len(rows) > 1:
        sql_query = text("""
        DELETE from reference
        WHERE reference_id = :reference_id
        """)
        db.execute(sql_query, {"reference_id": reference_id})
        logger.info(f"The reference for reference_id = {reference_id}, curie={curie} has been deleted.")


def remove_pubmed_types(db, reference_id, curie):

    sql_query = text("""
    UPDATE reference
    SET pubmed_types = NULL
    WHERE reference_id = :reference_id
    """)

    db.execute(sql_query, {
        "reference_id": reference_id
    })

    logger.info(f"The reference pubmed_types for reference_id = {reference_id}, curie={curie} has been set to NULL.")


if __name__ == "__main__":

    cleanup_data()
