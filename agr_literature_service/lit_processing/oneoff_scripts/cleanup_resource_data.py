import logging

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ReferenceModel, EditorModel, ResourceModel

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def fix_resource_data():

    db_session = create_postgres_session(False)

    logger.info("Getting data from the database...")

    resource_id_to_nlm = {}
    rows = db_session.execute("SELECT resource_id, curie FROM cross_reference "
                              "WHERE curie_prefix = 'NLM' and is_obsolete = False").fetchall()
    for x in rows:
        resource_id_to_nlm[x[0]] = x[1]

    duplicate_resource_id_to_correct_resource_id = get_duplicate_resources(db_session,
                                                                           resource_id_to_nlm)

    logger.info("Updating resource_ids in EDITOR table...")

    # update_editor_table(db_session, duplicate_resource_id_to_correct_resource_id)
    delete_editor_rows(db_session, duplicate_resource_id_to_correct_resource_id)

    logger.info("Updating resource_ids in REFERENCE table...")

    update_reference_table(db_session, duplicate_resource_id_to_correct_resource_id)

    logger.info("Deleting duplicate RESOURCEs...")

    remove_duplicate_ones(db_session, duplicate_resource_id_to_correct_resource_id)

    db_session.close()


def get_duplicate_resources(db_session, resource_id_to_nlm):

    key_to_resource_id_etc = {}
    duplicate_resource_id_to_correct_resource_id = {}

    rows = db_session.execute("SELECT resource_id, curie, title, iso_abbreviation, created_by "
                              "FROM resource ORDER BY resource_id").fetchall()

    for x in rows:
        resource_id = x[0]
        curie = x[1]
        title = x[2]
        iso_abbreviation = x[3]
        created_by = x[4]
        key = (title, iso_abbreviation)
        if key not in key_to_resource_id_etc:
            key_to_resource_id_etc[key] = (resource_id, curie, created_by, resource_id_to_nlm.get(resource_id))
        elif resource_id in resource_id_to_nlm:
            (correct_resource_id, correct_curie, correct_created_by, correct_nlm) = key_to_resource_id_etc[key]
            if correct_nlm is None:
                key_to_resource_id_etc[key] = (resource_id, curie, created_by, resource_id_to_nlm[resource_id])
        else:
            (correct_resource_id, correct_curie, correct_created_by, correct_nlm) = key_to_resource_id_etc[key]
            duplicate_resource_id_to_correct_resource_id[resource_id] = correct_resource_id
            logger.info("correct one:   " + str(correct_resource_id) + " | " + correct_curie + " | " + correct_created_by + " | " + str(correct_nlm))
            logger.info("duplicate one: " + str(resource_id) + " | " + curie + " | " + created_by + " | " + resource_id_to_nlm.get(resource_id, "None"))

    return duplicate_resource_id_to_correct_resource_id


def delete_editor_rows(db_session, duplicate_resource_id_to_correct_resource_id):

    rows = db_session.execute("SELECT editor_id, resource_id FROM editor").fetchall()
    row_count = 0
    for x in rows:
        editor_id = x[0]
        resource_id = x[1]
        if resource_id in duplicate_resource_id_to_correct_resource_id:
            row_count += 1
            x = db_session.query(EditorModel).filter_by(editor_id=editor_id).one_or_none()
            if x:
                try:
                    db_session.delete(x)
                    if row_count % 300 == 0:
                        db_session.commit()
                    logger.info("DELETE EDITOR for editor_id = " + str(editor_id) + " resource_id = " + str(resource_id))
                except Exception as e:
                    logger.info("Error occurred when deleting EDITOR for editor_id = " + str(editor_id) + " error = " + str(e))

    db_session.commit()


def update_editor_table(db_session, duplicate_resource_id_to_correct_resource_id):

    ## fix resource_id in EDITOR table
    rows = db_session.execute("SELECT editor_id, resource_id FROM editor").fetchall()
    row_count = 0
    for x in rows:
        editor_id = x[0]
        resource_id = x[1]
        if resource_id in duplicate_resource_id_to_correct_resource_id:
            row_count += 1
            correct_resource_id = duplicate_resource_id_to_correct_resource_id[resource_id]
            x = db_session.query(EditorModel).filter_by(editor_id=editor_id).one_or_none()
            if x:
                try:
                    x.resource_id = correct_resource_id
                    db_session.add(x)
                    if row_count % 300 == 0:
                        db_session.commit()
                    logger.info("UPDATE EDITOR for editor_id = " + str(editor_id) + " OLD resource_id = " + str(resource_id) + " NEW resource_id = " + str(correct_resource_id))
                except Exception as e:
                    logger.info("Error occurred when updating EDITOR for editor_id = " + str(editor_id) + " error = " + str(e))

    db_session.commit()


def update_reference_table(db_session, duplicate_resource_id_to_correct_resource_id):

    limit = 5000
    loop_count = 200
    row_count = 0
    for index in range(loop_count):
        offset = index * limit
        rows = db_session.execute(f"SELECT reference_id, resource_id FROM reference "
                                  f"ORDER BY reference_id limit {limit} "
                                  f"offset {offset}").fetchall()
        if len(rows) == 0:
            break
        for x in rows:
            reference_id = x[0]
            resource_id = x[1]
            if resource_id in duplicate_resource_id_to_correct_resource_id:
                correct_resource_id = duplicate_resource_id_to_correct_resource_id[resource_id]
                row_count += 1
                x = db_session.query(ReferenceModel).filter_by(reference_id=reference_id).one_or_none()
                if x:
                    try:
                        x.resource_id = correct_resource_id
                        db_session.add(x)
                        if row_count % 300 == 0:
                            db_session.commit()
                        logger.info("UPDATE REFERENCE for reference_id = " + str(reference_id) + " OLD resource_id = " + str(resource_id) + " NEW resource_id = " + str(correct_resource_id))
                    except Exception as e:
                        logger.info("Error occurred when updating REFERENCE for reference_id = " + str(reference_id) + " error = " + str(e))

    db_session.commit()


def remove_duplicate_ones(db_session, duplicate_resource_id_to_correct_resource_id):

    resource_ids_with_XREF = set()
    rows = db_session.execute("SELECT resource_id FROM cross_reference "
                              "WHERE resource_id is not NULL").fetchall()
    for x in rows:
        resource_ids_with_XREF.add(x[0])

    row_count = 0
    for duplicate_resource_id in duplicate_resource_id_to_correct_resource_id:
        if duplicate_resource_id in resource_ids_with_XREF:
            logger.info("duplicate_resource_id: " + str(duplicate_resource_id) + " is still in CROSS_REFERENCE table so keep it")
        else:
            ## remove from resource table
            row_count += 1
            x = db_session.query(ResourceModel).filter_by(resource_id=duplicate_resource_id).one_or_none()
            if x:
                try:
                    db_session.delete(x)
                    if row_count % 300 == 0:
                        db_session.commit()
                    logger.info("DELETE RESOURCE for resource_id = " + str(duplicate_resource_id))
                except Exception as e:
                    logger.info("Error occurred when deleting RESOURCE for resource_id = " + str(duplicate_resource_id) + " error = " + str(e))

            ## remove from resource_version table
            try:
                db_session.execute(f"DELETE from resource_version WHERE resource_id = {duplicate_resource_id}")
                logger.info("DELETE RESOURCE_VERSION for resource_id = " + str(duplicate_resource_id))
            except Exception as e:
                logger.info("Error occurred when deleting RESOURCE_VERSION for resource_id = " + str(duplicate_resource_id) + " error = " + str(e))
    db_session.commit()


if __name__ == "__main__":

    fix_resource_data()
