import datetime
from sqlalchemy import text
from sqlalchemy.orm import Session
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session


def do_it(db_session):
    """Remove tet for version if no longer valid."""
    # Get a set of topic_enity_tags ids
    print(datetime.datetime.now())
    query = """SELECT topic_entity_tag_id from topic_entity_tag"""
    results = db_session.execute(text(query)).fetchall()
    tet_ids = set()
    for result in results:
        tet_ids.add(result[0])

    print(f"tet_ids = {len(tet_ids)}")
    print(datetime.datetime.now())

    query = """SELECT topic_entity_tag_id from topic_entity_tag_version"""
    del_set = set()
    del_list = []
    results = db_session.execute(text(query)).fetchall()
    for result in results:
        if result[0] not in tet_ids:
            if result[0] not in del_set:
                del_set.add(result[0])
                del_list.append(str(result[0]))

    print(f"del_set = {len(del_set)}")
    print(f"del_list = {len(del_list)}")
    print(f"del list first={del_list[0]}, last={del_list[-1]}")
    print(datetime.datetime.now())

    large_batch_size = 500
    for batch_num, i in enumerate(range(0, len(del_list), large_batch_size), start=1):
        batch_tets = del_list[i:i + large_batch_size]
        print(f"batch_num {batch_num} {datetime.datetime.now()}")
        tets = ', '.join(batch_tets)
        query = f"""DELETE fROM topic_entity_tag_version where topic_entity_tag_id in ({tets})"""
        db_session.execute(text(query))
        db_session.commit()

if __name__ == "__main__":
    db_session: Session = create_postgres_session(False)
    do_it(db_session)
