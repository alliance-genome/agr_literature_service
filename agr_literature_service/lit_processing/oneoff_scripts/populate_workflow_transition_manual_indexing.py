from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ModModel
from agr_literature_service.api.models import WorkflowTransitionModel
import logging
from datetime import datetime
from sqlalchemy import text
import pytz

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)


def populate_data():
    terms = [
        'ATP:0000336',
        'ATP:0000274',
        'ATP:0000275',
        'ATP:0000276',
        'ATP:0000343'
    ]
    target_terms = ['ATP:0000275', 'ATP:0000276']
    db_session = create_postgres_session(False)
    mods = db_session.query(ModModel).all()
    now = datetime.now(tz=pytz.timezone('UTC'))

    existing_transitions = set(
        db_session.query(
            WorkflowTransitionModel.mod_id,
            WorkflowTransitionModel.transition_from,
            WorkflowTransitionModel.transition_to
        ).all()
    )

    try:
        for mod in mods:
            if mod.abbreviation == 'GO':
                continue
            for from_term in terms:
                for to_term in target_terms:
                    if from_term != to_term:
                        transition_key = (mod.mod_id, from_term, to_term)
                        if transition_key in existing_transitions:
                            print(f"{transition_key} already in db")
                            continue
                        insert_sql = (
                            f"INSERT INTO workflow_transition (mod_id, transition_from, transition_to, date_created) "
                            f"VALUES ({mod.mod_id}, '{from_term}', '{to_term}', '{now}');"
                        )
                        print(insert_sql)
                        db_session.execute(text(insert_sql))
        db_session.commit()
    except Exception as e:
        db_session.rollback()
        print(f"Error occurred: {e}")
    finally:
        db_session.close()


if __name__ == "__main__":

    populate_data()
