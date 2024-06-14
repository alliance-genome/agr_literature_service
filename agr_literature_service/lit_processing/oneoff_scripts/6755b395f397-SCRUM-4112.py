from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ModModel
import logging
from datetime import datetime
import pytz

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)


def populate_data():
    # just to make it easier to see and check the mappings
    name_to_atp = {'file upload': 'ATP:0000140',
                   'file needed': 'ATP:0000141',
                   'file unavailable': 'ATP:0000135',
                   'file upload in progress': 'ATP:0000139',
                   'files uploaded': 'ATP:0000134'
                   }
    mappings = [
        ['file needed', 'files uploaded'],
        ['file needed', 'file upload in progress'],
        ['file upload in progress', 'files uploaded'],
        ['file upload in progress', 'file unavailable'],
    ]
    db_session = create_postgres_session(False)
    mods = db_session.query(ModModel).all()
    for mod in mods:
        for transition in mappings:
            cmd = f"INSERT INTO workflow_transition (mod_id, transition_from, transition_to, date_created)"
            cmd += f"VALUES ({mod.mod_id}, '{name_to_atp[transition[0]]}', '{name_to_atp[transition[1]]}', '{datetime.now(tz=pytz.timezone('UTC'))}')"
            db_session.execute(cmd)
            log.info("%s(%s): Inserting transition %s to %s",
                        mod.abbreviation, mod.mod_id, name_to_atp[transition[0]], name_to_atp[transition[1]])
    db_session.rollback()
    # db_session.commit()
    db_session.close()

if __name__ == "__main__":

    populate_data()


