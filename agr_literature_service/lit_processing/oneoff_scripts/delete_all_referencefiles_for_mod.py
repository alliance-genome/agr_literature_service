import argparse

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.crud.referencefile_mod_utils import destroy as delete_referencefile_mod


def remove_all_referencefiles_for_mod(mod_abbreviation):
    db_session = create_postgres_session(False)

    rs = db_session.execute(f"SELECT referencefile_mod_id from referencefile_mod where mod_id ="
                            f" (select mod_id from mod where abbreviation = {mod_abbreviation})")
    referencefile_mod_ids = [row[0] for row in rs.fetchall()]

    for referencefile_mod_id in referencefile_mod_ids:
        delete_referencefile_mod(db_session, referencefile_mod_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Remove all reference files for a specific mod.')
    parser.add_argument('--mod', type=str, help='MOD abbreviation', required=True)

    args = parser.parse_args()
    remove_all_referencefiles_for_mod(args.mod)
