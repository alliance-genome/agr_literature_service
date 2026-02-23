import logging.config
from os import path

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ModModel
from agr_literature_service.api.user import set_global_user_id

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


def populate_test_mods():      # noqa: C901
    """
    :return:
    """

    db_session = create_postgres_session(False)
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)

    # test mod:
    # {"abbreviation": "AtDB",
    # "short_name": "AtDB2",
    # "full_name": "AtDB test full name"}

    mod_data = [{"abbreviation": "FB",
                 "short_name": "FlyBase",
                 "full_name": "FlyBase"},
                {"abbreviation": "WB",
                 "short_name": "WormBase",
                 "full_name": "WormBase"},
                {"abbreviation": "ZFIN",
                 "short_name": "ZFIN",
                 "full_name": "Zebrafish Information Network"},
                {"abbreviation": "SGD",
                 "short_name": "SGD",
                 "full_name": "Saccharomyces Genome Database"},
                {"abbreviation": "MGI",
                 "short_name": "MGD",
                 "full_name": "Mouse Genome Database"},
                {"abbreviation": "RGD",
                 "short_name": "RGD",
                 "full_name": "Rat Genome Database"},
                {"abbreviation": "XB",
                 "short_name": "Xenbase",
                 "full_name": "Xenbase"},
                {"abbreviation": "GO",
                 "short_name": "GOC",
                 "full_name": "Gene Ontology Consortium"},
                {"abbreviation": "alliance",
                 "short_name": "Alliance",
                 "full_name": "Alliance of Genome Resources"}]

    for data in mod_data:

        x = db_session.query(ModModel).filter_by(abbreviation=data["abbreviation"]).one_or_none()

        if x is None:

            try:
                x = ModModel(**data)
                db_session.add(x)
                logger.info("Insert " + data["abbreviation"] + " info into Mod table.")
            except Exception as e:
                logger.info("An error occurred when inserting " + data["abbreviation"] + " info into Mod table. " + str(e))

        elif x.short_name == data["short_name"] and x.full_name == data["full_name"]:
            continue
        else:
            try:
                if x.short_name != data["short_name"]:
                    x.short_name = data["short_name"]
                if x.full_name != data["full_name"]:
                    x.full_name = data["full_name"]
                db_session.add(x)
                logger.info("Mod info for " + data["abbreviation"] + " has been updated.")
            except Exception as e:
                logger.info("An error occurred when updating Mod info for " + data["abbreviation"] + ". " + str(e))

    db_session.commit()
    logger.info("DONE!")


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("Starting mod_populate_load.py")
    populate_test_mods()
    logger.info("ending mod_populate_load.py")

# pipenv run python mod_populate_load.py
