import logging
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import CopyrightLicenseModel

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def load_licenses():

    db_session = create_postgres_session(False)

    for data in get_data():
        status = insert_license(db_session, data)
        if status:
            db_session.rollback()
            return

    db_session.commit()
    db_session.close()


def insert_license(db_session, data):

    try:
        license = CopyrightLicenseModel(name=data['name'],
                                        url=data['url'],
                                        description=data['description'])
        db_session.add(license)
        logger.info("Insert " + data['name'] + " into copyright_license table.")
    except Exception as e:
        logger.info("Insert " + data['name'] + " into copyright_license table FAILED. error = " + str(e))
        return 1
    return 0


def get_data():

    return [{"name": "CC BY",
             "url": "https://creativecommons.org/licenses/by/4.0/",
             "description": "This license allows reusers to distribute, remix, adapt, and build upon the material in any medium or format, so long as attribution is given to the creator. The license allows for commercial use."},
            {"name": "CC BY-SA",
             "url": "https://creativecommons.org/licenses/by-sa/4.0/",
             "description": "This license allows reusers to distribute, remix, adapt, and build upon the material in any medium or format, so long as attribution is given to the creator. The license allows for commercial use. If you remix, adapt, or build upon the material, you must license the modified material under identical terms."},
            {"name": "CC BY-NC",
             "url": "https://creativecommons.org/licenses/by-nc/4.0/",
             "description": "This license allows reusers to distribute, remix, adapt, and build upon the material in any medium or format for noncommercial purposes only, and only so long as attribution is given to the creator."},
            {"name": "CC BY-NC-SA",
             "url": "https://creativecommons.org/licenses/by-nc-sa/4.0/",
             "description": "This license allows reusers to distribute, remix, adapt, and build upon the material in any medium or format for noncommercial purposes only, and only so long as attribution is given to the creator. If you remix, adapt, or build upon the material, you must license the modified material under identical terms."},
            {"name": "CC BY-ND",
             "url": "https://creativecommons.org/licenses/by-nd/4.0/",
             "description": "This license allows reusers to copy and distribute the material in any medium or format in unadapted form only, and only so long as attribution is given to the creator. The license allows for commercial use."},
            {"name": "CC BY-NC-ND",
             "url": "https://creativecommons.org/licenses/by-nc-nd/4.0/",
             "description": "This license allows reusers to copy and distribute the material in any medium or format in unadapted form only, for noncommercial purposes only, and only so long as attribution is given to the creator."},
            {"name": "CC0",
             "url": "https://creativecommons.org/publicdomain/zero/1.0/",
             "description": "This license (aka CC Zero) is a public dedication tool, which allows creators to give up their copyright and put their works into the worldwide public domain. CC0 allows reusers to distribute, remix, adapt, and build upon the material in any medium or format, with no conditions."},
            {"name": "PMC-none",
             "url": "",
             "description": ""}]


if __name__ == "__main__":

    load_licenses()
