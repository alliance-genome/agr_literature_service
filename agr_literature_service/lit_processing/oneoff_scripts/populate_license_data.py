import logging
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.api.models import ReferenceModel

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

## this is a list tab-delimited PMCID and its license name
## which is not included in the repo, see a few example below
# PMC9727813	CC BY-NC-SA
# PMC6798786	CC BY
# PMC2883084	CC BY-NC
# PMC4047897	CC BY-NC-ND
# PMC3901671	none
## this file was generated by another PMC search script
## and it will be added to the repo soon
licenseFile = "data/pmc_license_searched.txt"
batch_size = 250


def populate_license_column():

    db_session = create_postgres_session(False)

    logger.info("Retrieving data from copyright_license table...")

    rows = db_session.execute("SELECT copyright_license_id, name FROM copyright_license").fetchall()

    license_name_to_id = {}
    for x in rows:
        license_name_to_id[x['name']] = x['copyright_license_id']

    logger.info("Retrieving data from cross_reference table...")

    rows = db_session.execute(
        "SELECT reference_id, curie "
        "FROM cross_reference "
        "WHERE curie_prefix = 'PMCID'").fetchall()
    pmcid_to_reference_id = {}
    for x in rows:
        pmcid = x['curie'].replace('PMCID:', '')
        pmcid_to_reference_id[pmcid] = x['reference_id']

    logger.info("Retrieving data from reference table...")

    rows = db_session.execute(
        "SELECT reference_id "
        "FROM reference "
        "WHERE copyright_license_id is not null").fetchall()

    license_id_populated = set()
    for x in rows:
        license_id_populated.add(x[0])

    f = open(licenseFile)
    i = 0
    for line in f:

        pieces = line.strip().split("\t")
        pmcid = pieces[0]
        license_name = pieces[1]

        reference_id = pmcid_to_reference_id.get(pmcid)
        if reference_id is None:
            logger.info("pmcid: " + pmcid + " is not in the cross_reference table.")
            continue
        if reference_id in license_id_populated:
            continue

        if license_name == 'none':
            license_name = 'PMC-none'
        license_id = license_name_to_id.get(license_name)
        if license_id is None:
            logger.info("license_name: " + license_name + " is not in the copyright_license table.")
            continue

        x = db_session.query(ReferenceModel).filter_by(reference_id=reference_id).one_or_none()
        if x and x.copyright_license_id is None:
            try:
                x.copyright_license_id = license_id
                db_session.add(x)
                i += 1
                if i % batch_size == 0:
                    db_session.commit()
                logger.info("Adding license_id to reference table for reference_id = " + str(reference_id))
            except Exception as e:
                logger.info("An error occurred when adding license_id to reference table for reference_id = " + str(reference_id) + ". error = " + str(e))

    db_session.commit()
    db_session.close()


if __name__ == "__main__":

    populate_license_column()
