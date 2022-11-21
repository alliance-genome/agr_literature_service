import logging
from os import path
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_referencefile_rows, get_referencefile_mod_rows, \
    get_pmid_to_reference_id_mapping
from agr_literature_service.lit_processing.data_ingest.utils.db_write_utils import \
    insert_referencefile_mod_for_pmc, insert_referencefile
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

infile = "data/pmc_oa_files_uploaded.txt"

file_class = "supplement"
file_publication_status = "final"
batch_commit_size = 250


def load_data():

    db_session = create_postgres_session(False)

    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)

    (referencefile_loaded, reference_id_file_name) = get_referencefile_rows(db_session)

    referencefile_mod_loaded = get_referencefile_mod_rows(db_session)

    pmid_to_reference_id = get_pmid_to_reference_id_mapping(db_session)

    f = open(infile)
    i = 0
    for line in f:
        # 35857496      PMC9278858      sciadv.abm9875-f5.jpg   17ef0e061fcdc9bd1f4338809f738d72
        pieces = line.strip().split("\t")
        pmid = pieces[0]
        reference_id = pmid_to_reference_id.get(pmid)
        if reference_id is None:
            continue
        md5sum = pieces[3]
        file_name_with_suffix = pieces[2]
        if (reference_id, file_name_with_suffix) in reference_id_file_name:
            file_name_with_suffix = process_file_name_conflict(file_name_with_suffix)
        referencefile_id = None
        if (reference_id, md5sum) in referencefile_loaded:
            referencefile_id = referencefile_loaded[(reference_id, md5sum)]
            if referencefile_id in referencefile_mod_loaded:
                continue
        i += 1

        if i % batch_commit_size == 0:
            db_session.commit()
            # db_session.rollback()

        if referencefile_id is None:
            referencefile_id = insert_referencefile(db_session, pmid, file_class,
                                                    file_publication_status,
                                                    file_name_with_suffix,
                                                    reference_id, md5sum,
                                                    logger)

        if referencefile_id:
            insert_referencefile_mod_for_pmc(db_session, pmid, file_name_with_suffix,
                                             referencefile_id, logger)
            referencefile_loaded[(reference_id, md5sum)] = referencefile_id
            referencefile_mod_loaded[referencefile_id] = 1
            reference_id_file_name[(reference_id, file_name_with_suffix)] = 1

    f.close()
    db_session.commit()
    # db_session.rollback()
    db_session.close()


def process_file_name_conflict(file_name_with_suffix):

    file_extension = file_name_with_suffix.split(".")[-1].lower()
    display_name = file_name_with_suffix.replace("." + file_extension, "") + "_1"
    return display_name + "." + file_extension


if __name__ == "__main__":

    load_data()
