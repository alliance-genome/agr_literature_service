import logging
from os import path
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.lit_processing.utils.db_read_utils import get_pmid_to_reference_id_mapping
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


def load_ref_file_metadata_into_db():  # pragma: no cover

    db_session = create_postgres_session(False)
    script_nm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, script_nm)

    ref_files_id_pmc_set = set([row["referencefile_id"] for row in db_session.execute(
        "SELECT referencefile_id FROM referencefile_mod WHERE mod_id is null").fetchall()])

    ref_file_key_dbid = {}
    ref_file_uniq_filename_set = set()
    for row in db_session.execute("SELECT referencefile_id, reference_id, md5sum, display_name, file_extension FROM "
                                  "referencefile").fetchall():
        ref_file_key = (row["reference_id"], row["md5sum"])
        ref_file_dbid = row["referencefile_id"]
        uniq_filename = (row["reference_id"], row["display_name"] + "." + row["file_extension"])
        ref_file_key_dbid[ref_file_key] = ref_file_dbid
        ref_file_uniq_filename_set.add(uniq_filename)

    pmid_to_reference_id = get_pmid_to_reference_id_mapping(db_session)

    with open(infile) as f:
        for line_num, line in enumerate(f):
            if line_num % batch_commit_size == 0:
                db_session.commit()
                # db_session.rollback()

            # 35857496      PMC9278858      sciadv.abm9875-f5.jpg   17ef0e061fcdc9bd1f4338809f738d72
            pieces = line.strip().split("\t")
            pmid = pieces[0]
            reference_id = pmid_to_reference_id.get(pmid)
            if reference_id is None:
                continue
            md5sum = pieces[3]
            file_name_with_suffix = pieces[2]
            if (reference_id, file_name_with_suffix) in ref_file_uniq_filename_set:
                file_name_with_suffix = resolve_displayname_conflict(file_name_with_suffix)
            referencefile_id = None
            if (reference_id, md5sum) in ref_file_key_dbid:
                referencefile_id = ref_file_key_dbid[(reference_id, md5sum)]
                if referencefile_id in ref_files_id_pmc_set:
                    continue

            if not referencefile_id:
                referencefile_id = insert_referencefile(db_session, pmid, file_class,
                                                        file_publication_status,
                                                        file_name_with_suffix,
                                                        reference_id, md5sum,
                                                        logger)

            if referencefile_id:
                insert_referencefile_mod_for_pmc(db_session, pmid, file_name_with_suffix,
                                                 referencefile_id, logger)
                ref_file_key_dbid[(reference_id, md5sum)] = referencefile_id
                ref_files_id_pmc_set.add(referencefile_id)
                ref_file_uniq_filename_set.add((reference_id, file_name_with_suffix))

        db_session.commit()
        # db_session.rollback()
        db_session.close()


def resolve_displayname_conflict(file_name_with_suffix):

    file_extension = file_name_with_suffix.split(".")[-1].lower()
    display_name = file_name_with_suffix.replace("." + file_extension, "") + "_1"
    return display_name + "." + file_extension


if __name__ == "__main__":

    load_ref_file_metadata_into_db()
