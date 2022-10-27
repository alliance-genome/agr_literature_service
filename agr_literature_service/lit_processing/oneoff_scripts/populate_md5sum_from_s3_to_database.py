from pickle import FALSE
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_engine, \
    create_postgres_session
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import \
    load_s3_md5data


# load md5sum from s3 to database table reference_mod_md5sum
def load_md5sum_from_s3_to_database():
    engine = create_postgres_engine(FALSE)
    db_connection = engine.connect()
    try:
        db_session = create_postgres_session(FALSE)
    except Exception as e:
        print('Error: ' + str(type(e)))
    for mod in ['WB', 'ZFIN', 'XB', 'FB', 'SGD', 'RGD', 'MGI', 'PMID']:
        if mod == 'PMID':
            print("Updating md5sum that are not associated with a mod:")
        else:
            print("Updating md5sum for " + mod + ":")
            mod_results = db_session.execute(f"select mod_id from mod where abbreviation = '{mod}'")
            mods = mod_results.fetchall()
            mod_id = mods[0]["mod_id"]
        md5dict = load_s3_md5data([mod])
        for PMID in md5dict[mod]:
            # in the PMID_md5sum, key for PMID without prefix PMID:, but in cross_reference.curie it store with PMID:nnnnn
            if mod == 'PMID':
                PMID_temp = 'PMID:' + PMID
                PMID_results = db_session.execute(f"SELECT reference_id FROM cross_reference WHERE curie  = '{PMID_temp}'")
            else:
                PMID_results = db_session.execute(f"SELECT reference_id FROM cross_reference WHERE curie  = '{PMID}'")
            PMIDs = PMID_results.fetchall()
            if len(PMIDs) == 0:
                print('unable to find this in cross_reference:', PMID, '->', md5dict[mod][PMID])
            else:
                reference_id = PMIDs[0]["reference_id"]
                md5sum = md5dict[mod][PMID]
                try:
                    if mod == 'PMID':
                        db_connection.execute(f"insert into  reference_mod_md5sum (reference_id, mod_id, md5sum, date_updated) values ('{reference_id}', null, '{md5sum}', 'now()') ")
                    else:
                        db_connection.execute(f"insert into  reference_mod_md5sum (reference_id, mod_id, md5sum, date_updated) values ('{reference_id}', '{mod_id}', '{md5sum}', 'now()') ")
                except Exception as e:
                    print('Error: ' + str(type(e)))
                    print("insert into  reference_mod_md5sum (reference_id, mod_id, md5sum, date_updated) values ('{reference_id}', '{mod_id}', '{md5sum}', 'now()') ")
    db_session.commit()
    db_session.close()


if __name__ == "__main__":

    load_md5sum_from_s3_to_database()
    # md5dict = load_database_md5data(["FB", "WB", "ZFIN", "SGD", "MGI", "RGD", "XB", "PMID"])
