# from pickle import FALSE
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_engine, \
    create_postgres_session
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import \
    load_s3_md5data, save_database_md5data


# load md5sum from s3 to database table reference_mod_md5sum
def load_md5sum_from_s3_to_database():
    engine = create_postgres_engine(False)
    db_connection = engine.connect()
    mod_ids = {}
    try:
        db_session = create_postgres_session(False)
        mod_results = db_session.execute("select abbreviation, mod_id from mod")
        ids = mod_results.fetchall()
        for id in ids:
            mod_ids[id["abbreviation"]] = id["mod_id"]
    except Exception as e:
        print('Error: ' + str(type(e)))
    for mod in ['WB', 'ZFIN', 'XB', 'FB', 'SGD', 'RGD', 'MGI', 'PMID']:
        if mod == 'PMID':
            print("Updating md5sum that are not associated with a mod:")
        elif mod in mod_ids.keys():
            print("Updating md5sum for " + mod + ":")
            mod_id = mod_ids[mod]
        else:
            print("invalid mod:" + mod)
            continue
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

    #load_md5sum_from_s3_to_database()
    md5sum_data = {"XB": {
                    "PMID:9241": "TEST1-19177c6f32fb8a80ef5955543b9dafde6",
                    "PMID:10735": "TEST2-5a24bf3a9e634fab93122b7c45a5d326",
                    },
                    "FB": {
                     "FB:FBrf0247859": "TEST3-d70b2ce7c56deab14722fb4ac2e7d287",
                     "FB:FBrf0251348": "TEST4-9ca72344a115c9ce612cab87869ccd94",
                    },
                    "PMID": {
                    "PMID:9241": "TEST5-6eac9538fafd9f73eff28dd0a28a2edf",
                    "PMID:23524264": "TEST6-adf54b253e058d911139f7599116c24e"
                    }
            }
    mods= ["FB", "XB", "PMID"]
    save_database_md5data(md5sum_data, mods)
