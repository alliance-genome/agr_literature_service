from agr_literature_service.api.models.reference_mod_md5sum_model import ReferenceModMd5sumModel
from agr_literature_service.api.models import ReferenceModel, ModModel, CrossReferenceModel
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from .....fixtures import db # noqa
from sqlalchemy import and_
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import save_database_md5data, load_database_md5data


class TestMd5sumUtil:

    def test_save_database_md5data(self, db): # noqa
        md5sum_data = {"XB": {
                    "PMID:9241": "TEST1-9177c6f32fb8a80ef5955543b9dafde6",
                    "PMID:10735": "TEST2-5a24bf3a9e634fab93122b7c45a5d326"
                    },
                    "FB": {
                     "FB:FBrf0251347": "TEST3-d70b2ce7c56deab14722fb4ac2e7d287",
                     "FB:FBrf0251348": "9ca72344a115c9ce612cab87869ccd94"
                    },
                    "PMID": {
                    "PMID:9241": "TEST5-6eac9538fafd9f73eff28dd0a28a2edf"
                    }
            }
        try:
            db.execute(f"insert into  mod (abbreviation, short_name, full_name, date_created) values ('FB', 'FlyBase', 'FlyBase', 'now()') ")
            mod_results = db.execute("select abbreviation, mod_id from mod where abbreviation='FB'")
            ids = mod_results.fetchall()
            mod_id_FB = ids[0]["mod_id"]
            print("mod_id for FB:" + str(mod_id_FB))
            db.execute(f"insert into  reference (title, curie, open_access, date_created) values ('Bob', 'AGR:AGR-Reference-0000808175', 'true', 'now()') ")
            ref_results = db.execute("select reference_id from reference where curie='AGR:AGR-Reference-0000808175'")
            refs = ref_results.fetchall()
            reference_id = refs[0]["reference_id"]
            print("reference_id here:" + str(reference_id))
            db.execute(f"insert into  cross_reference (reference_id, curie, curie_prefix, date_created) values ({reference_id}, 'FB:FBrf0251347', 'FB', 'now()') ")
            db.execute(f"insert into  cross_reference (reference_id, curie, curie_prefix, date_created) values ({reference_id}, 'PMID:9241', 'PMID', 'now()') ")
            db.execute(f"insert into  reference_mod_md5sum (reference_id, mod_id, md5sum, date_updated) values ({reference_id}, {mod_id_FB}, 'd70b2ce7c56deab14722fb4ac2e7d287', 'now()') ")
            db.execute(f"insert into  reference_mod_md5sum (reference_id, md5sum, date_updated) values ({reference_id}, 'd70b2ce7c56deab14722fb4ac2e7d288', 'now()') ")
        except Exception as e:
            print(e)
        db.commit()

        print(" here to update md5sum")
        mods = ["FB", "XB", "PMID"]
        save_database_md5data(md5sum_data, mods)
        print("here to assert update result for PMID")
        md5sum_results = db.execute(f"select rmm.md5sum from cross_reference r, reference_mod_md5sum rmm where rmm.mod_id is null and r.reference_id=rmm.reference_id and r.curie_prefix='PMID' and r.curie='PMID:9241' ")
        md5sums = md5sum_results.fetchall()
        md5sum_PIMD = md5sums[0]["md5sum"] 
        assert md5sum_PIMD == md5sum_data["PMID"]["PMID:9241"]

        print("here to assert md5sum for FB")
        md5sum_results = db.execute(f"select rmm.md5sum from cross_reference r, reference_mod_md5sum rmm, mod m where rmm.mod_id=m.mod_id and m.abbreviation='FB' and r.reference_id=rmm.reference_id and r.curie_prefix='FB' and r.curie='FB:FBrf0251347' ")
        md5sums = md5sum_results.fetchall()
        md5sum_FB = md5sums[0]["md5sum"] 
        assert md5sum_FB == md5sum_data["FB"]["FB:FBrf0251347"]


    def test_load_database_md5data(self, db): # noqa
        try:
            print("insert FB")
            db.execute(f"insert into  mod (abbreviation, short_name, full_name, date_created) values ('FB', 'FlyBase', 'FlyBase', 'now()') ")
            mod_results = db.execute("select abbreviation, mod_id from mod where abbreviation='FB'")
            ids = mod_results.fetchall()
            mod_id_FB = ids[0]["mod_id"]
            print("insert XB")
            db.execute(f"insert into  mod (abbreviation, short_name, full_name, date_created) values ('XB', 'Xenbase', 'Xenbase', 'now()') ")
            mod_results = db.execute("select abbreviation, mod_id from mod where abbreviation='XB'")
            ids = mod_results.fetchall()
            mod_id_XB = ids[0]["mod_id"]
            print("mod_id for XB:" + str(mod_id_XB))
            db.execute(f"insert into  reference (title, curie, open_access, date_created) values ('Bob', 'AGR:AGR-Reference-0000808175', 'true', 'now()') ")
            ref_results = db.execute("select reference_id from reference where curie='AGR:AGR-Reference-0000808175'")
            refs = ref_results.fetchall()
            reference_id = refs[0]["reference_id"]
            print("reference_id here:" + str(reference_id))
            db.execute(f"insert into  cross_reference (reference_id, curie, curie_prefix, date_created) values ({reference_id}, 'FB:FBrf0001', 'FB', 'now()') ")
            db.execute(f"insert into  cross_reference (reference_id, curie, curie_prefix, date_created) values ({reference_id}, 'PMID:0001', 'PMID', 'now()') ")
            db.execute(f"insert into  cross_reference (reference_id, curie, curie_prefix, date_created) values ({reference_id}, 'Xenbase:XB-ART-0001', 'Xenbase', 'now()') ")
            db.execute(f"insert into  reference_mod_md5sum (reference_id, mod_id, md5sum, date_updated) values ({reference_id}, {mod_id_FB}, 'TEST-md5sum-FB', 'now()') ")
            db.execute(f"insert into  reference_mod_md5sum (reference_id, md5sum, date_updated) values ({reference_id}, 'TEST-md5sum-PMID', 'now()') ")
            db.execute(f"insert into  reference_mod_md5sum (reference_id, mod_id, md5sum, date_updated) values ({reference_id}, {mod_id_XB}, 'TEST-md5sum-XB', 'now()') ")
        
        except Exception as e:
            print(e)
        db.commit()

        print(" here to load md5sum into dict")
        mods = ["FB", "XB", "PMID", "TEST"]
        dict_md5sum = load_database_md5data(mods)
        assert  dict_md5sum['FB']['FB:FBrf0001'] == 'TEST-md5sum-FB'
        assert  dict_md5sum['XB']['Xenbase:XB-ART-0001'] == 'TEST-md5sum-XB'
        # remove prefix 'PMID:' for PMID
        assert  dict_md5sum['PMID']['0001'] == 'TEST-md5sum-PMID'

         