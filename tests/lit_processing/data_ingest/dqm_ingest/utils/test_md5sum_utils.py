from .....fixtures import db # noqa
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import save_database_md5data, load_database_md5data
from sqlalchemy import text


class TestMd5sumUtil:

    def test_save_database_md5data(self, db):  # noqa
        md5sum_data = {
            "XB":
            {
                "PMID:9241": "TEST1-XB-001",
                "Xenbase:XB-ART-58863": "TEST2-XB-001"
            },
            "FB":
            {
                "FB:FBrf00000001": "TEST3-FB-001"
            },
            "PMID":
            {
                "PMID:9241": "TEST5-PMID-001"
            }
        }
        try:
            with db.begin():  # Start a transaction block
                db.execute(text("insert into  mod (abbreviation, short_name, full_name, date_created) values ('FB', 'FlyBase', 'FlyBase', 'now()') "))
                mod_results = db.execute(text("select abbreviation, mod_id from mod where abbreviation='FB'"))
                ids = mod_results.fetchall()
                mod_id_FB = ids[0]["mod_id"]
                db.execute(text("insert into  mod (abbreviation, short_name, full_name, date_created) values ('XB', 'Xenbase', 'Xenbase', 'now()') "))

                db.execute(text("insert into  reference (title, curie, date_created) values ('Bob', 'AGR:AGR-Reference-0000808175', 'now()') "))
                ref_results = db.execute(text("select reference_id from reference where curie='AGR:AGR-Reference-0000808175'"))
                refs = ref_results.fetchall()
                reference_id = refs[0]["reference_id"]
                print("reference_id here:" + str(reference_id))

                db.execute(text(f"insert into  cross_reference (reference_id, curie, curie_prefix, date_created, is_obsolete) values ({reference_id}, 'FB:FBrf00000001', 'FB', 'now()', 'false') "))
                db.execute(text(f"insert into  cross_reference (reference_id, curie, curie_prefix, date_created, is_obsolete) values ({reference_id}, 'PMID:9241', 'PMID', 'now()', 'false') "))
                db.execute(text(f"insert into  cross_reference (reference_id, curie, curie_prefix, date_created, is_obsolete) values ({reference_id}, 'Xenbase:XB-ART-58863', 'Xenbase', 'now()', 'false') "))
                db.execute(text(f"insert into  reference_mod_md5sum (reference_id, mod_id, md5sum, date_updated) values ({reference_id}, {mod_id_FB}, 'd70b2ce7c56deab14722fb4ac2e7d287', 'now()') "))
                db.execute(text(f"insert into  reference_mod_md5sum (reference_id, md5sum, date_updated) values ({reference_id}, 'd70b2ce7c56deab14722fb4ac2e7d288', 'now()') "))
        except Exception as e:
            print(e)
        # db.commit()

        print(" here to update md5sum")
        save_database_md5data(md5sum_data)
        print("here to assert update result for PMID")
        md5sum_results = db.execute(text("select rmm.md5sum from cross_reference r, reference_mod_md5sum rmm where rmm.mod_id is null and r.reference_id=rmm.reference_id and r.curie_prefix='PMID' and r.curie='PMID:9241' "))
        md5sums = md5sum_results.fetchall()
        md5sum_PMID = md5sums[0]["md5sum"]
        assert md5sum_PMID == md5sum_data["PMID"]["PMID:9241"]

        print("here to assert md5sum for FB")
        md5sum_results = db.execute(text("select rmm.md5sum from cross_reference r, reference_mod_md5sum rmm, mod m where rmm.mod_id=m.mod_id and m.abbreviation='FB' and r.reference_id=rmm.reference_id and r.curie='FB:FBrf00000001' "))
        md5sums = md5sum_results.fetchall()
        md5sum_FB = md5sums[0]["md5sum"]
        assert md5sum_FB == md5sum_data["FB"]["FB:FBrf00000001"]

        print("here to assert md5sum for XB")
        md5sum_results = db.execute(text("select rmm.md5sum from cross_reference r, reference_mod_md5sum rmm, mod m where rmm.mod_id=m.mod_id and m.abbreviation='XB' and r.reference_id=rmm.reference_id and r.curie='Xenbase:XB-ART-58863' "))
        md5sums = md5sum_results.fetchall()
        md5sum_XB = md5sums[0]["md5sum"]
        assert md5sum_XB == md5sum_data["XB"]["Xenbase:XB-ART-58863"]

        md5sum_data_empty = {}
        save_database_md5data(md5sum_data_empty)
        md5sum_results = db.execute(text("select rmm.md5sum from cross_reference r, reference_mod_md5sum rmm, mod m where rmm.mod_id=m.mod_id and m.abbreviation='FB' and r.reference_id=rmm.reference_id and r.curie='FB:FBrf00000001' "))
        md5sums = md5sum_results.fetchall()
        md5sum_XB = md5sums[0]["md5sum"]
        assert md5sum_XB == md5sum_data["FB"]["FB:FBrf00000001"]



    def test_load_database_md5data(self, db):  # noqa
        try:
            with db.begin():  # Start a transaction block
                db.execute(text("insert into  mod (abbreviation, short_name, full_name, date_created) values ('FB', 'FlyBase', 'FlyBase', 'now()') "))
                mod_results = db.execute(text("select abbreviation, mod_id from mod where abbreviation='FB'"))
                ids = mod_results.fetchall()
                mod_id_FB = ids[0]["mod_id"]
                db.execute(text("insert into  mod (abbreviation, short_name, full_name, date_created) values ('XB', 'Xenbase', 'Xenbase', 'now()') "))
                mod_results = db.execute(text("select abbreviation, mod_id from mod where abbreviation='XB'"))
                ids = mod_results.fetchall()
                mod_id_XB = ids[0]["mod_id"]
                db.execute(text("insert into  reference (title, curie, date_created) values ('Bob', 'AGR:AGR-Reference-0000808175', 'now()') "))
                ref_results = db.execute(text("select reference_id from reference where curie='AGR:AGR-Reference-0000808175'"))
                refs = ref_results.fetchall()
                reference_id = str(refs[0]["reference_id"])
                db.execute(text(f"insert into  cross_reference (reference_id, curie, curie_prefix, date_created) values ({reference_id}, 'FB:FBrf0001', 'FB', 'now()') "))
                db.execute(text(f"insert into  cross_reference (reference_id, curie, curie_prefix, date_created) values ({reference_id}, 'PMID:0001', 'PMID', 'now()') "))
                db.execute(text(f"insert into  cross_reference (reference_id, curie, curie_prefix, date_created) values ({reference_id}, 'Xenbase:XB-ART-0001', 'Xenbase', 'now()') "))
                db.execute(text(f"insert into  reference_mod_md5sum (reference_id, mod_id, md5sum, date_updated) values ({reference_id}, {mod_id_FB}, 'TEST-md5sum-FB', 'now()') "))
                db.execute(text(f"insert into  reference_mod_md5sum (reference_id, md5sum, date_updated) values ({reference_id}, 'TEST-md5sum-PMID', 'now()') "))
                db.execute(text(f"insert into  reference_mod_md5sum (reference_id, mod_id, md5sum, date_updated) values ({reference_id}, {mod_id_XB}, 'TEST-md5sum-XB', 'now()') "))
        except Exception as e:
            print(e)
        # db.commit()

        print(" here to load md5sum into dict")
        mods = ["FB", "XB", "PMID", "TEST"]
        dict_md5sum = load_database_md5data(mods)
        print(dict_md5sum)
        assert dict_md5sum['FB']['FB:FBrf0001'] == 'TEST-md5sum-FB'
        assert dict_md5sum['XB']['Xenbase:XB-ART-0001'] == 'TEST-md5sum-XB'
        assert dict_md5sum['PMID']['PMID:0001'] == 'TEST-md5sum-PMID'
