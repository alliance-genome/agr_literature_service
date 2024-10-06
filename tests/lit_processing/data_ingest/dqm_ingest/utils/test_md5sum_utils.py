from .....fixtures import db # noqa
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import save_database_md5data, load_database_md5data
from sqlalchemy import text


class TestMd5sumUtil:

    def test_save_database_md5data(self, db):  # noqa
        md5sum_data = {
            "XB": {
                "PMID:9241": "TEST1-XB-001",
                "Xenbase:XB-ART-58863": "TEST2-XB-001"
            },
            "FB": {
                "FB:FBrf00000001": "TEST3-FB-001"
            },
            "PMID": {
                "PMID:9241": "TEST5-PMID-001"
            }
        }

        # Data Insertion
        with db.begin():
            # Insert 'FB' mod
            db.execute(text("INSERT INTO mod (abbreviation, short_name, full_name, date_created) VALUES ('FB', 'FlyBase', 'FlyBase', now())"))
            mod_results = db.execute(text("SELECT abbreviation, mod_id FROM mod WHERE abbreviation='FB'"))
            ids = mod_results.mappings().fetchall()
            mod_id_FB = ids[0]["mod_id"]
            # Insert 'XB' mod
            db.execute(text("INSERT INTO mod (abbreviation, short_name, full_name, date_created) VALUES ('XB', 'Xenbase', 'Xenbase', now())"))
            mod_results = db.execute(text("SELECT abbreviation, mod_id FROM mod WHERE abbreviation='XB'"))
            ids = mod_results.mappings().fetchall()
            # mod_id_XB = ids[0]["mod_id"]
            # Insert reference
            db.execute(text("INSERT INTO reference (title, curie, date_created) VALUES ('Bob', 'AGR:AGR-Reference-0000808175', now())"))
            ref_results = db.execute(text("SELECT reference_id FROM reference WHERE curie='AGR:AGR-Reference-0000808175'"))
            refs = ref_results.mappings().fetchall()
            reference_id = refs[0]["reference_id"]

            # Insert cross_references
            db.execute(text(f"INSERT INTO cross_reference (reference_id, curie, curie_prefix, date_created, is_obsolete) VALUES ({reference_id}, 'FB:FBrf00000001', 'FB', now(), FALSE)"))
            db.execute(text(f"INSERT INTO cross_reference (reference_id, curie, curie_prefix, date_created, is_obsolete) VALUES ({reference_id}, 'PMID:9241', 'PMID', now(), FALSE)"))
            db.execute(text(f"INSERT INTO cross_reference (reference_id, curie, curie_prefix, date_created, is_obsolete) VALUES ({reference_id}, 'Xenbase:XB-ART-58863', 'Xenbase', now(), FALSE)"))

            # Insert reference_mod_md5sum
            db.execute(text(f"INSERT INTO reference_mod_md5sum (reference_id, mod_id, md5sum, date_updated) VALUES ({reference_id}, {mod_id_FB}, 'd70b2ce7c56deab14722fb4ac2e7d287', now())"))
            db.execute(text(f"INSERT INTO reference_mod_md5sum (reference_id, md5sum, date_updated) VALUES ({reference_id}, 'd70b2ce7c56deab14722fb4ac2e7d288', now())"))

        # Call the function under test
        save_database_md5data(md5sum_data)

        # Assertions
        # Assert md5sum for PMID
        md5sum_results = db.execute(text("""
            SELECT rmm.md5sum
            FROM cross_reference r
            JOIN reference_mod_md5sum rmm ON r.reference_id = rmm.reference_id
            WHERE rmm.mod_id IS NULL AND r.curie_prefix='PMID' AND r.curie='PMID:9241'
        """))
        md5sums = md5sum_results.mappings().fetchall()
        assert md5sums, "No md5sum found for PMID:9241"
        md5sum_PMID = md5sums[0]["md5sum"]
        assert md5sum_PMID == md5sum_data["PMID"]["PMID:9241"]

        # Assert md5sum for FB
        md5sum_results = db.execute(text("""
            SELECT rmm.md5sum
            FROM cross_reference r
            JOIN reference_mod_md5sum rmm ON r.reference_id = rmm.reference_id
            JOIN mod m ON rmm.mod_id = m.mod_id
            WHERE m.abbreviation='FB' AND r.curie='FB:FBrf00000001'
        """))
        md5sums = md5sum_results.mappings().fetchall()
        assert md5sums, "No md5sum found for FB:FBrf00000001"
        md5sum_FB = md5sums[0]["md5sum"]
        assert md5sum_FB == md5sum_data["FB"]["FB:FBrf00000001"]

        # Assert md5sum for XB
        md5sum_results = db.execute(text("""
            SELECT rmm.md5sum
            FROM cross_reference r
            JOIN reference_mod_md5sum rmm ON r.reference_id = rmm.reference_id
            JOIN mod m ON rmm.mod_id = m.mod_id
            WHERE m.abbreviation='XB' AND r.curie='Xenbase:XB-ART-58863'
        """))
        md5sums = md5sum_results.mappings().fetchall()
        assert md5sums, "No md5sum found for Xenbase:XB-ART-58863"
        md5sum_XB = md5sums[0]["md5sum"]
        assert md5sum_XB == md5sum_data["XB"]["Xenbase:XB-ART-58863"]

        # Test with empty md5sum_data
        md5sum_data_empty = {}
        save_database_md5data(md5sum_data_empty)
        # Assertions to verify no changes
        md5sum_results = db.execute(text("""
            SELECT rmm.md5sum
            FROM cross_reference r
            JOIN reference_mod_md5sum rmm ON r.reference_id = rmm.reference_id
            JOIN mod m ON rmm.mod_id = m.mod_id
            WHERE m.abbreviation='FB' AND r.curie='FB:FBrf00000001'
        """))
        md5sums = md5sum_results.mappings().fetchall()
        assert md5sums, "No md5sum found for FB:FBrf00000001 after empty update"
        md5sum_FB_after = md5sums[0]["md5sum"]
        assert md5sum_FB_after == md5sum_FB, "md5sum changed after empty update"


    def test_load_database_md5data(self, db): # noqa
        # Data Insertion
        with db.begin():
            # Insert 'FB' mod
            db.execute(
                text("INSERT INTO mod (abbreviation, short_name, full_name, date_created) VALUES (:abbr, :short_name, :full_name, now())"),
                {"abbr": "FB", "short_name": "FlyBase", "full_name": "FlyBase"}
            )
            mod_results = db.execute(text("SELECT abbreviation, mod_id FROM mod WHERE abbreviation = :abbr"), {"abbr": "FB"})
            ids = mod_results.mappings().fetchall()
            if not ids:
                raise ValueError("Mod 'FB' not found after insertion.")
            mod_id_FB = ids[0]["mod_id"]

            # Insert 'XB' mod
            db.execute(
                text("INSERT INTO mod (abbreviation, short_name, full_name, date_created) VALUES (:abbr, :short_name, :full_name, now())"),
                {"abbr": "XB", "short_name": "Xenbase", "full_name": "Xenbase"}
            )
            mod_results = db.execute(text("SELECT abbreviation, mod_id FROM mod WHERE abbreviation = :abbr"), {"abbr": "XB"})
            ids = mod_results.mappings().fetchall()
            if not ids:
                raise ValueError("Mod 'XB' not found after insertion.")
            mod_id_XB = ids[0]["mod_id"]

            # Insert reference
            db.execute(
                text("INSERT INTO reference (title, curie, date_created) VALUES (:title, :curie, now())"),
                {"title": "Bob", "curie": "AGR:AGR-Reference-0000808175"}
            )
            ref_results = db.execute(text("SELECT reference_id FROM reference WHERE curie = :curie"), {"curie": "AGR:AGR-Reference-0000808175"})
            refs = ref_results.mappings().fetchall()
            if not refs:
                raise ValueError("Reference not found after insertion.")
            reference_id = refs[0]["reference_id"]

            # Insert cross_references
            cross_refs = [
                {"reference_id": reference_id, "curie": "FB:FBrf0001", "curie_prefix": "FB"},
                {"reference_id": reference_id, "curie": "PMID:0001", "curie_prefix": "PMID"},
                {"reference_id": reference_id, "curie": "Xenbase:XB-ART-0001", "curie_prefix": "Xenbase"},
            ]
            for cr in cross_refs:
                db.execute(
                    text("INSERT INTO cross_reference (reference_id, curie, curie_prefix, date_created) VALUES (:reference_id, :curie, :curie_prefix, now())"),
                    cr
                )

            # Insert reference_mod_md5sum
            md5sums = [
                {"reference_id": reference_id, "mod_id": mod_id_FB, "md5sum": "TEST-md5sum-FB"},
                {"reference_id": reference_id, "mod_id": None, "md5sum": "TEST-md5sum-PMID"},
                {"reference_id": reference_id, "mod_id": mod_id_XB, "md5sum": "TEST-md5sum-XB"},
            ]
            for md5 in md5sums:
                db.execute(
                    text("INSERT INTO reference_mod_md5sum (reference_id, mod_id, md5sum, date_updated) VALUES (:reference_id, :mod_id, :md5sum, now())"),
                    md5
                )

        # Call the function under test, ensuring data is committed and visible
        mods = ["FB", "XB", "PMID", "TEST"]
        dict_md5sum = load_database_md5data(mods)
        print(dict_md5sum)

        # Assertions
        assert dict_md5sum['FB']['FB:FBrf0001'] == 'TEST-md5sum-FB'
        assert dict_md5sum['XB']['Xenbase:XB-ART-0001'] == 'TEST-md5sum-XB'
        assert dict_md5sum['PMID']['PMID:0001'] == 'TEST-md5sum-PMID'
