from agr_literature_service.api.models import ResourceModel
from agr_literature_service.lit_processing.data_ingest.post_resource_to_db import process_resource_entry
# from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.dqm_resource_update_utils import \
#    process_update_resource

from ....fixtures import db # noqa

from agr_literature_service.lit_processing.utils.resource_reference_utils import \
    dump_xrefs, load_xref_data, reset_xref


class TestParseDqmJsonResource:
    def initialise_data(self, db):  # noqa
        """
        Initial data to check and also to check update mechanisms.
        NOTE: primaryId is not stored any where here.
              This is used in the sanitize json bit.
        """
        print("Start Initialisation")
        reset_xref()
        load_xref_data(db, 'resource')
        new_data = [
            {"primaryId" : "ZFIN:prim1",
             "title" : "title1",
             "medlineAbbreviation" : "medline1",
             "isoAbbreviation" : "iso1",
             "editorsOrAuthors": [
                 {"authorRank": 1,
                  "firstName": "f1",
                  "lastName": "last1",
                  "name": "f1 last1",
                  "referenceId": "FB:FBmultipub_1"},
                 {"authorRank": 2,
                  "firstName": "f2",
                  "lastName": "last2",
                  "name": "f2 last2",
                  "referenceId": "FB:FBmultipub_2"}],
             "crossReferences" : [
                 {"id" : "ZFIN:ZDB-JRNL-001-1",
                  "pages" : ["journal", "journal/references"]}]},

            {"primaryId" : "ZFIN:prim2",
             "title" : "title2",
             "medlineAbbreviation" : "medline2",
             "isoAbbreviation" : "iso2",
             "printISSN" : "issn2",
             "onlineISSN" : "online1sso2",
             "crossReferences" : [
                 {"id" : "ZFIN:ZDB-JRNL-001-2",
                  "pages" : ["journal", "journal/references"]}]},
        ]

        count = 0
        for entry in new_data:
            count += 1
            okay, message = process_resource_entry(db, entry)
            assert message == f"ZFIN:prim{count}\tAGRKB:10200000000000{count}\n"
            assert okay
        dump_xrefs()
        print("End Initialisation")

    # @pytest.fixture
    def test_zfin_resource_parse_new(self, db): # noqa
        """
        Check the inital data gets loaded correctly.
        """
        self.initialise_data(db)

        res = db.query(ResourceModel).filter_by(title='title1').one()

        assert res.title_abbreviation == "iso1"

        # check the cross reference
        assert res.cross_reference[0].curie_prefix == "ZFIN"
        assert res.cross_reference[0].curie == "ZFIN:ZDB-JRNL-001-1"
        assert not res.cross_reference[0].is_obsolete
        assert res.cross_reference[0].pages[0] == 'journal'
        assert res.cross_reference[0].pages[1] == 'journal/references'

        # check the editors
        if res.editor[0].first_name == "f1":
            assert res.editor[0].last_name == "last1"
            assert res.editor[0].name == "f1 last1"
            assert res.editor[0].order == 1
            assert res.editor[1].last_name == "last2"
            assert res.editor[1].name == "f2 last2"
            assert res.editor[1].order == 2
        else:
            assert res.editor[0].first_name == "f2"
            assert res.editor[0].last_name == "last2"
            assert res.editor[0].name == "f2 last2"
            assert res.editor[0].order == 2
            assert res.editor[1].last_name == "last1"
            assert res.editor[1].name == "f1 last1"
            assert res.editor[1].order == 1


    def test_zfin_resource_bad_key_process(self, db): # noqa
        self.initialise_data(db)
        bad_key_data = [
            {"primaryId" : "ZFIN:prim3",
             "title" : "another title",
             "medlineAbbreviation" : "medline1",
             "UnKnownKey" : "iso1",
             "isoAbbreviation" : "new iso",
             "editorsOrAuthors": [
                 {"authorRank": 1,
                  "firstName": "f3",
                  "lastName": "last3",
                  "name": "diff f3 last3",
                  "referenceId": "FB:FBmultipub_3"},
                 {"authorRank": 2,
                  "firstName": "f4",
                  "lastName": "last4",
                  "name": "diff f4 last4",
                  "referenceId": "FB:FBmultipub_2"}],
             "crossReferences" : [
                 {"id" : "ZFIN:ZDB-JRNL-001-TZRB1",
                  "pages" : ["journal", "journal/references"]},
                 {"id" : "ZFIN:ZDB-JRNL-001-TZRB2",
                  "pages" : ["journal", "journal/references"]}]}
        ]
        count = 0
        for entry in bad_key_data:
            count += 1

            okay, message = process_resource_entry(db, entry)
            assert not okay
            assert message.startswith("An error occurred when adding resource")
            assert 'UnKnownKey' in message

    def test_zfin_resource_double_prefix_process_bad(self, db): # noqa
        self.initialise_data(db)
        dup_xref_data = [
            {"primaryId" : "ZFIN:prim3",
             "title" : "another title",
             "medlineAbbreviation" : "medline1",
             "isoAbbreviation" : "iso1",
             "editorsOrAuthors": [
                 {"authorRank": 1,
                  "firstName": "f3",
                  "lastName": "last3",
                  "name": "diff f3 last3",
                  "referenceId": "FB:FBmultipub_3"},
                 {"authorRank": 2,
                  "firstName": "f4",
                  "lastName": "last4",
                  "name": "diff f4 last4",
                  "referenceId": "FB:FBmultipub_2"}],
             "crossReferences" : [
                 {"id" : "ZFIN:ZDB-JRNL-001-TZRB1",
                  "pages" : ["journal", "journal/references"]},
                 {"id" : "ZFIN:ZDB-JRNL-001-TZRB2",
                  "pages" : ["journal", "journal/references"]}]}
        ]
        count = 0
        for entry in dup_xref_data:
            count += 1

            okay, message = process_resource_entry(db, entry)
            assert not okay
            assert message == "Not allowed same prefix ZFIN multiple time for the same resource"

    def test_title_based_merge_prevents_duplicates(self, db):  # noqa
        """
        Test that when two MODs submit the same journal with different identifiers,
        the second submission merges cross-references with the first instead of
        creating a duplicate resource.

        This simulates the scenario where:
        - FB submits "Journal of xenobiotics" with FB:FBmultipub_11437
        - ZFIN submits "Journal of xenobiotics" with ZFIN:ZDB-JRNL-* + NLM + ISSN
        - The ZFIN xrefs should be added to the FB resource, not create a duplicate
        """
        reset_xref()
        load_xref_data(db, 'resource')

        # First MOD (FB) creates the resource with FB-specific identifier
        fb_entry = {
            "primaryId": "FB:FBmultipub_99999",
            "title": "Test Journal for Title Merge",
            "medlineAbbreviation": "Test J Title Merge",
            "crossReferences": [
                {"id": "FB:FBmultipub_99999", "pages": ["journal"]}
            ]
        }
        okay, message = process_resource_entry(db, fb_entry)
        assert okay, f"FB entry should be created successfully: {message}"

        # Get the resource that was created
        res_after_fb = db.query(ResourceModel).filter_by(
            title='Test Journal for Title Merge'
        ).one()
        resource_id = res_after_fb.resource_id
        initial_xref_count = len(res_after_fb.cross_reference)
        assert initial_xref_count == 1, "Should have 1 xref from FB"
        assert res_after_fb.cross_reference[0].curie == "FB:FBmultipub_99999"

        # Reload xref data to include the newly created resource
        load_xref_data(db, 'resource')

        # Second MOD (ZFIN) submits the same journal with different identifiers
        # This should NOT create a new resource, but should add xrefs to existing
        zfin_entry = {
            "primaryId": "ZFIN:ZDB-JRNL-999999-1",
            "title": "Test Journal for Title Merge",  # Same title!
            "medlineAbbreviation": "Test J Title Merge",
            "crossReferences": [
                {"id": "ZFIN:ZDB-JRNL-999999-1", "pages": ["journal"]},
                {"id": "NLM:999999999", "pages": []},
                {"id": "ISSN:9999-9999", "pages": []}
            ]
        }
        okay, message = process_resource_entry(db, zfin_entry)
        assert okay, f"ZFIN entry should merge successfully: {message}"
        assert "Title-merged" in message, f"Should indicate title-based merge: {message}"

        # Verify no duplicate was created - should still be just one resource
        resources = db.query(ResourceModel).filter_by(
            title='Test Journal for Title Merge'
        ).all()
        assert len(resources) == 1, \
            f"Should have exactly 1 resource, not duplicates. Found {len(resources)}"

        # Verify the resource now has cross-references from both MODs
        res_after_merge = db.query(ResourceModel).filter_by(
            title='Test Journal for Title Merge'
        ).one()
        assert res_after_merge.resource_id == resource_id, \
            "Should be the same resource, not a new one"

        xref_curies = {xref.curie for xref in res_after_merge.cross_reference}
        assert "FB:FBmultipub_99999" in xref_curies, "Should still have FB xref"
        assert "ZFIN:ZDB-JRNL-999999-1" in xref_curies, "Should have ZFIN xref added"
        assert "NLM:999999999" in xref_curies, "Should have NLM xref added"
        assert "ISSN:9999-9999" in xref_curies, "Should have ISSN xref added"
        assert len(xref_curies) == 4, \
            f"Should have 4 xrefs total (1 FB + 3 ZFIN). Found: {xref_curies}"
