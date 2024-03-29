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

        assert not res.print_issn
        assert res.medline_abbreviation == "medline1"
        assert res.iso_abbreviation == "iso1"

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
