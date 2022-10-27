from agr_literature_service.api.models import ResourceModel
from agr_literature_service.lit_processing.data_ingest.post_resource_to_db import process_resource_entry
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.dqm_resource_update_utils import \
    process_update_resource

from ....fixtures import db # noqa

from agr_literature_service.lit_processing.utils.resource_reference_utils import dump_xrefs, load_xref_data, reset_xref


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

    def test_zfin_resource_duplicate_bad(self, db): # noqa
        """
        New resource has cross refernce that is already attached to another resource
        """
        self.initialise_data(db)
        duplicate_data = [
            {"primaryId" : "ZFIN:prim_new",
             "title" : "new title",
             "medlineAbbreviation" : "medline1",
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
                 {"id" : "ZFIN:ZDB-JRNL-001-1",
                  "pages" : ["journal", "journal/references"]}]},
        ]

        count = 0
        for entry in duplicate_data:
            count += 1
            okay, message = process_resource_entry(db, entry)
            print(f"okay = {okay}")
            print(message)
            assert message.startswith("CrossReference with curie = ZFIN:ZDB-JRNL-001-1 already exists with a different resource")
            assert not okay

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

    def test_zfin_resource_two_xrefs_of_same_prefix_update_bad(self, db): # noqa
        """
           It should NOT be okay to add multiple cross references to an existing
           resource.
        """
        self.initialise_data(db)
        update_data = [
            {"crossReferences" : [
                {"id" : "ZFIN:ZDB-JRNL-001-NEW1",
                 "pages" : ["new journal", "new journal/references"]},
                {"id" : "NEWPREFIX:ZDB-JRNL-001-NEW2",
                 "pages" : ["new journal", "new journal/references"]}]}]

        try:
            okay, mess = process_update_resource(db, update_data[0], "AGRKB:102000000000001")
        except Exception as e:
            assert e == 'Exception'
        assert not okay
        assert mess == "Prefix ZFIN is already assigned to for this resource"
        try:
            res = db.query(ResourceModel).filter_by(curie='AGRKB:102000000000001').one()
        except Exception as e:
            assert e == 'Exception2'

        count = 0
        for xref in res.cross_reference:
            if xref.curie == "NEWPREFIX:ZDB-JRNL-001-NEW2":
                assert not xref.is_obsolete
                assert xref.pages[0] == 'new journal'
                assert xref.pages[1] == 'new journal/references'
                count += 1
            elif xref.curie == "ZFIN:ZDB-JRNL-001-1":
                assert not xref.is_obsolete
                assert xref.pages[0] == 'journal'
                assert xref.pages[1] == 'journal/references'
                count += 1
            else:
                assert "unknown xref" == xref.curie
        assert count == 2

    def test_zfin_resource_same_xref_diff_resource(self, db): # noqa
        self.initialise_data(db)        # What happens if we pass the same xrf with a diff resource?
        update_data = [
            {"title" : "new title 3",
             "crossReferences" : [
                 {"id" : "BOB:ShouldBeOkay",
                  "pages" : ["new journal", "new journal/references"]},
                 {"id" : "ZFIN:ZDB-JRNL-001-1",
                  "pages" : ["new journal", "new journal/references"]}]}]
        # try:
        okay, mess = process_update_resource(db, update_data[0], "AGRKB:102000000000002")
        db.flush()
        db.commit()
        assert mess == "Prefix ZFIN is already assigned to for this resource"
        assert not okay

        # except Exception as e:
        #     assert e == 'Exception'
        try:
            res = db.query(ResourceModel).filter_by(curie='AGRKB:102000000000002').one()
        except Exception as e:
            assert e == 'Exception2'

        # So we should have still added BOB:ShouldBeOkay
        # and there was an original ZFIN:ZDB-JRNL-001-2
        # BUT ZFIN:ZDB-JRNL-001-1 will not have been added
        # as it was already assigned to another resource
        count = 0
        for xref in res.cross_reference:
            count += 1
            print(xref.curie)
            if xref.curie == 'ZFIN:ZDB-JRNL-001-2':
                assert xref.pages[0] == 'journal'
            elif xref.curie == 'BOB:ShouldBeOkay':
                assert xref.pages[0] == 'new journal'
            else:
                assert 'UnExpected curie' == xref.curie
        assert count == 2
