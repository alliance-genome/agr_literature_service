# import os
# import pytest
# from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
# from agr_literature_service.lit_processing.data_ingest.dqm_ingest.# # sort_dqm_json_resource_updates \
#     import update_sanitized_resources
# from agr_literature_service.lit_processing.data_ingest.post_resource_to_db \
#    import post_resources
from agr_literature_service.api.models import ResourceModel
from agr_literature_service.lit_processing.data_ingest.post_resource_to_db import process_resource_entry
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.sort_dqm_json_resource_updates import process_update_resource

from ....fixtures import db # noqa
from fastapi.encoders import jsonable_encoder
# import json


class TestParseDqmJsonResource:
    # @pytest.fixture
    def test_zfin_resource_parse_new(self, db): # noqa
        new_data = [
            {"primaryId" : "ZFIN:prim1",
             "title" : "title1",
             "medlineAbbreviation" : "medline1",
             "isoAbbreviation" : "iso1",
             "editorsOrAuthors": [
                 {"authorRank": 1,
                  "firstName": "f1",
                  "lastName": "last1",
                  "name": "diff f1 last1",
                  "referenceId": "FB:FBmultipub_1"},
                 {"authorRank": 2,
                  "firstName": "f2",
                  "lastName": "last2",
                  "name": "diff f2 last2",
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
            okay, message = process_resource_entry(db, entry, [])
            assert okay
            assert message == f"ZFIN:prim{count}\tAGR:AGR-Resource-000000000{count}\n"

        res = db.query(ResourceModel).filter_by(title='title1').one()
        print(dir(res))
        print(res)
        assert not res.print_issn
        assert res.medline_abbreviation == "medline1"
        assert res.iso_abbreviation == "iso1"
        print(res.cross_reference[0])
        print(dir(res.cross_reference[0]))
        print(res.editor[0])
        print(dir(res.editor[0]))

        # check the cross reference
        assert res.cross_reference[0].curie == "ZFIN:ZDB-JRNL-001-1"
        assert not res.cross_reference[0].is_obsolete
        assert res.cross_reference[0].pages[0] == 'journal'
        assert res.cross_reference[0].pages[1] == 'journal/references'

        print(res.editor[0])
        print(res.editor[1])
        # check the editors
        if res.editor[0].first_name == "f1":
            assert res.editor[0].last_name == "last1"
            assert res.editor[0].name == "diff f1 last1"
            assert res.editor[0].order == 1
            assert res.editor[1].last_name == "last2"
            assert res.editor[1].name == "diff f2 last2"
            assert res.editor[1].order == 2
        else:
            assert res.editor[0].first_name == "f2"
            assert res.editor[0].last_name == "last2"
            assert res.editor[0].name == "diff f2 last2"
            assert res.editor[0].order == 2
            assert res.editor[1].last_name == "last1"
            assert res.editor[1].name == "diff f1 last1"
            assert res.editor[1].order == 1
        duplicate_data = [
            {"primaryId" : "ZFIN:prim1",
             "title" : "title1",
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
                 {"id" : "ZFIN:ZDB-JRNL-001-1",
                  "pages" : ["journal", "journal/references"]}]},
        ]

        count = 0
        for entry in duplicate_data:
            count += 1
            okay, message = process_resource_entry(db, entry, [])
            assert not okay
            assert message.startswith("An error occurred when adding resource")

        bad_key_data = [
            {"primaryId" : "ZFIN:prim1",
             "title" : "title1",
             "medlineAbbreviation" : "medline1",
             "UnKnownKey" : "iso1",
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
        for entry in bad_key_data:
            count += 1
            okay, message = process_resource_entry(db, entry, [])
            assert not okay
            assert message.startswith("An error occurred when adding resource")

        update_data = [
            {"primaryId" : "ZFIN:prim1",
             "title" : "title1",
             "medlineAbbreviation" : "new medline1",
             "isoAbbreviation" : "new iso1",
             "editorsOrAuthors": [
                 {"authorRank": 1,
                  "firstName": "new f1",
                  "lastName": "new last1",
                  "name": "new diff f1 last1",
                  "referenceId": "FB:FBmultipub_1"},
                 {"authorRank": 2,
                  "firstName": "new f2",
                  "lastName": "new last2",
                  "name": "new diff f2 last2",
                  "referenceId": "FB:FBmultipub_2"}],
             "crossReferences" : [
                 {"id" : "NEW ZFIN:ZDB-JRNL-001-1",
                  "pages" : ["new journal", "new journal/references"]},
                 {"id" : "ZFIN:ZDB-JRNL-001-1",
                  "pages" : ["new journal", "new journal/references"]}]}]
        try:
            process_update_resource(db, update_data[0], "AGR:AGR-Resource-0000000001")
        except Exception as e:
            assert e == 'Exception'
        try:
            res = db.query(ResourceModel).filter_by(title='title1').one()
        except Exception as e:
            assert e == 'Exception2'
        print(dir(res))
        print(res)
        print(jsonable_encoder(res))
        assert not res.print_issn
        assert res.medline_abbreviation == "new medline1"
        assert res.iso_abbreviation == "new iso1"
        print(res.cross_reference[0])
        print(dir(res.cross_reference[0]))
        for editor in res.editor:
            print(editor)

        for xref in res.cross_reference:
            print(xref)
        # New cross reference
        assert res.cross_reference[1].curie == "NEW ZFIN:ZDB-JRNL-001-1"
        assert not res.cross_reference[1].is_obsolete
        assert res.cross_reference[1].pages[0] == 'new journal'
        assert res.cross_reference[1].pages[1] == 'new journal/references'
        # NOTE: Old one stays unchanged
        assert res.cross_reference[0].curie == "ZFIN:ZDB-JRNL-001-1"
        assert not res.cross_reference[0].is_obsolete
        assert res.cross_reference[0].pages[0] == 'journal'
        assert res.cross_reference[0].pages[1] == 'journal/references'

        assert 1 == 2
