import json
from os import path

from agr_literature_service.api.models import ReferenceModel, CrossReferenceModel, CitationModel
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_references_single_mod import\
    update_database, update_reference_table, generate_pmids_with_info
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_pmid_to_reference_id, get_author_data
from ....fixtures import load_sanitized_references, populate_test_mod_reference_types, db # noqa


class TestPubmedUpdateReferenceSingleMod:

    def test_update_database(self, db, load_sanitized_references): # noqa

        ## getting things ready for pubmed update specific functions
        log_file = path.join(path.dirname(__file__), 'pubmed_update.log')
        fw = open(log_file, "w")
        update_log = {}
        bad_date_published = {}
        field_names_to_report = ['title', 'volume', 'issue_name', 'page_range', 'citation',
                                 'language', 'pmids_updated']
        for field_name in field_names_to_report:
            if field_name == 'pmids_updated':
                update_log[field_name] = []
            else:
                update_log[field_name] = 0
        pmid_to_reference_id = {}
        reference_id_to_pmid = {}
        mod = 'ZFIN'
        pmid = '33622238'
        pmid2 = '34354223'
        get_pmid_to_reference_id(db, mod, pmid_to_reference_id, reference_id_to_pmid)
        # pmid_to_reference_id = {'33622238': 2778, '34354223': 2779, '35151207': 2780}

        ## test update_database()
        old_md5sum = {}
        new_md5sum = {'PMID:33622238': '51ff265baf9919f9f944f5af8d17d036',
                      'PMID:34354223': 'c306828a3ff618b21c95a0b8b3e67041'}
        pmids_with_json_updated = []
        reference_id_list = [pmid_to_reference_id[pmid], pmid_to_reference_id[pmid2]]
        json_path = path.join(path.dirname(path.abspath(__file__)), "../../sample_data/pubmed_json/")
        for x in db.query(ReferenceModel).all():
            if x.reference_id == pmid_to_reference_id[pmid]:
                assert x.page_range == '88'
                assert x.volume == '888'
                assert "OLD: " in x.title
            elif x.reference_id == pmid_to_reference_id[pmid2]:
                assert x.issue_name == "66"
                assert "OLD2: " in x.title

        update_database(fw, mod, reference_id_list, reference_id_to_pmid,
                        pmid_to_reference_id, update_log, new_md5sum,
                        old_md5sum, json_path, pmids_with_json_updated,
                        bad_date_published)

        for x in db.query(ReferenceModel).all():
            if x.reference_id == pmid_to_reference_id[pmid]:
                assert x.page_range == '8'
                assert x.volume == '71'
                assert x.title.startswith("Role of PGE")
                assert "OLD: " not in x.title
                # get citation
                cit = db.query(CitationModel).filter_by(citation_id=x.citation_id).one_or_none()
                if not cit:
                    assert "No citation created" == "AHH"
                else:
                    assert cit.citation == "Shin-Ichiro Karaki; Ryo Tanaka, (2021) Role of PGE<sub>2</sub> in colonic '\n 'motility: PGE<sub>2</sub> attenuates spontaneous contractions of circular '\n 'smooth muscle via EP<sub>4</sub> receptors in the rat colon..  71 (1): 8"
            elif x.reference_id == pmid_to_reference_id[pmid2]:
                assert x.issue_name == "1"
                assert x.title.startswith("Mapping lung squamous cell")
                assert "OLD2: " not in x.title

        ## test create_new_citation()
        # journal = None
        # if ref.resource_id:
        #    res = db.query(ResourceModel).filter_by(resource_id=ref.resource_id).one_or_none()
        #    journal = res.title
        # reference_id_to_authors = get_author_data(db, mod, reference_id_list, 50)
        # authors = reference_id_to_authors.get(pmid_to_reference_id[pmid])
        # citation = create_new_citation(authors, ref.date_published, ref.title,
        #                               journal, ref.volume, ref.issue_name, ref.page_range)

        ## test generate_pmids_with_info()
        (ref_id_list, pmid_to_md5sum) = generate_pmids_with_info([pmid, pmid2],
                                                                 old_md5sum, new_md5sum,
                                                                 pmid_to_reference_id)
        assert ref_id_list == reference_id_list
        assert pmid_to_md5sum == new_md5sum


    def test_update_reference_table(self, db, load_sanitized_references): # noqa

        pmid = '33622238'
        json_file = path.join(path.dirname(path.abspath(__file__)),
                              "../../sample_data/pubmed_json",
                              pmid + ".json")
        json_data = json.load(open(json_file))

        log_file = path.join(path.dirname(__file__), 'pubmed_update.log')
        fw = open(log_file, "w")
        update_log = {}
        bad_date_published = {}
        field_names_to_report = ['title', 'volume', 'issue_name', 'page_range', 'citation',
                                 'language', 'pmids_updated']
        for field_name in field_names_to_report:
            if field_name == 'pmids_updated':
                update_log[field_name] = []
            else:
                update_log[field_name] = 0

        crossRef = db.query(CrossReferenceModel).filter_by(curie='PMID:' + pmid).one_or_none()
        reference_id = crossRef.reference_id

        for x in db.query(ReferenceModel).all():
            if x.reference_id == reference_id:
                assert x.page_range == '88'
                assert x.volume == '888'
                assert "OLD: " in x.title

        ref = db.query(ReferenceModel).filter_by(reference_id=reference_id).one_or_none()
        mod = 'ZFIN'
        reference_id_list = [reference_id]
        reference_id_to_authors = get_author_data(db, mod, reference_id_list, 50)
        authors = reference_id_to_authors.get(reference_id, [])
        update_reference_table(db, fw, pmid, ref, json_data, None, None,
                               authors, bad_date_published, update_log, 1)
        db.commit()

        for x in db.query(ReferenceModel).all():
            if x.reference_id == reference_id:
                assert x.page_range != '88'
                assert x.volume != '888'
                assert "OLD: " not in x.title
