import json
from os import path

from agr_literature_service.api.models import CrossReferenceModel, ReferenceModel,\
    AuthorModel, ModCorpusAssociationModel, ModReferenceTypeModel, MeshDetailModel,\
    ModModel, ReferenceCommentAndCorrectionModel
from agr_literature_service.lit_processing.data_ingest.post_reference_to_db import \
    insert_reference, get_journal_data, get_orcid_data, insert_authors, \
    insert_cross_references, get_doi_data, set_primaryId, insert_mesh_terms, \
    insert_mod_reference_types, insert_mod_corpus_associations, \
    read_data_and_load_references, insert_comment_corrections
from agr_literature_service.lit_processing.tests.mod_populate_load import post_mods
from ...fixtures import db, cleanup_tmp_files_when_done # noqa


class TestPostReferenceToDb:

    ## post_references() has been tested in "data_export" tests

    def test_read_data_and_load_references(self, db): # noqa

        post_mods()
        json_file = path.join(path.dirname(path.abspath(__file__)), "../sample_data",
                              "sanitized_references", "REFERENCE_PUBMED_ZFIN.json")
        json_data = json.load(open(json_file))
        journal_to_resource_id = get_journal_data(db)
        orcid_dict = get_orcid_data(db)
        newly_added_orcid = []
        doi_to_reference_id = get_doi_data(db)
        mod_to_mod_id = dict([(x.abbreviation, x.mod_id) for x in db.query(ModModel).all()])

        ## test read_data_and_load_references()
        read_data_and_load_references(db, json_data, journal_to_resource_id,
                                      orcid_dict, newly_added_orcid,
                                      doi_to_reference_id,
                                      mod_to_mod_id, True)
        refs = db.query(ReferenceModel).all()
        assert len(refs) == 3
        assert 'lung squamous cell carcinoma pathogenesis' in refs[1].title
        assert 'The role of the histone methyltransferase' in refs[2].title
        assert refs[1].volume == '4'
        assert refs[2].volume == '598'

        ## test set_primaryId()
        primaryId = set_primaryId(json_data[0])
        assert primaryId == 'PMID:33622238'

        ## test insert_comment_corrections()
        reference_id = refs[0].reference_id
        correctionData = {
            "RepublishedFrom": [
                "34354223"
            ]
        }
        insert_comment_corrections(db, primaryId, reference_id, correctionData)
        db.commit()
        cc = db.query(ReferenceCommentAndCorrectionModel).first()
        assert cc.reference_id_from == reference_id
        assert cc.reference_id_to == refs[1].reference_id
        assert cc.reference_comment_and_correction_type == 'RepublishedFrom'

    def test_load_one_reference(self, db, cleanup_tmp_files_when_done): # noqa

        post_mods()

        json_file = path.join(path.dirname(path.abspath(__file__)), "../sample_data",
                              "sanitized_references", "REFERENCE_PUBMED_ZFIN.json")
        json_data = json.load(open(json_file))
        entry = json_data[0]

        primaryId = set_primaryId(entry)

        ## test get_journal_data()
        journal_to_resource_id = get_journal_data(db)
        assert type(journal_to_resource_id) == dict

        ## test insert_reference()
        reference_id = insert_reference(db, primaryId, journal_to_resource_id, entry)
        assert type(reference_id) == int
        db.commit()
        x = db.query(ReferenceModel).filter_by(title=entry['title']).one_or_none()

        assert x is not None
        assert x.reference_id == reference_id

        ## test insert_authors()
        orcid_dict = get_orcid_data(db)
        newly_added_orcid = []
        insert_authors(db, primaryId, reference_id, entry['authors'],
                       orcid_dict, newly_added_orcid)
        db.commit()
        x = db.query(AuthorModel).filter_by(last_name='Karaki', order=1).one_or_none()
        assert x is not None
        assert x.name == 'Shin-Ichiro Karaki'

        ## test insert_cross_references()
        doi_to_reference_id = get_doi_data(db)
        insert_cross_references(db, primaryId, reference_id, doi_to_reference_id,
                                entry['crossReferences'])
        db.commit()

        crossRefs = db.query(CrossReferenceModel).filter_by(reference_id=reference_id).all()
        for x in crossRefs:
            if x.curie.startswith('PMID:'):
                assert x.curie == 'PMID:33622238'
            elif x.curie.startswith('DOI:'):
                assert x.curie == 'DOI:10.1186/s12576-021-00791-4'
            elif x.curie.startswith('NLM:'):
                assert x.curie == 'NLM:101262417'

        ## test insert_mesh_terms()
        insert_mesh_terms(db, primaryId, reference_id, entry['meshTerms'])
        db.commit()
        mt = db.query(MeshDetailModel).filter_by(reference_id=reference_id).first()
        assert mt.heading_term == 'Animals'

        ## test insert_mod_reference_types()
        insert_mod_reference_types(db, primaryId, reference_id, entry['MODReferenceTypes'])
        db.commit()
        mft = db.query(ModReferenceTypeModel).filter_by(reference_id=reference_id).first()
        assert mft.reference_type == 'Journal'
        assert mft.source == 'ZFIN'

        ## test insert_mod_corpus_associations()
        mod_to_mod_id = dict([(x.abbreviation, x.mod_id) for x in db.query(ModModel).all()])
        insert_mod_corpus_associations(db, primaryId, reference_id, mod_to_mod_id,
                                       entry['modCorpusAssociations'])
        db.commit()
        mca = db.query(ModCorpusAssociationModel).filter_by(reference_id=reference_id).first()
        assert mca.mod_id == mod_to_mod_id['ZFIN']
        assert mca.mod_corpus_sort_source == 'dqm_files'
