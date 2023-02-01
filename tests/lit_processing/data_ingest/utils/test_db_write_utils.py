import logging
from os import path

from agr_literature_service.api.models import CrossReferenceModel, ReferenceModel, \
    AuthorModel, ModModel, ModCorpusAssociationModel, MeshDetailModel, \
    ReferenceCommentAndCorrectionModel, ReferenceModReferencetypeAssociationModel
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_references_by_curies, get_pmid_to_reference_id
from agr_literature_service.lit_processing.data_ingest.utils.db_write_utils import \
    add_cross_references, update_authors, update_mod_corpus_associations, \
    update_mod_reference_types, add_mca_to_existing_references, \
    update_comment_corrections, update_mesh_terms, update_cross_reference, \
    mark_false_positive_papers_as_out_of_corpus, mark_not_in_mod_papers_as_out_of_corpus

from ....fixtures import db, load_sanitized_references, populate_test_mod_reference_types # noqa

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class TestDbReadUtils:

    def test_dqm_update_functions(self, db, load_sanitized_references, populate_test_mod_reference_types): # noqa

        refs = db.query(ReferenceModel).order_by(ReferenceModel.curie).all()
        ref_curie_list = [refs[0].curie, refs[1].curie]
        cross_references_to_add = [{'reference_curie': refs[0].curie,
                                    'curie': 'ISBN:88888',
                                    'pages': {}},
                                   {'reference_curie': refs[1].curie,
                                    'curie': 'ISBN:66666',
                                    'pages': {}}]
        reference_id = refs[0].reference_id
        reference_id2 = refs[1].reference_id

        ## test add_cross_references() function
        add_cross_references(cross_references_to_add, ref_curie_list, logger)
        db.commit()
        for x in db.query(CrossReferenceModel).filter(CrossReferenceModel.curie.like('NLM:%')).all():
            if x.reference == reference_id:
                assert x.curie == 'ISBN:88888'
            elif x.reference == reference_id2:
                assert x.curie == 'ISBN:66666'

        ## test update_authors()
        db_entries = get_references_by_curies(db, [refs[0].curie, refs[1].curie])
        db_entry = db_entries[refs[0].curie]
        author_list_in_json = [
            {
                "authorRank": 1,
                "firstname": "Hello",
                "lastname": "World",
                "name": "Hello World"},
            {
                "authorRank": 2,
                "firstname": "Hello",
                "lastname": "There",
                "name": "Hello There"
            }
        ]
        update_authors(db, reference_id, db_entry.get('author', []), author_list_in_json,
                       logger)
        db.commit()
        for x in db.query(AuthorModel).filter_by(reference_id=reference_id).all():
            if x.order == 1:
                assert x.name == 'Hello World'
                assert x.last_name == 'World'
            elif x.order == 2:
                assert x.name == 'Hello There'
                assert x.last_name == 'There'

        mod_to_mod_id = dict([(x.abbreviation, x.mod_id) for x in db.query(ModModel).all()])

        ## test update_mod_corpus_associations()
        mod_corpus_associations = [
            {
                "corpus": False,
                "mod_abbreviation": "SGD",
                "mod_corpus_sort_source": "dqm_files"
            }
        ]
        update_mod_corpus_associations(db, mod_to_mod_id, reference_id,
                                       db_entry.get('mod_corpus_association', []),
                                       mod_corpus_associations, logger)
        db.commit()
        mca = db.query(ModCorpusAssociationModel).filter_by(reference_id=reference_id, corpus=False).first()
        for mod in mod_to_mod_id:
            if mod_to_mod_id[mod] == mca.mod_id:
                assert mod == 'SGD'

        ## test update_mod_reference_types()
        mrt_rows = db.query(ReferenceModReferencetypeAssociationModel).filter_by(reference_id=reference_id).all()
        assert len(mrt_rows) == 1
        mod_reference_types = [
            {
                "referenceType": "Journal",
                "source": "SGD"
            }
        ]
        update_mod_reference_types(db, reference_id,
                                   db_entry.get('mod_referencetypes', []),
                                   mod_reference_types, {'Journal'}, logger)
        db.commit()
        mrt_rows = db.query(ReferenceModReferencetypeAssociationModel).filter_by(reference_id=reference_id).order_by(
            ReferenceModReferencetypeAssociationModel.reference_mod_referencetype_id).all()
        assert len(mrt_rows) == 2
        assert mrt_rows[0].mod_referencetype.mod.abbreviation == 'ZFIN'
        assert mrt_rows[1].mod_referencetype.mod.abbreviation == 'SGD'

        ## test mark_not_in_mod_papers_as_out_of_corpus()
        mod_corpus_associations = [
            {
                "corpus": True,
                "mod_abbreviation": "ZFIN",
                "mod_corpus_sort_source": "dqm_files"
            }
        ]

        update_mod_corpus_associations(db, mod_to_mod_id, reference_id,
                                       db_entry.get('mod_corpus_association', []),
                                       mod_corpus_associations, logger)
        db.commit()

        cr = db.query(CrossReferenceModel).filter_by(
            reference_id=reference_id, curie_prefix='ZFIN', is_obsolete=False).one_or_none()
        mod_xref_id = cr.curie
        missing_papers_in_mod = [(mod_xref_id, 'test_AGR_ID', None)]
        mark_not_in_mod_papers_as_out_of_corpus('ZFIN', missing_papers_in_mod)

        mod_id = mod_to_mod_id['ZFIN']
        mca = db.query(ModCorpusAssociationModel).filter_by(
            reference_id=reference_id, mod_id=mod_id).one_or_none()
        assert mca.corpus is False


    def test_pubmed_search_update_functions(self, db, load_sanitized_references): # noqa

        ## test add_mca_to_existing_references()
        r = db.query(ReferenceModel).first()
        add_mca_to_existing_references(db, [r.curie], 'XB', logger)
        db.commit()
        mca_rows = db.query(ModCorpusAssociationModel).filter_by(reference_id=r.reference_id).order_by(
            ModCorpusAssociationModel.mod_corpus_association_id).all()
        assert len(mca_rows) == 2
        assert mca_rows[1].corpus is None
        assert mca_rows[1].mod_corpus_sort_source == 'mod_pubmed_search'

        ## getting things ready for pubmed update specific functions
        # base_path = environ.get('XML_PATH')
        log_file = path.join(path.dirname(__file__), 'pubmed_update.log')
        fw = open(log_file, "w")
        update_log = {}
        field_names_to_report = ['comment_erratum', 'mesh_term', 'doi', 'pmcid', 'pmids_updated']
        for field_name in field_names_to_report:
            if field_name == 'pmids_updated':
                update_log[field_name] = []
            else:
                update_log[field_name] = 0
        pmid = "33622238"
        pmid_to_reference_id = {}
        reference_id_to_pmid = {}
        mod = 'ZFIN'
        get_pmid_to_reference_id(db, mod, pmid_to_reference_id, reference_id_to_pmid)
        reference_id = pmid_to_reference_id[pmid]

        ## test update_comment_corrections()
        reference_ids_to_comment_correction_type = {}
        comment_correction_in_json = {
            "ErratumIn": [
                "34354223"
            ]
        }
        update_comment_corrections(db, fw, pmid, reference_id, pmid_to_reference_id,
                                   reference_ids_to_comment_correction_type,
                                   comment_correction_in_json, update_log)
        db.commit()

        rcc = db.query(ReferenceCommentAndCorrectionModel).filter_by(reference_id_to=reference_id).one_or_none()
        assert rcc.reference_comment_and_correction_type == 'ErratumFor'
        assert rcc.reference_id_from == pmid_to_reference_id['34354223']

        ## test update_mesh_terms()
        mt_rows = db.query(MeshDetailModel).filter_by(reference_id=reference_id).all()
        assert len(mt_rows) == 24
        mesh_terms_in_json_data = [
            {
                "meshHeadingTerm": "Fibroblast Growth Factors",
                "meshQualifierTerm": "genetics"
            }
        ]
        update_mesh_terms(db, fw, pmid, reference_id, [],
                          mesh_terms_in_json_data, update_log)
        db.commit()
        mt_rows = db.query(MeshDetailModel).filter_by(reference_id=reference_id).order_by(
            MeshDetailModel.mesh_detail_id).all()
        assert len(mt_rows) == 25
        assert mt_rows[24].heading_term == 'Fibroblast Growth Factors'
        assert mt_rows[24].qualifier_term == 'genetics'

        ## test update_cross_reference()
        doi_db = "DOI:10.1186/s12576-021-00791-4"
        doi_json = "DOI:10.1186/s12576-021-00791-x"
        doi_list_in_db = []
        pmcid_db = ""
        pmcid_json = "PMCID:PMC667788"
        pmcid_list_in_db = []
        update_cross_reference(db, fw, pmid, reference_id, doi_db, doi_list_in_db, doi_json,
                               pmcid_db, pmcid_list_in_db, pmcid_json, update_log)
        db.commit()
        for x in db.query(CrossReferenceModel).filter_by(reference_id=reference_id).all():
            if x.curie.startswith('PMCID:'):
                assert x.curie == 'PMCID:PMC667788'
            elif x.curie.startswith('DOI:'):
                assert x.curie == 'DOI:10.1186/s12576-021-00791-4'

        ## test mark_false_positive_papers_as_out_of_corpus()
        mca_rows = db.execute("SELECT cr.curie FROM cross_reference cr, mod_corpus_association mca, "
                              "mod m WHERE mca.reference_id = cr.reference_id "
                              "AND mca.mod_id = m.mod_id "
                              "AND m.abbreviation = 'XB'").fetchall()
        assert len(mca_rows) > 0

        fp_pmids = set()
        for x in mca_rows:
            fp_pmids.add(x[0].replace("PMID:", ""))
        mark_false_positive_papers_as_out_of_corpus(db, 'XB', fp_pmids)

        cr_rows = db.execute("SELECT is_obsolete FROM cross_reference "
                             "WHERE curie_prefix = 'Xenbase'").fetchall()
        for x in cr_rows:
            assert x[0] is True
