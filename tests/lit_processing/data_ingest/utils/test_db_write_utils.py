import logging
from os import path
from unittest.mock import patch
from sqlalchemy import text

from agr_literature_service.api.models import CrossReferenceModel, ReferenceModel, \
    ModModel, ModCorpusAssociationModel, MeshDetailModel, \
    ReferenceRelationModel, ReferenceModReferencetypeAssociationModel, \
    ReferencefileModel, ReferencefileModAssociationModel
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_references_by_curies, get_pmid_to_reference_id
from agr_literature_service.lit_processing.data_ingest.utils.db_write_utils import \
    add_cross_references, update_mod_corpus_associations, \
    update_mod_reference_types, add_mca_to_existing_references, \
    update_reference_relations, update_mesh_terms, \
    mark_false_positive_papers_as_out_of_corpus, \
    mark_not_in_mod_papers_as_out_of_corpus, \
    update_title_for_one_retracted_paper, \
    update_title_for_retracted_papers, \
    cleanup_tags_for_one_retracted_paper, \
    cleanup_tags_for_retracted_papers, \
    set_retraction_status, \
    restore_file_upload_workflow_tags
from agr_literature_service.lit_processing.data_ingest.utils.author import Author, \
    authors_lists_are_equal, authors_have_same_name

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

        db_entries = get_references_by_curies(db, [refs[0].curie, refs[1].curie])
        db_entry = db_entries[refs[0].curie]

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

        ## test update_reference_relations()
        reference_ids_to_reference_relation_type = {}
        reference_relation_in_json = {
            "ErratumIn": [
                "34354223"
            ]
        }
        update_reference_relations(db, fw, pmid, reference_id, pmid_to_reference_id,
                                   reference_ids_to_reference_relation_type,
                                   reference_relation_in_json, update_log)
        db.commit()

        rcc = db.query(ReferenceRelationModel).filter_by(reference_id_to=reference_id).one_or_none()
        assert rcc.reference_relation_type == 'ErratumFor'
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

        ## test mark_false_positive_papers_as_out_of_corpus()
        mca_rows = db.execute(text("SELECT cr.curie FROM cross_reference cr, mod_corpus_association mca, "
                                   "mod m WHERE mca.reference_id = cr.reference_id "
                                   "AND mca.mod_id = m.mod_id "
                                   "AND m.abbreviation = 'XB'")).fetchall()
        assert len(mca_rows) > 0

        fp_pmids = set()
        for x in mca_rows:
            fp_pmids.add(x[0].replace("PMID:", ""))
        mark_false_positive_papers_as_out_of_corpus(db, 'XB', fp_pmids)

        cr_rows = db.execute(text("SELECT is_obsolete FROM cross_reference "
                                  "WHERE curie_prefix = 'Xenbase'")).fetchall()
        for x in cr_rows:
            assert x[0] is True

    def test_author_functions(self):

        test_author_pairs = {
            ("Noda T", "", "", ""): ("Taichi Noda", "Noda", "Taichi", "T"),
            (" Blaha A", "", "", ""): ("Andreas Blaha", "Blaha", "Andreas", "A"),
            ("Hahrami AH", "", "", ""): ("Hahrami A", "Hahrami", "Andreas", "A"),
            ("Kurat CF", "", "", ""): ("Christoph F Kurat", "Kurat", "Christoph Frank", "C"),
            ("Bahrami AH", "", "", ""): ("Amir Houshang Bahrami", "Bahrami", "", ""),
            ("Li H", "", "", ""): ("H Li", "", "", ""),
            ("Anderson,C", "", "", ""): ("Carrie Anderson", "Anderson", "Carrie", "C"),
            ("Wang,Y.", "", "", ""): ("Yicui Wang", "Wang", "Yicui", "Y"),
            ("T Kutateladze", "", "", ""): ("T G Kutateladze", "", "", ""),
            ("David J. Smith", "", "", ""): ("David Smith", "", "", ""),
            ("Smith,David", "", "", ""): ("Smith, David", "", "", "")
        }
        for name_list1, name_list2 in test_author_pairs.items():
            (name, last_name, first_name, first_initial) = name_list1
            author1 = Author.load_from_db_dict({"name": name,
                                                "last_name": last_name,
                                                "first_name": first_name,
                                                "first_initial": first_initial,
                                                "order": 1,
                                                "orcid": "",
                                                "affiliations": []})
            (name, last_name, first_name, first_initial) = name_list2
            author2 = Author.load_from_json_dict({"name": name,
                                                  "lastname": last_name,
                                                  "firstname": first_name,
                                                  "firstinit": first_initial,
                                                  "order": 2,
                                                  "orcid": "ORCID:00099",
                                                  "affiliations": []})
            assert author1.get_key_based_on_unaccented_names() == author2.get_key_based_on_unaccented_names()

        author1 = Author.load_from_db_dict({"name": "Smith D",
                                            "last_name": "",
                                            "first_name": "",
                                            "first_initial": "",
                                            "order": 1,
                                            "orcid": "",
                                            "affiliations": []})
        author2 = Author.load_from_json_dict({"name": "David Smith",
                                              "lastname": "Smith",
                                              "firstname": "David",
                                              "firstinit": "D",
                                              "order": 2,
                                              "orcid": "ORCID:00099",
                                              "affiliations": []})
        is_same = authors_have_same_name(author1, author2)
        assert is_same is True

        author_list1 = [
            {'orcid': None, 'first_author': False, 'order': 1, 'corresponding_author': False,
             'name': 'Jatin Nason', 'affiliations': [], 'first_name': 'Jatin',
             'last_name': 'Nason', 'first_initial': 'J'},
            {'orcid': None, 'first_author': False, 'order': 2, 'corresponding_author': False,
             'name': 'Amy Each', 'affiliations': [], 'first_name': 'Amy', 'last_name': 'Each',
             'first_initial': 'A'},
            {'orcid': None, 'first_author': False, 'order': 3, 'corresponding_author': False,
             'name': 'Olga Smith', 'affiliations': ['Harker Medical Center'], 'first_name': 'Olga',
             'last_name': 'Smith', 'first_initial': 'O'}
        ]

        author_list2 = [
            {'affiliations': [], 'firstinit': 'J', 'firstname': 'Jatin', 'lastname': 'Nason',
             'name': 'Jatin Nason'},
            {'affiliations': [], 'firstinit': 'A', 'firstname': 'Amy', 'lastname': 'Each',
             'name': 'Amy Each'},
            {'affiliations': ['Harker medical center'], 'firstinit': 'O', 'firstname': 'olga',
             'lastname': 'smith', 'name': 'Olga smith'}
        ]

        authors_db = Author.load_list_of_authors_from_db_dict_list(author_list1)
        authors_json = Author.load_list_of_authors_from_json_dict_list(author_list2)
        is_equal = authors_lists_are_equal(authors_db, authors_json)
        assert is_equal is True


class TestRetractedPaperFunctions:

    def test_update_title_for_one_retracted_paper_full_retraction(self, db, load_sanitized_references):  # noqa
        """Test that full retraction adds RETRACTED: prefix to title."""
        ref = db.query(ReferenceModel).first()
        original_title = ref.title
        ref.retraction_status = 'ATP:0000346'
        db.commit()

        result = update_title_for_one_retracted_paper(db, logger, ref.reference_id)

        db.refresh(ref)
        assert result is True
        assert ref.title.startswith("RETRACTED: ")
        assert original_title in ref.title

    def test_update_title_for_one_retracted_paper_partial_retraction(self, db, load_sanitized_references):  # noqa
        """Test that partial retraction adds PARTIALLY RETRACTED: prefix to title."""
        ref = db.query(ReferenceModel).first()
        original_title = ref.title
        ref.retraction_status = 'ATP:0000347'
        db.commit()

        result = update_title_for_one_retracted_paper(db, logger, ref.reference_id)

        db.refresh(ref)
        assert result is True
        assert ref.title.startswith("PARTIALLY RETRACTED: ")
        assert original_title in ref.title

    def test_update_title_for_one_retracted_paper_already_prefixed(self, db, load_sanitized_references):  # noqa
        """Test that already prefixed title is not modified again."""
        ref = db.query(ReferenceModel).first()
        ref.title = "RETRACTED: Some title"
        ref.retraction_status = 'ATP:0000346'
        db.commit()

        result = update_title_for_one_retracted_paper(db, logger, ref.reference_id)

        db.refresh(ref)
        assert result is False
        assert ref.title == "RETRACTED: Some title"

    def test_update_title_for_one_retracted_paper_replaces_variant_prefix(self, db, load_sanitized_references):  # noqa
        """Test that variant prefixes are replaced with standardized prefix."""
        ref = db.query(ReferenceModel).first()
        ref.title = "retracted: Some title"
        ref.retraction_status = 'ATP:0000346'
        db.commit()

        result = update_title_for_one_retracted_paper(db, logger, ref.reference_id)

        db.refresh(ref)
        assert result is True
        assert ref.title == "RETRACTED: Some title"

    def test_update_title_for_one_retracted_paper_no_retraction_status(self, db, load_sanitized_references):  # noqa
        """Test that paper without retraction status is not modified."""
        ref = db.query(ReferenceModel).first()
        original_title = ref.title
        ref.retraction_status = None
        db.commit()

        result = update_title_for_one_retracted_paper(db, logger, ref.reference_id)

        db.refresh(ref)
        assert result is False
        assert ref.title == original_title

    def test_update_title_for_one_retracted_paper_nonexistent_reference(self, db, load_sanitized_references):  # noqa
        """Test handling of nonexistent reference_id."""
        result = update_title_for_one_retracted_paper(db, logger, 999999)
        assert result is False

    def test_update_title_for_retracted_papers(self, db, load_sanitized_references):  # noqa
        """Test bulk update of retracted paper titles."""
        refs = db.query(ReferenceModel).limit(2).all()
        refs[0].retraction_status = 'ATP:0000346'
        refs[0].title = "First paper title"
        refs[1].retraction_status = 'ATP:0000348'
        refs[1].title = "Second paper title"
        db.commit()

        update_title_for_retracted_papers(db, logger)

        db.refresh(refs[0])
        db.refresh(refs[1])
        assert refs[0].title == "RETRACTED: First paper title"
        assert refs[1].title == "RETRACTED: Second paper title"

    def test_set_retraction_status(self, db, load_sanitized_references):  # noqa
        """Test setting retraction_status for papers with 'Retracted Publication' type."""
        ref = db.query(ReferenceModel).first()
        ref.pubmed_types = ['Journal Article', 'Retracted Publication']
        ref.retraction_status = None
        db.commit()

        set_retraction_status(db, logger)

        db.refresh(ref)
        assert ref.retraction_status == 'ATP:0000346'

    def test_set_retraction_status_already_set(self, db, load_sanitized_references):  # noqa
        """Test that existing retraction_status is not overwritten."""
        ref = db.query(ReferenceModel).first()
        ref.pubmed_types = ['Journal Article', 'Retracted Publication']
        ref.retraction_status = 'ATP:0000347'
        db.commit()

        set_retraction_status(db, logger)

        db.refresh(ref)
        assert ref.retraction_status == 'ATP:0000347'

    def test_set_retraction_status_no_retracted_type(self, db, load_sanitized_references):  # noqa
        """Test that papers without 'Retracted Publication' type are not modified."""
        ref = db.query(ReferenceModel).first()
        ref.pubmed_types = ['Journal Article', 'Review']
        ref.retraction_status = None
        db.commit()

        set_retraction_status(db, logger)

        db.refresh(ref)
        assert ref.retraction_status is None

    @patch('agr_literature_service.lit_processing.data_ingest.utils.db_write_utils.get_workflow_tags_from_process')
    def test_cleanup_tags_for_one_retracted_paper_no_mods(self, mock_get_workflow_tags, db, load_sanitized_references):  # noqa
        """Test cleanup when paper has no in-corpus mod associations."""
        mock_get_workflow_tags.return_value = ['ATP:0000162', 'ATP:0000163']

        ref = db.query(ReferenceModel).first()
        ref.retraction_status = 'ATP:0000346'
        db.commit()

        db.query(ModCorpusAssociationModel).filter_by(
            reference_id=ref.reference_id
        ).update({'corpus': False})
        db.commit()

        stats = cleanup_tags_for_one_retracted_paper(db, logger, ref.reference_id)

        assert stats['processed_mod_count'] == 0

    @patch('agr_literature_service.lit_processing.data_ingest.utils.db_write_utils.get_workflow_tags_from_process')
    def test_cleanup_tags_for_one_retracted_paper_with_mod(self, mock_get_workflow_tags, db, load_sanitized_references):  # noqa
        """Test cleanup when paper has in-corpus mod association."""
        mock_get_workflow_tags.return_value = ['ATP:0000162', 'ATP:0000163']

        ref = db.query(ReferenceModel).first()
        ref.retraction_status = 'ATP:0000346'
        db.commit()

        mca = db.query(ModCorpusAssociationModel).filter_by(
            reference_id=ref.reference_id, corpus=True
        ).first()

        if mca is None:
            mod = db.query(ModModel).first()
            mca = ModCorpusAssociationModel(
                reference_id=ref.reference_id,
                mod_id=mod.mod_id,
                corpus=True,
                mod_corpus_sort_source='manual_creation'
            )
            db.add(mca)
            db.commit()

        stats = cleanup_tags_for_one_retracted_paper(db, logger, ref.reference_id)

        assert stats['processed_mod_count'] >= 1

    @patch('agr_literature_service.lit_processing.data_ingest.utils.db_write_utils.get_workflow_tags_from_process')
    def test_cleanup_tags_for_retracted_papers_no_retracted(self, mock_get_workflow_tags, db, load_sanitized_references):  # noqa
        """Test cleanup when there are no retracted papers."""
        mock_get_workflow_tags.return_value = ['ATP:0000162', 'ATP:0000163']

        db.query(ReferenceModel).update({'retraction_status': None})
        db.commit()

        cleanup_tags_for_retracted_papers(db, logger)

    @patch('agr_literature_service.lit_processing.data_ingest.utils.db_write_utils.get_workflow_tags_from_process')
    def test_cleanup_tags_for_retracted_papers_with_retracted(self, mock_get_workflow_tags, db, load_sanitized_references):  # noqa
        """Test cleanup when there are retracted papers."""
        mock_get_workflow_tags.return_value = ['ATP:0000162', 'ATP:0000163']

        ref = db.query(ReferenceModel).first()
        ref.retraction_status = 'ATP:0000346'
        db.commit()

        cleanup_tags_for_retracted_papers(db, logger)

    @patch('agr_literature_service.lit_processing.data_ingest.utils.db_write_utils.transition_to_workflow_status')
    @patch('agr_literature_service.lit_processing.data_ingest.utils.db_write_utils.get_current_workflow_status')
    def test_restore_file_upload_workflow_tags_no_mods(self, mock_get_status, mock_transition, db, load_sanitized_references):  # noqa
        """Test restore when paper has no in-corpus mod associations."""
        ref = db.query(ReferenceModel).first()

        # Remove all corpus associations
        db.query(ModCorpusAssociationModel).filter_by(
            reference_id=ref.reference_id
        ).update({'corpus': False})
        db.commit()

        stats = restore_file_upload_workflow_tags(db, logger, ref.reference_id)

        assert stats['workflow_tags_added'] == 0
        assert len(stats['mods_processed']) == 0
        mock_transition.assert_not_called()

    @patch('agr_literature_service.lit_processing.data_ingest.utils.db_write_utils.transition_to_workflow_status')
    @patch('agr_literature_service.lit_processing.data_ingest.utils.db_write_utils.get_current_workflow_status')
    def test_restore_file_upload_workflow_tags_existing_tag(self, mock_get_status, mock_transition, db, load_sanitized_references):  # noqa
        """Test restore when MOD already has a file upload workflow tag."""
        ref = db.query(ReferenceModel).first()

        # Ensure there's a corpus association
        mca = db.query(ModCorpusAssociationModel).filter_by(
            reference_id=ref.reference_id, corpus=True
        ).first()
        if mca is None:
            mod = db.query(ModModel).filter(ModModel.abbreviation != 'AGR').first()
            mca = ModCorpusAssociationModel(
                reference_id=ref.reference_id,
                mod_id=mod.mod_id,
                corpus=True,
                mod_corpus_sort_source='manual_creation'
            )
            db.add(mca)
            db.commit()

        # Mock that workflow tag already exists
        mock_get_status.return_value = 'ATP:0000141'

        stats = restore_file_upload_workflow_tags(db, logger, ref.reference_id)

        # Should skip since tag already exists
        assert stats['workflow_tags_added'] == 0
        mock_transition.assert_not_called()

    @patch('agr_literature_service.lit_processing.data_ingest.utils.db_write_utils.transition_to_workflow_status')
    @patch('agr_literature_service.lit_processing.data_ingest.utils.db_write_utils.get_current_workflow_status')
    def test_restore_file_upload_workflow_tags_with_main_pdf(self, mock_get_status, mock_transition, db, load_sanitized_references):  # noqa
        """Test restore when paper has a main PDF file."""
        ref = db.query(ReferenceModel).first()

        # Ensure there's a corpus association (not AGR)
        mod = db.query(ModModel).filter(ModModel.abbreviation != 'AGR').first()
        mca = db.query(ModCorpusAssociationModel).filter_by(
            reference_id=ref.reference_id, mod_id=mod.mod_id, corpus=True
        ).first()
        if mca is None:
            mca = ModCorpusAssociationModel(
                reference_id=ref.reference_id,
                mod_id=mod.mod_id,
                corpus=True,
                mod_corpus_sort_source='manual_creation'
            )
            db.add(mca)
            db.commit()

        # No existing workflow tag
        mock_get_status.return_value = None

        # Add a main PDF file
        referencefile = ReferencefileModel(
            reference_id=ref.reference_id,
            display_name='test_main',
            file_class='main',
            file_publication_status='final',
            file_extension='pdf',
            pdf_type='pdf',
            md5sum='test_md5_main_pdf'
        )
        db.add(referencefile)
        db.commit()

        referencefile_mod = ReferencefileModAssociationModel(
            referencefile_id=referencefile.referencefile_id,
            mod_id=mod.mod_id
        )
        db.add(referencefile_mod)
        db.commit()

        stats = restore_file_upload_workflow_tags(db, logger, ref.reference_id)

        # Should have added files uploaded tag (ATP:0000134)
        assert stats['workflow_tags_added'] >= 1
        mock_transition.assert_called()
        # Check that the call was made with ATP:0000134 (files uploaded)
        calls = mock_transition.call_args_list
        assert any('ATP:0000134' in str(call) for call in calls)

        # Cleanup
        db.delete(referencefile_mod)
        db.delete(referencefile)
        db.commit()

    @patch('agr_literature_service.lit_processing.data_ingest.utils.db_write_utils.transition_to_workflow_status')
    @patch('agr_literature_service.lit_processing.data_ingest.utils.db_write_utils.get_current_workflow_status')
    def test_restore_file_upload_workflow_tags_with_supplement_only(self, mock_get_status, mock_transition, db, load_sanitized_references):  # noqa
        """Test restore when paper has supplement file but no main PDF."""
        ref = db.query(ReferenceModel).first()

        # Ensure there's a corpus association (not AGR)
        mod = db.query(ModModel).filter(ModModel.abbreviation != 'AGR').first()
        mca = db.query(ModCorpusAssociationModel).filter_by(
            reference_id=ref.reference_id, mod_id=mod.mod_id, corpus=True
        ).first()
        if mca is None:
            mca = ModCorpusAssociationModel(
                reference_id=ref.reference_id,
                mod_id=mod.mod_id,
                corpus=True,
                mod_corpus_sort_source='manual_creation'
            )
            db.add(mca)
            db.commit()

        # No existing workflow tag
        mock_get_status.return_value = None

        # Add a supplement file (not main)
        referencefile = ReferencefileModel(
            reference_id=ref.reference_id,
            display_name='test_supplement',
            file_class='supplement',
            file_publication_status='final',
            file_extension='pdf',
            pdf_type='pdf',
            md5sum='test_md5_supplement'
        )
        db.add(referencefile)
        db.commit()

        referencefile_mod = ReferencefileModAssociationModel(
            referencefile_id=referencefile.referencefile_id,
            mod_id=mod.mod_id
        )
        db.add(referencefile_mod)
        db.commit()

        stats = restore_file_upload_workflow_tags(db, logger, ref.reference_id)

        # Should have added file upload in progress tag (ATP:0000139)
        assert stats['workflow_tags_added'] >= 1
        mock_transition.assert_called()
        # Check that the call was made with ATP:0000139 (file upload in progress)
        calls = mock_transition.call_args_list
        assert any('ATP:0000139' in str(call) for call in calls)

        # Cleanup
        db.delete(referencefile_mod)
        db.delete(referencefile)
        db.commit()

    @patch('agr_literature_service.lit_processing.data_ingest.utils.db_write_utils.transition_to_workflow_status')
    @patch('agr_literature_service.lit_processing.data_ingest.utils.db_write_utils.get_current_workflow_status')
    def test_restore_file_upload_workflow_tags_no_files(self, mock_get_status, mock_transition, db, load_sanitized_references):  # noqa
        """Test restore when paper has no files."""
        ref = db.query(ReferenceModel).first()

        # Ensure there's a corpus association (not AGR)
        mod = db.query(ModModel).filter(ModModel.abbreviation != 'AGR').first()
        mca = db.query(ModCorpusAssociationModel).filter_by(
            reference_id=ref.reference_id, mod_id=mod.mod_id, corpus=True
        ).first()
        if mca is None:
            mca = ModCorpusAssociationModel(
                reference_id=ref.reference_id,
                mod_id=mod.mod_id,
                corpus=True,
                mod_corpus_sort_source='manual_creation'
            )
            db.add(mca)
            db.commit()

        # No existing workflow tag
        mock_get_status.return_value = None

        # Make sure there are no files for this reference
        db.query(ReferencefileModel).filter_by(reference_id=ref.reference_id).delete()
        db.commit()

        stats = restore_file_upload_workflow_tags(db, logger, ref.reference_id)

        # Should have added file needed tag (ATP:0000141)
        assert stats['workflow_tags_added'] >= 1
        mock_transition.assert_called()
        # Check that the call was made with ATP:0000141 (file needed)
        calls = mock_transition.call_args_list
        assert any('ATP:0000141' in str(call) for call in calls)
