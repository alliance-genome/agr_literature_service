import gzip
import os
import tempfile
from unittest.mock import MagicMock, patch

from agr_literature_service.lit_processing.data_ingest.go_annotations.load_goa_human_papers import (
    compose_report_message,
)
from agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils import (
    associate_papers_with_alliance,
    search_pubmed_for_validity,
)


class TestExtractPmidsFromGoaHuman:
    """Tests for extracting PMIDs from GOA human GAF files."""

    def test_extract_pmids_from_gaf_basic(self):
        """Test basic PMID extraction from GAF content."""
        # Create a temporary gzipped GAF file
        gaf_content = """!gaf-version: 2.2
!This is a comment line
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0003824\tPMID:21873635\tIBA\tPANTHER:PTN000034743|UniProtKB:P0A7K6\tF\tNucleoside diphosphate-linked moiety X motif 4B\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0046872\tPMID:21873635|GO_REF:0000033\tIBA\tPANTHER:PTN000034743|UniProtKB:P0A7K6\tF\tNucleoside diphosphate-linked moiety X motif 4B\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
UniProtKB\tA0A075B6H9\tIGKV3-7\t\tGO:0003823\tPMID:27312411\tIDA\t\tF\tImmunoglobulin kappa variable 3-7\tHGNC:5819\tprotein\ttaxon:9606\t20170217\tUniProt\t\t
UniProtKB\tA0A075B6H9\tIGKV3-7\t\tGO:0005576\tGO_REF:0000044\tIEA\tUniProtKB-SubCell:SL-0243\tC\tImmunoglobulin kappa variable 3-7\tHGNC:5819\tprotein\ttaxon:9606\t20240205\tUniProt\t\t
"""
        with tempfile.NamedTemporaryFile(suffix='.gaf.gz', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            with gzip.open(tmp_path, 'wt') as f:
                f.write(gaf_content)

        try:
            # Import the function that extracts PMIDs from a file path
            from agr_literature_service.lit_processing.data_ingest.go_annotations.load_mod_gaf_papers import (
                extract_pmids_from_gaf,
            )
            pmids = extract_pmids_from_gaf(tmp_path)

            assert len(pmids) == 2
            assert "21873635" in pmids
            assert "27312411" in pmids
        finally:
            os.unlink(tmp_path)

    def test_extract_pmids_skips_comments(self):
        """Test that comment lines are skipped."""
        gaf_content = """!gaf-version: 2.2
!This file contains GO annotations
!PMID:12345678 in comment should be ignored
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0003824\tPMID:11111111\tIBA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
"""
        with tempfile.NamedTemporaryFile(suffix='.gaf.gz', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            with gzip.open(tmp_path, 'wt') as f:
                f.write(gaf_content)

        try:
            from agr_literature_service.lit_processing.data_ingest.go_annotations.load_mod_gaf_papers import (
                extract_pmids_from_gaf,
            )
            pmids = extract_pmids_from_gaf(tmp_path)

            assert len(pmids) == 1
            assert "11111111" in pmids
            assert "12345678" not in pmids
        finally:
            os.unlink(tmp_path)

    def test_extract_pmids_handles_multiple_refs(self):
        """Test extraction when multiple references are pipe-separated."""
        gaf_content = """!gaf-version: 2.2
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0003824\tPMID:11111111|PMID:22222222|GO_REF:0000033\tIBA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
"""
        with tempfile.NamedTemporaryFile(suffix='.gaf.gz', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            with gzip.open(tmp_path, 'wt') as f:
                f.write(gaf_content)

        try:
            from agr_literature_service.lit_processing.data_ingest.go_annotations.load_mod_gaf_papers import (
                extract_pmids_from_gaf,
            )
            pmids = extract_pmids_from_gaf(tmp_path)

            assert len(pmids) == 2
            assert "11111111" in pmids
            assert "22222222" in pmids
        finally:
            os.unlink(tmp_path)

    def test_extract_pmids_ignores_non_pmid_refs(self):
        """Test that non-PMID references are ignored."""
        gaf_content = """!gaf-version: 2.2
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0003824\tGO_REF:0000033\tIEA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0003824\tDOI:10.1234/test\tIDA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0003824\tPMID:33333333\tIDA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
"""
        with tempfile.NamedTemporaryFile(suffix='.gaf.gz', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            with gzip.open(tmp_path, 'wt') as f:
                f.write(gaf_content)

        try:
            from agr_literature_service.lit_processing.data_ingest.go_annotations.load_mod_gaf_papers import (
                extract_pmids_from_gaf,
            )
            pmids = extract_pmids_from_gaf(tmp_path)

            assert len(pmids) == 1
            assert "33333333" in pmids
        finally:
            os.unlink(tmp_path)

    def test_extract_pmids_empty_file(self):
        """Test handling of empty GAF file."""
        gaf_content = """!gaf-version: 2.2
!This file is empty
"""
        with tempfile.NamedTemporaryFile(suffix='.gaf.gz', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            with gzip.open(tmp_path, 'wt') as f:
                f.write(gaf_content)

        try:
            from agr_literature_service.lit_processing.data_ingest.go_annotations.load_mod_gaf_papers import (
                extract_pmids_from_gaf,
            )
            pmids = extract_pmids_from_gaf(tmp_path)

            assert len(pmids) == 0
        finally:
            os.unlink(tmp_path)


class TestSearchPubmedForValidity:
    """Tests for PubMed validation using the shared utility."""

    @patch('agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils.requests.get')
    def test_search_pubmed_valid_pmid(self, mock_get):
        """Test that valid PMIDs are correctly identified."""
        mock_response = MagicMock()
        mock_response.text = "<PubmedArticleSet><PubmedArticle>...</PubmedArticle></PubmedArticleSet>"
        mock_get.return_value = mock_response

        obsolete, valid = search_pubmed_for_validity({"12345678"})

        assert len(valid) == 1
        assert "12345678" in valid
        assert len(obsolete) == 0

    @patch('agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils.requests.get')
    def test_search_pubmed_obsolete_pmid(self, mock_get):
        """Test that obsolete PMIDs are correctly identified."""
        mock_response = MagicMock()
        mock_response.text = "<PubmedArticleSet></PubmedArticleSet>"
        mock_get.return_value = mock_response

        obsolete, valid = search_pubmed_for_validity({"99999999"})

        assert len(obsolete) == 1
        assert "99999999" in obsolete
        assert len(valid) == 0

    @patch('agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils.requests.get')
    def test_search_pubmed_handles_request_exception(self, mock_get):
        """Test that request exceptions are handled gracefully."""
        import requests
        mock_get.side_effect = requests.RequestException("Connection error")

        obsolete, valid = search_pubmed_for_validity({"12345678"})

        # On error, PMIDs are assumed valid
        assert len(valid) == 1
        assert "12345678" in valid
        assert len(obsolete) == 0


class TestComposeReportMessage:
    """Tests for report message composition."""

    def test_compose_report_message_basic(self):
        """Test basic report message composition."""
        mock_session = MagicMock()
        # Mock retrieve_all_pmids to return some PMIDs
        with patch('agr_literature_service.lit_processing.data_ingest.go_annotations.load_goa_human_papers.retrieve_all_pmids') as mock_retrieve:
            mock_retrieve.return_value = ["11111111", "22222222", "33333333"]

            all_pmids = {"11111111", "22222222", "44444444"}
            pmids_loaded = {"44444444"}
            papers_associated = 1
            obsolete_pmids: set = set()

            message = compose_report_message(
                mock_session, "test.gaf.gz", all_pmids, pmids_loaded,
                papers_associated, obsolete_pmids
            )

            assert "GOA Human Paper Loading Report" in message
            assert "Total unique PMIDs in GAF file: 3" in message
            assert "New references loaded: 1" in message
            assert "Papers associated with AGR MOD: 1" in message


class TestAssociatePapersWithAlliance:
    """Tests for associating papers with AGR MOD using shared utility."""

    def test_associate_empty_pmids(self):
        """Test that empty PMIDs returns 0."""
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = MagicMock(mod_id=1)

        result = associate_papers_with_alliance(mock_session, set())

        assert result == 0

    def test_associate_no_mod_found(self):
        """Test handling when MOD is not found."""
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = None

        result = associate_papers_with_alliance(mock_session, {"12345678"}, 'AGR')

        assert result == 0
