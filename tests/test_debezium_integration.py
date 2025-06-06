"""
Test Debezium integration for both public and private reference indexes.
This module tests the complete data pipeline: PostgreSQL -> Kafka -> KsqlDB -> Elasticsearch.
"""

import json
import os
import time
import pytest
import requests
from typing import Dict, Any

from agr_literature_service.api.database import get_db
from agr_literature_service.api.models.reference_model import ReferenceModel
from agr_literature_service.api.models.citation_model import CitationModel
from agr_literature_service.api.models.author_model import AuthorModel
from agr_literature_service.api.models.cross_reference_model import CrossReferenceModel
from agr_literature_service.api.models.reference_relation_model import ReferenceRelationModel
from agr_literature_service.api.models.copyright_license_model import CopyrightLicenseModel
from agr_literature_service.api.models.mesh_detail_model import MeshDetailModel
from agr_literature_service.api.models.resource_model import ResourceModel


class MockDataFactory:
    """Factory for creating realistic mock data based on RDS dev patterns."""

    def __init__(self):
        self.mock_patterns = self._load_mock_patterns()

    def _load_mock_patterns(self) -> Dict[str, Any]:
        """Load mock data patterns from the generated file."""
        patterns_file = os.path.join(os.path.dirname(__file__), 'mock_data_patterns.json')
        if os.path.exists(patterns_file):
            with open(patterns_file, 'r') as f:
                return json.load(f)
        return {}

    def create_resource(self, db_session, resource_id: int = 1) -> ResourceModel:
        """Create a realistic resource based on RDS dev patterns."""
        resource_pattern = (self.mock_patterns.get('resources', [{}])[0]
                            if self.mock_patterns.get('resources') else {})

        resource = ResourceModel(
            curie=f"AGRKB:resource{resource_id:04d}",
            title=resource_pattern.get('title', f"Test Genomics Journal {resource_id}"),
            iso_abbreviation=resource_pattern.get('iso_abbreviation', f"TGJ{resource_id}"),
            medline_abbreviation=resource_pattern.get('medline_abbreviation', f"Test Genom J{resource_id}"),
            print_issn=f"1234-567{resource_id}",
            online_issn=f"8765-432{resource_id}",
            publisher=resource_pattern.get('publisher', "Academic Press")
        )
        db_session.add(resource)
        db_session.flush()
        return resource

    def create_citation(self, db_session, citation_id: int = 1) -> CitationModel:
        """Create a realistic citation based on RDS dev patterns."""
        citation_pattern = (self.mock_patterns.get('citations', [{}])[0]
                            if self.mock_patterns.get('citations') else {})

        citation = CitationModel(
            citation=f"Test Citation {citation_id}. Test Journal. 2024;{citation_id}:123-456.",
            short_citation=citation_pattern.get('short_citation', "Test et al. 2024")
        )
        db_session.add(citation)
        db_session.flush()
        return citation

    def create_reference(self, db_session, ref_id: int, citation: CitationModel,
                         resource: ResourceModel) -> ReferenceModel:
        """Create a realistic reference based on RDS dev patterns."""
        ref_patterns = self.mock_patterns.get('references', [{}])
        ref_pattern = (ref_patterns[ref_id % len(ref_patterns)]
                       if ref_patterns else {})

        reference = ReferenceModel(
            curie=f"AGRKB:10100{ref_id:04d}",
            title=f"Test Reference {ref_id}: " + (ref_pattern.get('title', 'Genomics Study')[:50] + "..."),
            abstract=ref_pattern.get('abstract', f"This is a test abstract for reference {ref_id}."),
            category=ref_pattern.get('category', 'research_article'),
            citation_id=citation.citation_id,
            resource_id=resource.resource_id,
            date_published=ref_pattern.get('date_published', '2024-01-01'),
            date_created=int(time.time() * 1000),
            language=ref_pattern.get('language', 'eng'),
            publisher=ref_pattern.get('publisher', 'Academic Press'),
            keywords=ref_pattern.get('keywords', ['genomics', 'test']),
            pubmed_types=ref_pattern.get('pubmed_types', ['Journal Article']),
            pubmed_publication_status=ref_pattern.get('pubmed_publication_status', 'ppublish'),
            volume=ref_pattern.get('volume', f"Vol{ref_id}"),
            issue_name=ref_pattern.get('issue_name', f"Issue{ref_id}"),
            page_range=ref_pattern.get('page_range', f"{100 + ref_id * 10}-{110 + ref_id * 10}")
        )
        db_session.add(reference)
        db_session.flush()
        return reference

    def create_author(self, db_session, reference: ReferenceModel, author_id: int) -> AuthorModel:
        """Create a realistic author based on RDS dev patterns."""
        author_patterns = self.mock_patterns.get('authors', [{}])
        author_pattern = (author_patterns[author_id % len(author_patterns)]
                          if author_patterns else {})

        author = AuthorModel(
            reference_id=reference.reference_id,
            name=author_pattern.get('name', f"Test Author {author_id}"),
            orcid=f"0000-0000-0000-{author_id:04d}"
        )
        db_session.add(author)
        return author

    def create_cross_reference(self, db_session, reference: ReferenceModel,
                               xref_id: int, is_obsolete: bool = False) -> CrossReferenceModel:
        """Create a realistic cross-reference based on RDS dev patterns."""
        if is_obsolete:
            curie = f"DOI:10.1000/test{xref_id}_obsolete"
        else:
            curie = f"PMID:1234567{xref_id}"

        xref = CrossReferenceModel(
            reference_id=reference.reference_id,
            curie=curie,
            is_obsolete=is_obsolete
        )
        db_session.add(xref)
        return xref

    def create_reference_relation(self, db_session, ref_from: ReferenceModel,
                                  ref_to: ReferenceModel, relation_type: str = "Reviews") -> ReferenceRelationModel:
        """Create a reference relation."""
        relation = ReferenceRelationModel(
            reference_id_from=ref_from.reference_id,
            reference_id_to=ref_to.reference_id,
            reference_relation_type=relation_type
        )
        db_session.add(relation)
        return relation

    def create_copyright_license(self, db_session, license_id: int) -> CopyrightLicenseModel:
        """Create a copyright license based on RDS dev patterns."""
        license_patterns = self.mock_patterns.get('copyright_licenses', [{}])
        license_pattern = (license_patterns[license_id % len(license_patterns)]
                           if license_patterns else {})

        license_obj = CopyrightLicenseModel(
            name=license_pattern.get('name', f"Creative Commons Attribution {license_id}.0 License"),
            url=license_pattern.get('url', f"https://creativecommons.org/licenses/by/{license_id}.0/"),
            description=license_pattern.get('description', f"Open access license {license_id}"),
            open_access=license_pattern.get('open_access', license_id % 2 == 0)
        )
        db_session.add(license_obj)
        return license_obj

    def create_mesh_detail(self, db_session, reference: ReferenceModel, mesh_id: int) -> MeshDetailModel:
        """Create MeSH terms based on RDS dev patterns."""
        mesh_terms = [
            ("Genomics", "methods"),
            ("Bioinformatics", "classification"),
            ("Gene Expression", "genetics"),
            ("Proteomics", "analysis"),
            ("Systems Biology", "methods")
        ]

        heading, qualifier = mesh_terms[mesh_id % len(mesh_terms)]

        mesh = MeshDetailModel(
            reference_id=reference.reference_id,
            heading_term=heading,
            qualifier_term=qualifier
        )
        db_session.add(mesh)
        return mesh


@pytest.fixture
def mock_data_factory():
    """Provide mock data factory for tests."""
    return MockDataFactory()


@pytest.fixture
def elasticsearch_config():
    """Provide Elasticsearch configuration for tests."""
    return {
        'host': os.getenv('ELASTICSEARCH_HOST', 'localhost'),
        'port': int(os.getenv('ELASTICSEARCH_PORT', '9200')),
        'private_index': os.getenv('DEBEZIUM_INDEX_NAME', 'test_references_index'),
        'public_index': os.getenv('PUBLIC_INDEX_NAME', 'public_references_index')
    }


@pytest.fixture
def test_database():
    """Provide database session for tests."""
    # Set test environment
    os.environ['ENV_STATE'] = 'test'

    db_gen = get_db()
    db_session = next(db_gen)

    yield db_session

    # Cleanup
    db_session.rollback()
    db_session.close()


class TestDebeziumIntegration:
    """Test Debezium integration for both public and private indexes."""

    def test_create_mock_data(self, test_database, mock_data_factory):
        """Test creating realistic mock data in the test database."""
        # Create test data
        resource = mock_data_factory.create_resource(test_database, 1)
        citation = mock_data_factory.create_citation(test_database, 1)

        references = []
        for i in range(3):
            ref = mock_data_factory.create_reference(test_database, i + 1, citation, resource)
            references.append(ref)

            # Add authors
            mock_data_factory.create_author(test_database, ref, i + 1)

            # Add cross-references (regular and obsolete)
            mock_data_factory.create_cross_reference(test_database, ref, i + 1, False)
            mock_data_factory.create_cross_reference(test_database, ref, i + 1, True)

            # Add MeSH terms
            mock_data_factory.create_mesh_detail(test_database, ref, i)

        # Add reference relations
        if len(references) >= 2:
            mock_data_factory.create_reference_relation(test_database, references[0], references[1], "Reviews")
            mock_data_factory.create_reference_relation(test_database, references[1], references[2], "Cites")

        # Add copyright licenses
        for i in range(3):
            mock_data_factory.create_copyright_license(test_database, i + 1)

        # Commit test data
        test_database.commit()

        # Verify data was created
        ref_count = test_database.query(ReferenceModel).count()
        author_count = test_database.query(AuthorModel).count()
        xref_count = test_database.query(CrossReferenceModel).count()

        assert ref_count == 3
        assert author_count == 3
        assert xref_count == 6  # 3 regular + 3 obsolete

    @pytest.mark.webtest
    def test_elasticsearch_indexes_exist(self, elasticsearch_config):
        """Test that both public and private Elasticsearch indexes exist."""
        es_url = f"http://{elasticsearch_config['host']}:{elasticsearch_config['port']}"

        # Check private index
        private_response = requests.get(f"{es_url}/{elasticsearch_config['private_index']}")
        assert private_response.status_code == 200, f"Private index {elasticsearch_config['private_index']} not found"

        # Check public index
        public_response = requests.get(f"{es_url}/{elasticsearch_config['public_index']}")
        assert public_response.status_code == 200, f"Public index {elasticsearch_config['public_index']} not found"

    @pytest.mark.webtest
    def test_public_index_limited_fields(self, elasticsearch_config):
        """Test that public index contains only the specified limited fields."""
        es_url = f"http://{elasticsearch_config['host']}:{elasticsearch_config['port']}"

        # Get sample document from public index
        response = requests.get(f"{es_url}/{elasticsearch_config['public_index']}/_search?size=1")
        assert response.status_code == 200

        data = response.json()
        if data['hits']['total']['value'] > 0:
            sample_doc = data['hits']['hits'][0]['_source']

            # Check that sample document has reasonable field count (not too many)
            doc_fields = set(sample_doc.keys())
            assert len(doc_fields) <= 25, f"Public index has too many fields: {len(doc_fields)}"

            # Check for some key fields that should be present
            key_fields = {'curie', 'title', 'abstract'}
            assert key_fields.issubset(doc_fields), f"Missing key fields: {key_fields - doc_fields}"

    @pytest.mark.webtest
    def test_private_index_full_fields(self, elasticsearch_config):
        """Test that private index contains full dataset including sensitive fields."""
        es_url = f"http://{elasticsearch_config['host']}:{elasticsearch_config['port']}"

        # Get sample document from private index
        response = requests.get(f"{es_url}/{elasticsearch_config['private_index']}/_search?size=1")
        assert response.status_code == 200

        data = response.json()
        if data['hits']['total']['value'] > 0:
            sample_doc = data['hits']['hits'][0]['_source']

            # Private index should have more fields than public
            doc_fields = set(sample_doc.keys())
            assert len(doc_fields) >= 25, f"Private index has too few fields: {len(doc_fields)}"

    @pytest.mark.webtest
    def test_document_counts_match(self, elasticsearch_config):
        """Test that both indexes have the same number of documents."""
        es_url = f"http://{elasticsearch_config['host']}:{elasticsearch_config['port']}"

        # Get document count from private index
        private_response = requests.get(f"{es_url}/{elasticsearch_config['private_index']}/_count")
        assert private_response.status_code == 200
        private_count = private_response.json()['count']

        # Get document count from public index
        public_response = requests.get(f"{es_url}/{elasticsearch_config['public_index']}/_count")
        assert public_response.status_code == 200
        public_count = public_response.json()['count']

        # Both indexes should have the same number of documents
        assert private_count == public_count, f"Document counts differ: private={private_count}, public={public_count}"

        # Both should have some documents
        assert private_count > 0, "Both indexes are empty"

    @pytest.mark.webtest
    def test_new_fields_present_in_public_index(self, elasticsearch_config):
        """Test that new fields (relations, copyright_license, mesh_terms) are present in public index."""
        es_url = f"http://{elasticsearch_config['host']}:{elasticsearch_config['port']}"

        # Search for documents with new fields
        new_fields = ['relations', 'copyright_license', 'mesh_terms', 'resource_title']

        for field in new_fields:
            response = requests.get(f"{es_url}/{elasticsearch_config['public_index']}/_search?q={field}:*&size=1")
            assert response.status_code == 200

            # Field should exist in the mapping even if no documents have values
            # Get mapping to verify field exists
            mapping_response = requests.get(f"{es_url}/{elasticsearch_config['public_index']}/_mapping")
            assert mapping_response.status_code == 200

            mapping = mapping_response.json()
            index_mapping = list(mapping.values())[0]['mappings']['properties']

            # Field should be in the mapping
            assert field in index_mapping or any(field in str(index_mapping)), f"Field {field} not found in public index mapping"

    @pytest.mark.webtest
    def test_search_functionality(self, elasticsearch_config):
        """Test that search functionality works in both indexes."""
        es_url = f"http://{elasticsearch_config['host']}:{elasticsearch_config['port']}"

        # Test search in private index
        private_response = requests.get(f"{es_url}/{elasticsearch_config['private_index']}/_search?q=*&size=5")
        assert private_response.status_code == 200
        private_data = private_response.json()

        # Test search in public index
        public_response = requests.get(f"{es_url}/{elasticsearch_config['public_index']}/_search?q=*&size=5")
        assert public_response.status_code == 200
        public_data = public_response.json()

        # Both searches should return some results if data exists
        if private_data['hits']['total']['value'] > 0:
            assert public_data['hits']['total']['value'] > 0, "Public index search returned no results"

            # Verify basic structure
            assert 'hits' in private_data
            assert 'hits' in public_data
            assert len(private_data['hits']['hits']) > 0
            assert len(public_data['hits']['hits']) > 0
