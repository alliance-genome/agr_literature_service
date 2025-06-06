#!/usr/bin/env python3
"""
Populate test database with mock data for Debezium integration tests.
This script runs after init_test_db.sh to create initial data that Debezium can sync.
"""

import json
import os
import sys
from typing import Dict, Any

# Add the project root to the path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from agr_literature_service.api.database.main import get_db  # noqa: E402
from agr_literature_service.api.models.reference_model import ReferenceModel  # noqa: E402
from agr_literature_service.api.models.citation_model import CitationModel  # noqa: E402
from agr_literature_service.api.models.author_model import AuthorModel  # noqa: E402
from agr_literature_service.api.models.cross_reference_model import CrossReferenceModel  # noqa: E402
from agr_literature_service.api.models.reference_relation_model import ReferenceRelationModel  # noqa: E402
from agr_literature_service.api.models.copyright_license_model import CopyrightLicenseModel  # noqa: E402
from agr_literature_service.api.models.mesh_detail_model import MeshDetailModel  # noqa: E402
from agr_literature_service.api.models.resource_model import ResourceModel  # noqa: E402


class MockDataFactory:
    """Factory for creating realistic mock data for Debezium integration testing."""

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
            curie_prefix = "DOI"
        else:
            curie = f"PMID:1234567{xref_id}"
            curie_prefix = "PMID"

        xref = CrossReferenceModel(
            reference_id=reference.reference_id,
            curie=curie,
            curie_prefix=curie_prefix,
            is_obsolete=is_obsolete
        )
        db_session.add(xref)
        return xref

    def create_reference_relation(self, db_session, ref_from: ReferenceModel,
                                  ref_to: ReferenceModel, relation_type: str = "CommentOn") -> ReferenceRelationModel:
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


def populate_database():
    """Populate the test database with mock data for Debezium integration tests."""
    print("Starting mock data population for Debezium integration tests...")

    # Initialize database connection
    db = next(get_db())
    factory = MockDataFactory()

    try:
        # Create resources
        print("Creating resources...")
        resources = []
        for i in range(3):
            resource = factory.create_resource(db, i + 1)
            resources.append(resource)

        # Create citations
        print("Creating citations...")
        citations = []
        for i in range(5):
            citation = factory.create_citation(db, i + 1)
            citations.append(citation)

        # Create references
        print("Creating references...")
        references = []
        for i in range(10):
            resource = resources[i % len(resources)]
            citation = citations[i % len(citations)]
            reference = factory.create_reference(db, i + 1, citation, resource)
            references.append(reference)

            # Add authors for each reference
            factory.create_author(db, reference, i + 1)

            # Add cross-references (regular and obsolete)
            factory.create_cross_reference(db, reference, i + 1, False)
            if i % 3 == 0:  # Add some obsolete cross-references
                factory.create_cross_reference(db, reference, i + 1, True)

            # Add MeSH terms
            factory.create_mesh_detail(db, reference, i)

        # Create reference relations
        print("Creating reference relations...")
        for i in range(len(references) - 1):
            if i % 2 == 0:
                factory.create_reference_relation(db, references[i], references[i + 1], "CommentOn")
            else:
                factory.create_reference_relation(db, references[i], references[i + 1], "ErratumFor")

        # Create copyright licenses
        print("Creating copyright licenses...")
        for i in range(5):
            factory.create_copyright_license(db, i + 1)

        # Commit all changes
        db.commit()

        # Print summary
        ref_count = db.query(ReferenceModel).count()
        author_count = db.query(AuthorModel).count()
        xref_count = db.query(CrossReferenceModel).count()
        relation_count = db.query(ReferenceRelationModel).count()
        license_count = db.query(CopyrightLicenseModel).count()
        mesh_count = db.query(MeshDetailModel).count()

        print("Mock data population completed successfully!")
        print(f"Created: {ref_count} references, {author_count} authors, {xref_count} cross-references")
        print(f"Created: {relation_count} relations, {license_count} licenses, {mesh_count} mesh terms")

    except Exception as e:
        print(f"Error populating database: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    populate_database()
