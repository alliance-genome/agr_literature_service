#!/usr/bin/env python3
"""
Populate test database with mock data for Debezium integration tests.
This script runs after init_test_db.sh to create initial data that Debezium can sync.
"""

import json
import os
import sys
from typing import Dict, Any

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.models import initialize
from tests.fixtures import delete_all_table_content

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
from agr_literature_service.api.models.mod_model import ModModel  # noqa: E402
from agr_literature_service.api.models.mod_corpus_association_model import ModCorpusAssociationModel  # noqa: E402
from agr_literature_service.api.models.mod_reference_type_model import (  # noqa: E402
    ModReferencetypeAssociationModel, ReferencetypeModel, ReferenceModReferencetypeAssociationModel
)
from agr_literature_service.api.models.topic_entity_tag_model import TopicEntityTagModel, TopicEntityTagSourceModel  # noqa: E402
from agr_literature_service.api.models.workflow_tag_model import WorkflowTagModel  # noqa: E402
from agr_literature_service.api.models.obsolete_model import ObsoleteReferenceModel  # noqa: E402


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
                         resource: ResourceModel = None, copyright_license_id: int = None) -> ReferenceModel:
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
            resource_id=resource.resource_id if resource else None,
            copyright_license_id=copyright_license_id,
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

    def create_mod(self, db_session, mod_id: int) -> ModModel:
        """Create a MOD (Model Organism Database) entry."""
        mod_data = [
            ("WB", "WB", "WormBase", ["6239"]),
            ("FB", "FB", "FlyBase", ["7227"]),
            ("SGD", "SGD", "Saccharomyces Genome Database", ["559292"]),
            ("RGD", "RGD", "Rat Genome Database", ["10116"]),
            ("MGI", "MGI", "Mouse Genome Informatics", ["10090"]),
            ("ZFIN", "ZFIN", "Zebrafish Information Network", ["7955"]),
            ("XB", "XB", "Xenbase", ["8355"])
        ]

        abbrev, short, full, taxons = mod_data[mod_id % len(mod_data)]

        mod = ModModel(
            abbreviation=f"{abbrev}{mod_id}",
            short_name=f"{short}{mod_id}",
            full_name=f"{full} {mod_id}",
            taxon_ids=taxons
        )
        db_session.add(mod)
        db_session.flush()
        return mod

    def create_referencetype(self, db_session, ref_type_id: int) -> ReferencetypeModel:
        """Create a reference type entry."""
        ref_types = [
            "Research Article",
            "Review",
            "News Article",
            "Editorial",
            "Comment",
            "Letter",
            "Book Chapter",
            "Conference Proceeding"
        ]

        ref_type = ReferencetypeModel(
            label=f"{ref_types[ref_type_id % len(ref_types)]} {ref_type_id}"
        )
        db_session.add(ref_type)
        db_session.flush()
        return ref_type

    def create_mod_referencetype_association(self, db_session, mod: ModModel,
                                             referencetype: ReferencetypeModel, order: int = 1) -> ModReferencetypeAssociationModel:
        """Create a MOD-referencetype association."""
        association = ModReferencetypeAssociationModel(
            mod_id=mod.mod_id,
            referencetype_id=referencetype.referencetype_id,
            display_order=order
        )
        db_session.add(association)
        db_session.flush()
        return association

    def create_reference_mod_referencetype_association(self, db_session, reference: ReferenceModel,
                                                       mod_referencetype: ModReferencetypeAssociationModel):
        """Create a reference-MOD-referencetype association."""
        association = ReferenceModReferencetypeAssociationModel(
            reference_id=reference.reference_id,
            mod_referencetype_id=mod_referencetype.mod_referencetype_id
        )
        db_session.add(association)
        db_session.flush()
        return association

    def create_mod_corpus_association(self, db_session, reference: ReferenceModel,
                                      mod: ModModel, corpus: bool = True) -> ModCorpusAssociationModel:
        """Create a MOD corpus association."""
        from agr_literature_service.api.schemas.mod_corpus_sort_source_type import ModCorpusSortSourceType

        association = ModCorpusAssociationModel(
            reference_id=reference.reference_id,
            mod_id=mod.mod_id,
            corpus=corpus,
            mod_corpus_sort_source=ModCorpusSortSourceType.Dqm_files
        )
        db_session.add(association)
        return association

    def create_topic_entity_tag_source(self, db_session, source_id: int, mod: ModModel) -> TopicEntityTagSourceModel:
        """Create a topic entity tag source entry."""
        data_providers = [
            "professional_biocurator",
            "author",
            "alliance_automated",
            "mod_automated"
        ]

        evidence_assertions = [
            "curator_judgement",
            "author_statement",
            "automated_inference",
            "computational_analysis"
        ]

        source = TopicEntityTagSourceModel(
            data_provider=data_providers[source_id % len(data_providers)],
            secondary_data_provider_id=mod.mod_id,
            source_evidence_assertion=evidence_assertions[source_id % len(evidence_assertions)],
            source_method=f"test_method_{source_id}",
            validation_type="manual_validation",
            description=f"Test source {source_id}"
        )
        db_session.add(source)
        db_session.flush()
        return source

    def create_topic_entity_tag(self, db_session, reference: ReferenceModel,
                                tag_id: int, source: TopicEntityTagSourceModel) -> TopicEntityTagModel:
        """Create a topic entity tag entry."""
        # Use real topic values that would be found in the system
        topics = [
            "ATP:0000000",  # ATP root term
            "GO:0008150",   # biological_process
            "DOID:4",       # disease
            "HGNC:5",       # gene symbol
            "NCBITaxon:10090"  # species
        ]

        tag = TopicEntityTagModel(
            reference_id=reference.reference_id,
            topic=topics[tag_id % len(topics)],
            entity_type="gene",
            entity="HGNC:12345",
            entity_id_validation="alliance",
            topic_entity_tag_source_id=source.topic_entity_tag_source_id,
            species="NCBITaxon:10090",
            negated=False,
            data_novelty="ATP:0000334"
        )
        db_session.add(tag)
        return tag

    def create_workflow_tag(self, db_session, reference: ReferenceModel, tag_id: int,
                            mod: ModModel = None) -> WorkflowTagModel:
        """Create a workflow tag entry."""
        # Use real workflow tag IDs that would be found in the system
        workflow_tag_ids = [
            "ATP:0000103",  # classification_flagged
            "ATP:0000104",  # entity_extraction_flagged
            "ATP:0000105",  # file_upload_flagged
            "ATP:0000106",  # text_conversion_flagged
            "ATP:0000107",  # stage_flagged
        ]

        tag = WorkflowTagModel(
            reference_id=reference.reference_id,
            workflow_tag_id=workflow_tag_ids[tag_id % len(workflow_tag_ids)],
            mod_id=mod.mod_id if mod else None
        )
        db_session.add(tag)
        return tag

    def create_obsolete_reference_curie(self, db_session, curie: str,
                                        new_reference: ReferenceModel = None) -> ObsoleteReferenceModel:
        """Create an obsolete reference curie entry."""
        obsolete = ObsoleteReferenceModel(
            curie=curie,
            new_id=new_reference.reference_id if new_reference else None
        )
        db_session.add(obsolete)
        return obsolete


def populate_database():
    """Populate the test database with mock data for Debezium integration tests."""
    print("Starting mock data population for Debezium integration tests...")
    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})

    initialize()
    db_session = sessionmaker(bind=engine, autoflush=True)()  # Create session
    db_session.commit()
    delete_all_table_content(engine, db_session)
    # Initialize database connection
    db = next(get_db())
    factory = MockDataFactory()

    try:
        # Create MODs (Model Organism Databases) - REQUIRED for Debezium
        print("Creating MODs...")
        mods = []
        for i in range(7):  # Create all major MODs
            mod = factory.create_mod(db, i)
            mods.append(mod)

        # Create reference types - REQUIRED for Debezium
        print("Creating reference types...")
        reference_types = []
        for i in range(8):
            ref_type = factory.create_referencetype(db, i)
            reference_types.append(ref_type)

        # Create MOD-referencetype associations - REQUIRED for Debezium
        print("Creating MOD-referencetype associations...")
        mod_ref_associations = []
        for _i, mod in enumerate(mods):
            for j, ref_type in enumerate(reference_types[:3]):  # Each MOD gets 3 ref types
                association = factory.create_mod_referencetype_association(db, mod, ref_type, j + 1)
                mod_ref_associations.append(association)

        # Create topic entity tag sources - REQUIRED for Debezium
        print("Creating topic entity tag sources...")
        tag_sources = []
        for i in range(4):
            mod = mods[i % len(mods)]  # Use different MODs for different sources
            source = factory.create_topic_entity_tag_source(db, i, mod)
            tag_sources.append(source)

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

        # Create copyright licenses early so they can be linked to references
        print("Creating copyright licenses...")
        copyright_licenses = []
        for i in range(5):
            license_obj = factory.create_copyright_license(db, i + 1)
            copyright_licenses.append(license_obj)
        db.flush()  # Ensure licenses have IDs before linking

        # Create references
        print("Creating references...")
        references = []
        for i in range(10):
            resource = resources[i % len(resources)]
            citation = citations[i % len(citations)]
            # Link some references to copyright licenses (about 60% of them)
            copyright_license_id = None
            if i % 5 < 3:  # Link references 0,1,2,5,6,7 to licenses
                license_obj = copyright_licenses[i % len(copyright_licenses)]
                copyright_license_id = license_obj.copyright_license_id
            reference = factory.create_reference(db, i + 1, citation, resource, copyright_license_id)
            references.append(reference)

            # Add authors for each reference
            factory.create_author(db, reference, i + 1)

            # Add cross-references (regular and obsolete)
            factory.create_cross_reference(db, reference, i + 1, False)
            if i % 3 == 0:  # Add some obsolete cross-references
                factory.create_cross_reference(db, reference, i + 1, True)

            # Add MeSH terms
            factory.create_mesh_detail(db, reference, i)

            # Add MOD corpus associations - REQUIRED for Debezium
            mod = mods[i % len(mods)]
            factory.create_mod_corpus_association(db, reference, mod, True)

            # Add topic entity tags - REQUIRED for Debezium
            source = tag_sources[i % len(tag_sources)]
            factory.create_topic_entity_tag(db, reference, i, source)

            # Add workflow tags - REQUIRED for Debezium
            workflow_mod = mods[i % len(mods)]
            factory.create_workflow_tag(db, reference, i, workflow_mod)

            # Add reference-MOD-referencetype associations - REQUIRED for Debezium reference_mod_referencetype topic
            # Link this reference to some MOD-referencetype associations
            ref_mod_associations = [assoc for assoc in mod_ref_associations if assoc.mod_id == workflow_mod.mod_id]
            if ref_mod_associations:
                # Pick the first association for this MOD
                factory.create_reference_mod_referencetype_association(db, reference, ref_mod_associations[0])

        # Create references WITHOUT a resource (testing null resource_id indexing)
        print("Creating references without resource...")
        no_resource_categories = [
            'Internal_Process_Reference',
            'Direct_Data_Submission',
            'Personal_Communication'
        ]
        for i, category in enumerate(no_resource_categories):
            ref_id = 100 + i
            citation = citations[i % len(citations)]
            ref = ReferenceModel(
                curie=f"AGRKB:10100{ref_id:04d}",
                title=f"No-resource ref {ref_id}: {category} test",
                abstract=f"Test abstract for {category} reference without resource.",
                category=category,
                citation_id=citation.citation_id,
                resource_id=None,
                date_published='2024-06-01',
                language='eng',
                keywords=['test', 'no-resource'],
                pubmed_types=[],
                volume='',
                issue_name='',
                page_range=''
            )
            db.add(ref)
            db.flush()
            references.append(ref)

            # Add author and cross-reference
            factory.create_author(db, ref, ref_id)
            factory.create_cross_reference(db, ref, ref_id, False)

            # Add MOD corpus association so it should appear in public index
            mod = mods[i % len(mods)]
            factory.create_mod_corpus_association(db, ref, mod, True)

            # Add workflow tag
            factory.create_workflow_tag(db, ref, ref_id, mod)

        # Create reference relations
        print("Creating reference relations...")
        for i in range(len(references) - 1):
            if i % 2 == 0:
                factory.create_reference_relation(db, references[i], references[i + 1], "CommentOn")
            else:
                factory.create_reference_relation(db, references[i], references[i + 1], "ErratumFor")

        # Copyright licenses already created earlier and linked to references

        # Create some obsolete reference curies - REQUIRED for Debezium
        print("Creating obsolete reference curies...")
        for i in range(3):
            ref = references[i] if i < len(references) else None
            factory.create_obsolete_reference_curie(db, f"OBSOLETE:test{i}", ref)

        # Commit all changes
        db.commit()

        # Print summary
        ref_count = db.query(ReferenceModel).count()
        author_count = db.query(AuthorModel).count()
        xref_count = db.query(CrossReferenceModel).count()
        relation_count = db.query(ReferenceRelationModel).count()
        license_count = db.query(CopyrightLicenseModel).count()
        mesh_count = db.query(MeshDetailModel).count()
        mod_count = db.query(ModModel).count()
        mod_corpus_count = db.query(ModCorpusAssociationModel).count()
        ref_type_count = db.query(ReferencetypeModel).count()
        mod_ref_type_count = db.query(ModReferencetypeAssociationModel).count()
        ref_mod_ref_type_count = db.query(ReferenceModReferencetypeAssociationModel).count()
        topic_tag_count = db.query(TopicEntityTagModel).count()
        tag_source_count = db.query(TopicEntityTagSourceModel).count()
        workflow_tag_count = db.query(WorkflowTagModel).count()
        obsolete_count = db.query(ObsoleteReferenceModel).count()

        print("Mock data population completed successfully!")
        print(f"Created: {ref_count} references, {author_count} authors, {xref_count} cross-refs")
        print(f"Created: {relation_count} relations, {license_count} licenses, {mesh_count} mesh terms")
        print(f"Created: {mod_count} MODs, {mod_corpus_count} MOD corpus associations")
        print(f"Created: {ref_type_count} reference types, {mod_ref_type_count} MOD-ref type assocs")
        print(f"Created: {ref_mod_ref_type_count} reference-MOD-ref type associations")
        print(f"Created: {topic_tag_count} topic tags, {tag_source_count} tag sources, "
              f"{workflow_tag_count} workflow tags")
        print(f"Created: {obsolete_count} obsolete reference curies")

    except Exception as e:
        print(f"Error populating database: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    populate_database()
