#!/usr/bin/env python3
"""
Create a minimal test dataset for public index testing.
This script creates a small subset of data with all the required tables populated.
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agr_literature_service.api.database import get_db
from agr_literature_service.api.models.reference_model import Reference
from agr_literature_service.api.models.citation_model import Citation
from agr_literature_service.api.models.author_model import Author
from agr_literature_service.api.models.cross_reference_model import CrossReference
from agr_literature_service.api.models.reference_relation_model import ReferenceRelation
from agr_literature_service.api.models.copyright_license_model import CopyrightLicense
from agr_literature_service.api.models.mesh_detail_model import MeshDetail
from agr_literature_service.api.models.resource_model import Resource
from sqlalchemy.orm import Session

def create_test_subset():
    """Create a minimal test dataset with all tables populated."""
    
    # Get database session
    db_gen = get_db()
    db = next(db_gen)
    
    try:
        print("🔧 Creating test subset with all required tables...")
        
        # Create test resource
        resource = Resource(
            title="Test Genomics Journal",
            iso_abbreviation="Test Genom J",
            medline_abbreviation="Test Genom J",
            issn_print="1234-5678",
            issn_electronic="8765-4321"
        )
        db.add(resource)
        db.flush()  # Get the ID
        
        # Create test citations
        citations = []
        for i in range(3):
            citation = Citation(
                citation=f"Test Citation {i+1}. Test Journal. 2024;{i+1}:123-456.",
                short_citation=f"Test et al. 2024"
            )
            db.add(citation)
            citations.append(citation)
        db.flush()
        
        # Create test references
        references = []
        for i, citation in enumerate(citations):
            reference = Reference(
                curie=f"AGRKB:101000{i+1}",
                title=f"Test Reference {i+1}: Genomics Study",
                abstract=f"This is a test abstract for reference {i+1}. It describes important genomics research findings.",
                category="Research_Article",
                citation_id=citation.citation_id,
                resource_id=resource.resource_id,
                date_published="2024-01-01",
                date_created=1640995200000,  # 2022-01-01 as timestamp
                language="english",
                publisher="Test Publisher",
                keywords=["genomics", "bioinformatics", f"test{i+1}"],
                pubmed_types=["Journal Article"],
                pubmed_publication_status="published",
                volume=f"Vol{i+1}",
                issue_name=f"Issue{i+1}",
                page_range=f"{100+i*10}-{110+i*10}"
            )
            db.add(reference)
            references.append(reference)
        db.flush()
        
        # Create test authors
        for i, ref in enumerate(references):
            author = Author(
                reference_id=ref.reference_id,
                name=f"Test Author {i+1}",
                orcid=f"0000-0000-0000-000{i+1}"
            )
            db.add(author)
        
        # Create test cross-references
        for i, ref in enumerate(references):
            # Regular cross-reference
            xref = CrossReference(
                reference_id=ref.reference_id,
                curie=f"PMID:1234567{i+1}",
                is_obsolete=False
            )
            db.add(xref)
            
            # Obsolete cross-reference
            xref_obsolete = CrossReference(
                reference_id=ref.reference_id,
                curie=f"DOI:10.1000/test{i+1}_obsolete",
                is_obsolete=True
            )
            db.add(xref_obsolete)
        
        # Create test reference relations
        if len(references) >= 2:
            relation = ReferenceRelation(
                reference_id_from=references[0].reference_id,
                reference_id_to=references[1].reference_id,
                reference_relation_type="Reviews"
            )
            db.add(relation)
            
            relation2 = ReferenceRelation(
                reference_id_from=references[1].reference_id,
                reference_id_to=references[2].reference_id,
                reference_relation_type="Cites"
            )
            db.add(relation2)
        
        # Create test copyright licenses
        for i, ref in enumerate(references):
            copyright_license = CopyrightLicense(
                reference_id=ref.reference_id,
                copyright_license_text=f"Creative Commons Attribution {i+1}.0 License",
                copyright_license_url=f"https://creativecommons.org/licenses/by/{i+1}.0/",
                open_access=(i % 2 == 0)  # Alternate between open access and not
            )
            db.add(copyright_license)
        
        # Create test mesh terms
        mesh_terms = [
            ("Genomics", "methods"),
            ("Bioinformatics", "classification"), 
            ("Gene Expression", "genetics"),
            ("Proteomics", "analysis"),
            ("Systems Biology", "methods")
        ]
        
        for i, ref in enumerate(references):
            for j, (heading, qualifier) in enumerate(mesh_terms[:2]):  # 2 mesh terms per reference
                mesh = MeshDetail(
                    reference_id=ref.reference_id,
                    mesh_heading_term=heading,
                    mesh_qualifier_term=qualifier
                )
                db.add(mesh)
        
        # Commit all changes
        db.commit()
        
        print(f"✅ Created test subset:")
        print(f"   - {len(references)} references")
        print(f"   - {len(references)} authors")
        print(f"   - {len(references)*2} cross-references (including obsolete)")
        print(f"   - 2 reference relations")
        print(f"   - {len(references)} copyright licenses")
        print(f"   - {len(references)*2} mesh terms")
        print(f"   - 1 resource")
        print(f"   - {len(citations)} citations")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating test subset: {e}")
        db.rollback()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    # Set test environment
    os.environ['ENV_STATE'] = 'test'
    
    success = create_test_subset()
    if success:
        print("\n🎉 Test subset created successfully!")
        print("You can now run the Debezium setup to test the public index.")
    else:
        print("\n❌ Failed to create test subset.")
        sys.exit(1)