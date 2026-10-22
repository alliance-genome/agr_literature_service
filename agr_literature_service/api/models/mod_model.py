"""
mod_model.py
===============
"""


from typing import Dict
from sqlalchemy import BigInteger, Column, ForeignKey, Integer, String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel
from agr_literature_service.api.models.ontology_term_model import ontology_curie, register_curie_columns

enable_versioning()


class ModModel(Base, AuditedModel):
    __tablename__ = "mod"
    __versioned__: Dict = {}

    mod_id = Column(
        'id',
        Integer,
        primary_key=True,
        autoincrement=True
    )

    abbreviation = Column(
        String(10),
        unique=True,
        nullable=False
    )

    short_name = Column(
        String(10),
        unique=True,
        nullable=False
    )

    full_name = Column(
        String(100),
        unique=True,
        nullable=False
    )

    # SCRUM-6720 Phase 2: the MOD's NCBITaxon terms, in order (the first is its main taxon), are rows
    # of mod_taxon; taxon_ids stays the list of their curies, read and assigned as before
    taxons = relationship(
        "ModTaxonModel",
        order_by="ModTaxonModel.ordinal",
        collection_class=ordering_list("ordinal"),
        cascade="all, delete-orphan",
        lazy="selectin"
    )

    taxon_ids = association_proxy("taxons", "taxon", creator=lambda curie: ModTaxonModel(taxon=curie))

    referencetypes = relationship(
        "ModReferencetypeAssociationModel",
        back_populates="mod"
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return f"{self.abbreviation} {self.full_name} {self.taxon_ids}"


class ModTaxonModel(Base):
    """A taxon of a MOD (SCRUM-6720 Phase 2; was the mod.taxon_ids array). Not versioned: the
    taxa of a MOD are reference data that practically never change."""
    __tablename__ = "mod_taxon"

    mod_taxon_id = Column('id', Integer, primary_key=True, autoincrement=True)
    mod_id = Column(Integer, ForeignKey("mod.id", ondelete="CASCADE"), index=True, nullable=False)
    ordinal = Column(Integer, nullable=False)
    ontologyterm_id = Column(BigInteger, ForeignKey("ontologyterm.id"), nullable=False)
    taxon = ontology_curie(ontologyterm_id)


register_curie_columns(ModTaxonModel, {"taxon": ("ontologyterm_id", ("NCBITaxonTerm",))})
