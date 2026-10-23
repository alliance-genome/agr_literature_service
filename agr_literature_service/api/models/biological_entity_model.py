"""A-team biological entities, read by ABC in the merged database (SCRUM-6720 Phase 2).

A topic entity tag names its entity by a MOD curie (topic_entity_tag.entity, e.g. "WB:WBGene00000001"),
which is A-team's biologicalentity.primaryexternalid. The tag keeps that string -- the search pipeline,
the API and the entities A-team does not hold (SGD protein complexes and pathways, WB transgenes,
deleted genes, species tags, whose entity is an NCBITaxon term) all still use it -- and also links
the entity when A-team has it: entity_id, an FK to biologicalentity.id, set from the string on every
write (register_entity_column). An entity A-team does not have leaves entity_id NULL; it is not an
error, since the entity is validated (or not) by entity_id_validation as before.

biologicalentity belongs to the A-team: like ontologyterm it is mapped read-only, flagged
ATEAM_TABLE_INFO, so ABC never creates, cleans or migrates it.
"""
from typing import Optional

from sqlalchemy import BigInteger, Boolean, Column, ForeignKey, String, event, select, inspect as sa_inspect
from sqlalchemy.orm import object_session

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models.ontology_term_model import ATEAM_TABLE_INFO


class BiologicalEntityModel(Base):
    __tablename__ = 'biologicalentity'
    __table_args__ = {'info': ATEAM_TABLE_INFO}

    id = Column(BigInteger, primary_key=True)
    curie = Column(String, unique=True)
    primaryexternalid = Column(String, unique=True)
    modinternalid = Column(String)
    taxon_id = Column(BigInteger, ForeignKey('ontologyterm.id'))
    obsolete = Column(Boolean, nullable=False)


_ENTITY = BiologicalEntityModel.__table__


def entity_id_for_curie(connection, curie: Optional[str]) -> Optional[int]:
    """biologicalentity.id of the entity whose primaryexternalid is curie, or None."""
    if curie is None:
        return None
    return connection.execute(select(_ENTITY.c.id).where(_ENTITY.c.primaryexternalid == curie)).scalar()


def register_entity_column(model, entity_attr: str = 'entity', id_attr: str = 'entity_id') -> None:
    """Keep model.<id_attr> the biologicalentity of the curie in model.<entity_attr>.

    As in ontology_term_model.register_curie_columns, an object already in a session is resolved when
    the curie is assigned, so SQLAlchemy-Continuum sees the id column change; a new object is resolved
    at insert."""

    def _resolve_at_flush(mapper, connection, target):
        history = sa_inspect(target).attrs[entity_attr].history
        if history.added or (history.deleted and not history.unchanged):
            setattr(target, id_attr, entity_id_for_curie(connection, getattr(target, entity_attr)))

    event.listen(model, 'before_insert', _resolve_at_flush)
    event.listen(model, 'before_update', _resolve_at_flush)

    def _resolve_on_set(target, value, _oldvalue, _initiator):
        session = object_session(target)
        if session is None or sa_inspect(target).transient or sa_inspect(target).pending:
            return value
        with session.no_autoflush:
            setattr(target, id_attr, entity_id_for_curie(session.connection(), value))
        return value

    event.listen(getattr(model, entity_attr), 'set', _resolve_on_set, retval=True)
