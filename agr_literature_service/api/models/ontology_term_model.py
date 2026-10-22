"""A-team ontology terms, read by ABC in the merged database (SCRUM-6720 Phase 1).

ABC columns that held an ontology curie (ATP:, NCBITaxon:, ECO:) hold a foreign key to A-team's
ontologyterm.id instead. The API keeps speaking curies, as SCRUM-6719 did for created_by:

  <column>_id   the integer column in the database (FK ontologyterm.id)
  <column>      the curie, a column_property read with the row; assigning a curie sets
                <column>_id at flush (register_curie_columns)

Filters on the curie attribute (Model.topic == 'ATP:...', .in_([...]), .is_(None)) compare
<column>_id against the term's id, so they use the indexes on ontologyterm.curie and on
<column>_id instead of evaluating a subquery for every row.

ontologyterm belongs to the A-team: it is mapped here (read-only) so foreign keys and joins can
name it, but it is flagged ATEAM_TABLE_INFO, so ABC never creates it (create_all_tables), cleans
it (tests) or autogenerates migrations for it (alembic/env.py).
"""
from typing import Dict, Iterable, Optional, Tuple

from sqlalchemy import BigInteger, Boolean, Column, String, event, select, inspect as sa_inspect
from sqlalchemy.orm import ColumnProperty, column_property

from agr_literature_service.api.database.base import Base

ATEAM_TABLE_INFO = {'ateam': True}


def is_ateam_table(table) -> bool:
    return bool(table.info.get('ateam'))


class OntologyTermModel(Base):
    __tablename__ = 'ontologyterm'
    __table_args__ = {'info': ATEAM_TABLE_INFO}

    id = Column(BigInteger, primary_key=True)
    curie = Column(String, nullable=False, unique=True)
    name = Column(String)
    ontologytermtype = Column(String, nullable=False)
    namespace = Column(String)
    obsolete = Column(Boolean, nullable=False)


class UnknownOntologyTermError(ValueError):
    """A curie that is not an ontologyterm (of the allowed types) was assigned to a curie column."""


_TERM = OntologyTermModel.__table__


def _term_id_select(condition):
    return select(_TERM.c.id).where(condition).scalar_subquery()


class CurieComparator(ColumnProperty.Comparator):
    """Comparisons on the curie compare the id column, so they can use its index."""

    @property
    def _id_column(self):
        return self.prop.info['curie_id_column']

    def __eq__(self, other):
        if other is None:
            return self._id_column.is_(None)
        return self._id_column == _term_id_select(_TERM.c.curie == other)

    def __ne__(self, other):
        if other is None:
            return self._id_column.is_not(None)
        return self._id_column != _term_id_select(_TERM.c.curie == other)

    def in_(self, other):
        return self._id_column.in_(select(_TERM.c.id).where(_TERM.c.curie.in_(other)))

    def not_in(self, other):
        return self._id_column.not_in(select(_TERM.c.id).where(_TERM.c.curie.in_(other)))

    def is_(self, other):
        return self._id_column.is_(other)

    def is_not(self, other):
        return self._id_column.is_not(other)


def ontology_curie(id_column) -> ColumnProperty:
    """The curie of id_column's ontology term, read with the row (correlated on the primary key)."""
    term = _TERM.alias('curie_term')
    expression = (select(term.c.curie).where(term.c.id == id_column).correlate_except(term).scalar_subquery())
    return column_property(expression, comparator_factory=CurieComparator, info={'curie_id_column': id_column})


def term_id_for_curie(connection, curie: Optional[str], types: Iterable[str]) -> Optional[int]:
    """ontologyterm.id of curie, which must be one of the given ontologytermtypes."""
    if curie is None:
        return None
    row = connection.execute(select(_TERM.c.id, _TERM.c.ontologytermtype).where(_TERM.c.curie == curie)).first()
    allowed = tuple(types)
    if row is None or row.ontologytermtype not in allowed:
        raise UnknownOntologyTermError(f"{curie} is not a known {' or '.join(allowed)} ontology term")
    return row.id


def register_curie_columns(model, columns: Dict[str, Tuple[str, Tuple[str, ...]]]) -> None:
    """Resolve curies assigned to model's curie attributes into their id columns at flush.

    columns: {curie attribute: (id attribute, allowed ontologytermtypes)}
    """
    def _resolve(mapper, connection, target):
        state = sa_inspect(target)
        for curie_attr, (id_attr, types) in columns.items():
            history = state.attrs[curie_attr].history
            if history.added:
                setattr(target, id_attr, term_id_for_curie(connection, history.added[0], types))
            elif history.deleted and not history.unchanged:
                setattr(target, id_attr, None)

    event.listen(model, 'before_insert', _resolve)
    event.listen(model, 'before_update', _resolve)
