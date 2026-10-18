"""History (Continuum) and serialization of the LinkML audit columns (SCRUM-6719 final review).

The LinkML columns are deferred so that serialized objects keep their keys; these tests pin
what that must not cost: history rows keep their audit ids (also on DELETE), an UPDATE does
not lazy-load them one by one, the *_mod flags land on renamed columns, and the API's history
(changesets) and TET projections keep showing labels, not ids.
"""
import re

from sqlalchemy import event, text
from sqlalchemy.orm import configure_mappers
from sqlalchemy_continuum import version_class
from sqlalchemy_continuum.utils import versioned_column_properties
from starlette.testclient import TestClient

from agr_literature_service.api.crud.topic_entity_tag_crud import _orm_column_keys
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.main import app
from agr_literature_service.api.models import CrossReferenceModel, TopicEntityTagModel

from ..fixtures import db  # noqa: F401
from .fixtures import auth_headers  # noqa: F401
from .test_cross_ref import test_cross_reference  # noqa: F401
from .test_reference import test_reference  # noqa: F401

LINKML_DEFERRED = {'created_by_id', 'updated_by_id', 'db_date_created', 'db_date_updated', 'internal', 'obsolete'}
# a lazy load of one deferred column: SELECT <table>.<column> AS <table>_<column> FROM <table> WHERE ...
SINGLE_DEFERRED_LOAD = re.compile(r'^SELECT \w+\.(created_by_id|updated_by_id|db_date_created|db_date_updated|'
                                  r'internal|obsolete) AS \w+\s+FROM')


def _versioned_models():
    configure_mappers()
    return [mapper.class_ for mapper in Base.registry.mappers if getattr(mapper.class_, '__versioned__', None) is not None]


def _xref_id(test_cross_reference):  # noqa
    return test_cross_reference.response.json()['cross_reference_id']


def test_orm_column_keys_skip_deferred_linkml_columns():
    keys = set(_orm_column_keys(TopicEntityTagModel))
    assert not keys & LINKML_DEFERRED
    assert {'topic_entity_tag_id', 'created_by', 'updated_by'} <= keys


def test_mod_columns_follow_attribute_keys():
    """PropertyModTrackerPlugin sets <attribute key>_mod: every versioned attribute needs that key."""
    missing = [f'{cls.__name__}.{prop.key}_mod' for cls in _versioned_models()
               for prop in versioned_column_properties(cls)
               if not prop.columns[0].primary_key and not hasattr(version_class(cls), f'{prop.key}_mod')]
    assert missing == []


def test_cross_reference_none_is_obsolete_becomes_false():
    assert CrossReferenceModel(curie='XREF:1', curie_prefix='XREF', is_obsolete=None).is_obsolete is False


def test_obsoleting_sets_obsolete_mod(db, test_cross_reference):  # noqa
    xref = db.get(CrossReferenceModel, _xref_id(test_cross_reference))
    xref.is_obsolete = True
    db.commit()
    flag = db.execute(text('SELECT obsolete_mod FROM cross_reference_version WHERE id = :i '
                           'ORDER BY transaction_id DESC LIMIT 1'), {'i': xref.cross_reference_id}).scalar()
    assert flag is True


def test_update_loads_linkml_columns_without_one_query_each(db, test_cross_reference):  # noqa
    xref_id = _xref_id(test_cross_reference)
    db.expunge_all()
    xref = db.get(CrossReferenceModel, xref_id)
    statements = []

    def record(conn, cursor, statement, parameters, context, executemany):  # noqa
        statements.append(statement)

    engine = db.get_bind()
    event.listen(engine, 'before_cursor_execute', record)
    try:
        xref.pages = ['reference', 'linked']
        db.commit()
    finally:
        event.remove(engine, 'before_cursor_execute', record)
    assert [s for s in statements if SINGLE_DEFERRED_LOAD.match(s.strip())] == []


def test_delete_history_keeps_audit_ids(db, test_cross_reference):  # noqa
    xref_id = _xref_id(test_cross_reference)
    db.expunge_all()
    xref = db.get(CrossReferenceModel, xref_id)
    created_by_id = db.execute(text('SELECT created_by_id FROM cross_reference WHERE id = :i'), {'i': xref_id}).scalar()
    db.delete(xref)
    db.commit()
    row = db.execute(text('SELECT created_by_id, updated_by_id, db_date_created FROM cross_reference_version '
                          'WHERE id = :i AND operation_type = 2'), {'i': xref_id}).one()
    assert row.created_by_id == created_by_id
    assert row.updated_by_id is not None
    assert row.db_date_created is not None


def test_changeset_shows_audit_labels(db, auth_headers, test_cross_reference):  # noqa
    with TestClient(app) as client:
        response = client.get(url=f'/cross_reference/{_xref_id(test_cross_reference)}/versions', headers=auth_headers)
    changeset = response.json()[0]['changeset']
    assert isinstance(changeset['created_by'][1], str)
    assert not set(changeset) & {'created_by_id', 'updated_by_id', 'db_date_created', 'db_date_updated', 'internal'}
