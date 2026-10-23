from sqlalchemy import text
from sqlalchemy.orm import object_session
from sqlalchemy_continuum.plugins import Plugin


class UserPlugin(Plugin):
    def transaction_args(self, uow, session):
        """
        Provide the current user's agent.id for the transaction row.
        Falls back to NULL if no current user is set.

        ``api.user`` is imported lazily: this module is pulled in at import
        time via models -> versioning, and a module-level import of
        ``api.user`` here closes a circular import loop when a script
        imports ``api.user`` first.
        """
        from agr_literature_service.api.user import get_current_user_pk
        return {
            'user_id': get_current_user_pk(session)
        }


class AuditLabelPlugin(Plugin):
    """
    Keep history changesets in the API's terms (SCRUM-6719): created_by /
    updated_by as agent labels rather than the created_by_id / updated_by_id
    integers they are stored as, and without the LinkML-only columns, which
    the API does not expose.
    """
    HIDDEN = ('db_date_created', 'db_date_updated', 'internal', 'obsolete')
    LABELS = (('created_by_id', 'created_by'), ('updated_by_id', 'updated_by'))
    _SQL_LABELS = text(
        "SELECT a.id, COALESCE(a.automation_username, p.curie) FROM agent a "
        "LEFT JOIN person p ON p.id = a.person_id WHERE a.id = ANY(:ids)"
    )

    def after_construct_changeset(self, version_obj, changeset):
        for key in self.HIDDEN:
            changeset.pop(key, None)
        id_keys = [id_key for id_key, _label in self.LABELS if id_key in changeset]
        if not id_keys:
            return
        ids = sorted({value for key in id_keys for value in changeset[key] if value is not None})
        labels = {}
        if ids:
            session = object_session(version_obj)
            labels = dict(session.execute(self._SQL_LABELS, {"ids": ids}).all())
        for id_key, label_key in self.LABELS:
            if id_key in changeset:
                changeset[label_key] = [labels.get(value) for value in changeset.pop(id_key)]


class CurieLabelPlugin(Plugin):
    """
    Keep history changesets in the API's terms (SCRUM-6720): an ontology curie column
    stored as an ontologyterm id (register_curie_columns) shows as its curie attribute
    with curies -- workflow_tag_id: ['ATP:...', 'ATP:...'] -- not as the id column with
    integers.
    """
    _SQL_CURIES = text("SELECT id, curie FROM ontologyterm WHERE id = ANY(:ids)")

    def after_construct_changeset(self, version_obj, changeset):
        from sqlalchemy_continuum import parent_class
        curie_columns = getattr(parent_class(type(version_obj)), '__curie_columns__', {})
        present = [(curie_attr, id_attr) for curie_attr, id_attr in curie_columns.items() if id_attr in changeset]
        if not present:
            return
        ids = sorted({value for _curie_attr, id_attr in present for value in changeset[id_attr] if value is not None})
        curies = {}
        if ids:
            curies = dict(object_session(version_obj).execute(self._SQL_CURIES, {"ids": ids}).all())
        for curie_attr, id_attr in present:
            changeset[curie_attr] = [curies.get(value) for value in changeset.pop(id_attr)]
