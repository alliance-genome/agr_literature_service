from sqlalchemy_continuum.plugins import Plugin


class UserPlugin(Plugin):
    def transaction_args(self, uow, session):
        """
        Provide integer users.user_id for the transaction row.
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
