from sqlalchemy_continuum.plugins import Plugin

from agr_literature_service.api.user import get_current_user_pk


class UserPlugin(Plugin):
    def transaction_args(self, uow, session):
        """
        Provide integer users.user_id for the transaction row.
        Falls back to NULL if no current user is set.
        """
        return {
            'user_id': get_current_user_pk(session)
        }
