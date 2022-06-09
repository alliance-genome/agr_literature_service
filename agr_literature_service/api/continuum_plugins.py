from sqlalchemy_continuum.plugins import Plugin

from agr_literature_service.api.user import get_global_user_id


class UserPlugin(Plugin):

    def transaction_args(self, uow, session):
        return {
            'user_id': get_global_user_id()
        }
