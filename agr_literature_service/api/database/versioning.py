from sqlalchemy_continuum import make_versioned
from sqlalchemy_continuum.plugins import PropertyModTrackerPlugin
from agr_literature_service.api.continuum_plugins import UserPlugin


def enable_versioning():
    user_plugin = UserPlugin()
    make_versioned(user_cls='UserModel', plugins=[user_plugin, PropertyModTrackerPlugin()])
