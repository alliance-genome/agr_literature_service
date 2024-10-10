from sqlalchemy_continuum import make_versioned  # type: ignore
from sqlalchemy_continuum.plugins import PropertyModTrackerPlugin  # type: ignore
from agr_literature_service.api.continuum_plugins import UserPlugin
from agr_literature_service.global_utils import execute_once
from sqlalchemy_continuum.plugins import TransactionChangesPlugin
from sqlalchemy_continuum import VersioningManager

@execute_once
def enable_versioning():
    #make_versioned()  # Call without arguments to set up default versioning
    user_plugin = UserPlugin()
    make_versioned(user_cls='UserModel', plugins=[user_plugin, PropertyModTrackerPlugin()])


