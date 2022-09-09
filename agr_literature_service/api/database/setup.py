from agr_literature_service.api.initialize import setup_resource_descriptor
from agr_literature_service.api.models import initialize


def setup_database():
    """

    :return:
    """
    initialize()
    setup_resource_descriptor()
