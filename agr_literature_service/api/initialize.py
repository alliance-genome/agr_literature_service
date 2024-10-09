import urllib.request

import yaml
import logging
from sqlalchemy.orm import Session

from agr_literature_service.api.config import config
from agr_literature_service.api.models.resource_descriptor_models import (
    ResourceDescriptorModel, ResourceDescriptorPageModel)
from agr_literature_service.api.database.main import get_db
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session

db_session: Session = create_postgres_session(False)


def initialize_database():
    global db_session
    db_session = next(get_db(), None)


def update_resource_descriptor(db: Session = db_session):
    """
    :param db:
    :return:
    """

    try:
        with urllib.request.urlopen(config.RESOURCE_DESCRIPTOR_URL) as response:
            resource_descriptors = yaml.full_load(response)

            db.query(ResourceDescriptorModel).delete()

            for resource_descriptor in resource_descriptors:
                resource_descriptor_data = dict()
                for field, value in resource_descriptor.items():
                    if field == 'pages':
                        page_objs = []
                        for page in value:
                            page_obj = ResourceDescriptorPageModel(name=page['name'],
                                                                   url=page['url'])
                            db.add(page_obj)
                            page_objs.append(page_obj)
                        resource_descriptor_data['pages'] = page_objs
                    elif field == 'example_id':
                        resource_descriptor_data['example_gid'] = value
                    else:
                        resource_descriptor_data[field] = value

                resource_descriptor_obj = ResourceDescriptorModel(**resource_descriptor_data)
                db.add(resource_descriptor_obj)
            db.commit()
    except Exception as e:
        logging.error(f"Unable to process resource_descriptor '{config.RESOURCE_DESCRIPTOR_URL}' with error {e}")
        exit(-1)

    return resource_descriptors


def setup_resource_descriptor():
    """
    :return:
    """

    initialize_database()
    global db_session
    update_resource_descriptor(db_session)


if __name__ == '__main__':
    setup_resource_descriptor()
