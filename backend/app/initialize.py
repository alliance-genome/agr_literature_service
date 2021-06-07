import urllib.request
import yaml

from sqlalchemy.orm import Session

from literature.config import config
from literature import database

from literature.models.resource_descriptor import ResourceDescriptor
from literature.models.resource_descriptor import ResourceDescriptorPage


def update_resource_descriptor(db: Session):
    with urllib.request.urlopen(config.RESOURCE_DESCRIPTOR_URL) as response:
        resource_descriptors = yaml.full_load(response)

        db.query(ResourceDescriptor).delete()

        for resource_descriptor in resource_descriptors:
            resource_descriptor_data = dict()
            for field, value in resource_descriptor.items():
               if field == 'pages':
                   page_objs = []
                   for page in value:
                       page_obj = ResourceDescriptorPage(name=page['name'],
                                                         url=page['url'])
                       db.add(page_obj)
                       page_objs.append(page_obj)
                   resource_descriptor_data['pages'] = page_objs
               elif field == 'example_id':
                   resource_descriptor_data['example_gid'] = value
               else:
                   resource_descriptor_data[field] = value

            resource_descriptor_obj = ResourceDescriptor(**resource_descriptor_data)
            db.add(resource_descriptor_obj)
        db.commit()

    return resource_descriptors


async def setup_resource_descriptor(db: Session):
    resource_descriptor_yaml = update_resource_descriptor(db)
