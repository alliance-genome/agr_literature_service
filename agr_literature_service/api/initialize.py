# api/initialize.py

import sys
import urllib.request
from urllib.error import URLError
from urllib.parse import urlparse
import yaml
import logging
from sqlalchemy.orm import Session

from agr_literature_service.api.config import config
from agr_literature_service.api.models.resource_descriptor_models import (
    ResourceDescriptorModel, ResourceDescriptorPageModel
)
from agr_literature_service.api.database.main import get_db
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session
)

logger = logging.getLogger(__name__)

# start with a DB session you can override in initialize_database()
db_session: Session = create_postgres_session(False)


def initialize_database():
    global db_session
    # grab the first session from the FastAPI dependency generator
    db_session = next(get_db(), None)


def update_resource_descriptor(db: Session = None):
    """
    Fetch the YAML at RESOURCE_DESCRIPTOR_URL and reload the
    ResourceDescriptorModel + ResourceDescriptorPageModel tables.
    """
    if db is None:
        db = db_session

    raw = config.RESOURCE_DESCRIPTOR_URL or ""
    # strip whitespace and any angle‑brackets or quotes
    url = raw.strip().strip("<>'\" ")
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        logger.error(
            f"Invalid RESOURCE_DESCRIPTOR_URL, must start with http:// or https:// — got: {raw!r}"
        )
        sys.exit(-1)

    try:
        with urllib.request.urlopen(url) as resp:
            # read + decode before passing to yaml
            body = resp.read().decode("utf-8")
            descriptors = yaml.full_load(body)

        # clear out old descriptors
        db.query(ResourceDescriptorModel).delete()

        for rd in descriptors:
            data = {}
            pages = []
            for key, val in rd.items():
                if key == "pages":
                    for p in val:
                        page = ResourceDescriptorPageModel(
                            name=p.get("name"),
                            url=p.get("url")
                        )
                        db.add(page)
                        pages.append(page)
                    data["pages"] = pages
                elif key == "example_id":
                    data["example_gid"] = val
                else:
                    data[key] = val

            obj = ResourceDescriptorModel(**data)
            db.add(obj)

        db.commit()
    except URLError as e:
        logger.error(f"Could not fetch resource descriptor from {url}: {e}")
        sys.exit(-1)
    except Exception as e:
        logger.error(f"Unable to process resource_descriptor '{url}': {e}")
        sys.exit(-1)

    return descriptors


def setup_resource_descriptor():
    initialize_database()
    update_resource_descriptor()


if __name__ == '__main__':
    setup_resource_descriptor()
