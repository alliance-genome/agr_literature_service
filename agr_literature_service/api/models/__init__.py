from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import configure_mappers, create_session

from agr_literature_service.api.database.main import (
    create_all_tables,
    create_default_user,
    create_all_triggers,
    drop_open_db_sessions)
from agr_literature_service.api.models.author_model import AuthorModel
from agr_literature_service.api.models.cross_reference_model import CrossReferenceModel
from agr_literature_service.api.models.editor_model import EditorModel
from agr_literature_service.api.models.mesh_detail_model import MeshDetailModel
from agr_literature_service.api.models.mod_reference_type_model import ReferencetypeModel, \
    ModReferencetypeAssociationModel, ReferenceModReferencetypeAssociationModel
from agr_literature_service.api.models.mod_model import ModModel
from agr_literature_service.api.models.mod_corpus_association_model import ModCorpusAssociationModel
from agr_literature_service.api.models.reference_relation_model import \
    ReferenceRelationModel
from agr_literature_service.api.models.reference_model import ReferenceModel
from agr_literature_service.api.models.resource_descriptor_models import (
    ResourceDescriptorModel, ResourceDescriptorPageModel)
from agr_literature_service.api.models.resource_model import ResourceModel
from agr_literature_service.api.models.user_model import UserModel
from agr_literature_service.api.models.obsolete_model import ObsoleteReferenceModel
from agr_literature_service.api.models.workflow_tag_model import WorkflowTagModel
from agr_literature_service.api.models.workflow_transition_model import WorkflowTransitionModel
from agr_literature_service.api.models.topic_entity_tag_model import TopicEntityTagModel, TopicEntityTagSourceModel
from agr_literature_service.api.models.reference_mod_md5sum_model import ReferenceModMd5sumModel
from agr_literature_service.api.models.referencefile_model import ReferencefileModel, ReferencefileModAssociationModel
from agr_literature_service.api.models.copyright_license_model import CopyrightLicenseModel
from agr_literature_service.api.models.citation_model import CitationModel
import logging

logger = logging.getLogger(__name__)


def initialize():
    # logging.basicConfig(filename='/mnt/d/alliance/agr_literature_service/python.log',level=logging.DEBUG)
    logger.debug('Initialising models')
    print('Initialising models')
    try:
        configure_mappers()
        print('Mappers initialized')
    except Exception as e:
        logger.error('configure Mappers Error: ' + str(type(e)))
        logger.error(e)

    try:
        create_all_tables()
        print('Tables created')
    except Exception as e:
        logger.error('Create all tables Error: ' + str(type(e)))
        logger.error(e)
    create_default_user()
    print('Default user created')

    try:
        #create_all_triggers()
        logger.debug("Triggers updated successfully")
    except Exception as e:
        logger.error('Create triggers Error: ' + str(type(e)))
        logger.error(e)
