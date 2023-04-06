from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import configure_mappers, create_session

from agr_literature_service.api.database.main import create_all_tables, create_default_user, create_all_triggers
from agr_literature_service.api.models.author_model import AuthorModel
from agr_literature_service.api.models.cross_reference_model import CrossReferenceModel
from agr_literature_service.api.models.editor_model import EditorModel
from agr_literature_service.api.models.mesh_detail_model import MeshDetailModel
from agr_literature_service.api.models.mod_reference_type_model import ReferencetypeModel, \
    ModReferencetypeAssociationModel, ReferenceModReferencetypeAssociationModel
from agr_literature_service.api.models.mod_model import ModModel
from agr_literature_service.api.models.mod_corpus_association_model import ModCorpusAssociationModel
from agr_literature_service.api.models.reference_comment_and_correction_model import \
    ReferenceCommentAndCorrectionModel
from agr_literature_service.api.models.reference_model import ReferenceModel
from agr_literature_service.api.models.resource_descriptor_models import (
    ResourceDescriptorModel, ResourceDescriptorPageModel)
from agr_literature_service.api.models.resource_model import ResourceModel
from agr_literature_service.api.models.user_model import UserModel
from agr_literature_service.api.models.obsolete_model import ObsoleteReferenceModel
from agr_literature_service.api.models.workflow_tag_model import WorkflowTagModel
from agr_literature_service.api.models.topic_entity_tag_model import (
    TopicEntityTagModel,
    TopicEntityTagPropModel
)
from agr_literature_service.api.models.mod_taxon_model import ModTaxonModel
from agr_literature_service.api.models.reference_mod_md5sum_model import ReferenceModMd5sumModel
from agr_literature_service.api.models.referencefile_model import ReferencefileModel, ReferencefileModAssociationModel
from agr_literature_service.api.models.copyright_license_model import CopyrightLicenseModel
from agr_literature_service.api.models.citation_model import CitationModel
import logging

logger = logging.getLogger(__name__)


def initialize():
    # logging.basicConfig(filename='/mnt/d/alliance/agr_literature_service/python.log',level=logging.DEBUG)
    logger.warning('Initialising models')
    print('Initialising models')
    try:
        configure_mappers()
    except Exception as e:
        logger.error('configure Mappers Error: ' + str(type(e)))
        logger.error(e)

    try:
        create_all_tables()
    except Exception as e:
        logger.error('Create all tables Error: ' + str(type(e)))
        logger.error(e)
    create_default_user()

    try:
        create_all_triggers()
    except Exception as e:
        logger.error('Create triggers Error: ' + str(type(e)))
        logger.error(e)
