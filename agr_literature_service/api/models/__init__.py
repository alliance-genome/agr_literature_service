from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import configure_mappers, create_session

from agr_literature_service.api.database.main import create_all_tables, create_default_user
from agr_literature_service.api.models.author_model import AuthorModel
from agr_literature_service.api.models.cross_reference_model import CrossReferenceModel
from agr_literature_service.api.models.editor_model import EditorModel
from agr_literature_service.api.models.mesh_detail_model import MeshDetailModel
from agr_literature_service.api.models.mod_reference_type_model import ReferenceTypeModel, \
    ModReferenceTypeAssociationModel, ReferenceModReferenceTypeAssociationModel
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


def initialize():
    configure_mappers()
    create_all_tables()
    create_default_user()
