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
from agr_literature_service.api.models.person_model import PersonModel
from agr_literature_service.api.models.email_model import EmailModel
from agr_literature_service.api.models.person_cross_reference_model import PersonCrossReferenceModel
from agr_literature_service.api.models.obsolete_model import ObsoleteReferenceModel
from agr_literature_service.api.models.workflow_tag_model import WorkflowTagModel
from agr_literature_service.api.models.workflow_tag_topic_model import WorkflowTagTopicModel
from agr_literature_service.api.models.workflow_transition_model import WorkflowTransitionModel
from agr_literature_service.api.models.topic_entity_tag_model import TopicEntityTagModel, TopicEntityTagSourceModel
from agr_literature_service.api.models.reference_mod_md5sum_model import ReferenceModMd5sumModel
from agr_literature_service.api.models.referencefile_model import ReferencefileModel, ReferencefileModAssociationModel
from agr_literature_service.api.models.copyright_license_model import CopyrightLicenseModel
from agr_literature_service.api.models.citation_model import CitationModel
from agr_literature_service.api.models.dataset_model import DatasetModel
from agr_literature_service.api.models.ml_model_model import MLModel
from agr_literature_service.api.models.curation_status_model import CurationStatusModel
from agr_literature_service.api.models.indexing_priority_model import IndexingPriorityModel
from agr_literature_service.api.models.manual_indexing_tag_model import ManualIndexingTagModel

import logging

logger = logging.getLogger(__name__)


def initialize():
    import os
    pid = os.getpid()

    logger.info(f'[PID:{pid}] Initializing database models...')
    print(f'[PID:{pid}] Initializing database models...')

    try:
        logger.info(f'[PID:{pid}] Configuring SQLAlchemy mappers...')
        configure_mappers()
        logger.info(f'[PID:{pid}] Mappers configured successfully')
        print(f'[PID:{pid}] Mappers configured successfully')
    except Exception as e:
        logger.error(f'[PID:{pid}] Configure mappers error: {type(e).__name__}: {e}')
        raise

    try:
        logger.info(f'[PID:{pid}] Creating database tables...')
        create_all_tables()
        logger.info(f'[PID:{pid}] Tables created successfully')
        print(f'[PID:{pid}] Tables created successfully')
    except Exception as e:
        logger.error(f'[PID:{pid}] Create tables error: {type(e).__name__}: {e}')
        raise

    try:
        logger.info(f'[PID:{pid}] Creating default user...')
        create_default_user()
        logger.info(f'[PID:{pid}] Default user created successfully')
        print(f'[PID:{pid}] Default user created successfully')
    except Exception as e:
        logger.error(f'[PID:{pid}] Create default user error: {type(e).__name__}: {e}')
        raise

    try:
        logger.info(f'[PID:{pid}] Creating database triggers...')
        create_all_triggers()
        logger.info(f'[PID:{pid}] Triggers created successfully')
        print(f'[PID:{pid}] Triggers created successfully')
    except Exception as e:
        logger.error(f'[PID:{pid}] Create triggers error: {type(e).__name__}: {e}')
        raise

    logger.info(f'[PID:{pid}] Database initialization completed')
