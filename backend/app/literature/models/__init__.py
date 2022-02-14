from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import configure_mappers, create_session

from literature.database.main import Base
from literature.models.author_model import AuthorModel
from literature.models.cross_reference_model import CrossReferenceModel
from literature.models.editor_model import EditorModel
from literature.models.file_model import FileModel
from literature.models.mesh_detail_model import MeshDetailModel
from literature.models.mod_reference_type_model import ModReferenceTypeModel
from literature.models.note_model import NoteModel
from literature.models.person_model import PersonModel
from literature.models.person_orcid_cross_reference_link_model import \
    PersonOrcidCrossReferenceLinkModel
from literature.models.person_reference_link_model import \
    PersonReferenceLinkModel
from literature.models.reference_automated_term_tag_model import \
    ReferenceAutomatedTermTagModel
from literature.models.reference_comment_and_correction_model import \
    ReferenceCommentAndCorrectionModel
from literature.models.reference_manual_term_tag_model import \
    ReferenceManualTermTagModel
from literature.models.reference_model import ReferenceModel
from literature.models.reference_tag_model import ReferenceTagModel
from literature.models.resource_descriptor_models import (
    ResourceDescriptorModel, ResourceDescriptorPageModel)
from literature.models.resource_model import ResourceModel
from literature.models.user_model import UserModel

configure_mappers()
