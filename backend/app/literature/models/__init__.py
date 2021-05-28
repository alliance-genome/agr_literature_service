from sqlalchemy.orm import configure_mappers

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import create_session, configure_mappers

from literature.database.main import Base

from literature.models.reference import Reference
from literature.models.reference_tag import ReferenceTag
from literature.models.mesh_detail import MeshDetail
from literature.models.resource import Resource
from literature.models.author import Author
from literature.models.editor import Editor
from literature.models.person import Person
from literature.models.user import User
from literature.models.cross_reference import CrossReference
from literature.models.mod_reference_type import ModReferenceType
from literature.models.s3file import File

from literature.models.resource_descriptor import ResourceDescriptor
from literature.models.resource_descriptor import ResourceDescriptorPage

from literature.models.person_reference_link import PersonReferenceLink
from literature.models.person_orcid_cross_reference_link import PersonOrcidCrossReferenceLink

configure_mappers()
