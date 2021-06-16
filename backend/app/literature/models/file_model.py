from datetime import datetime
import pytz

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Boolean
from sqlalchemy import DateTime
from sqlalchemy import ARRAY
from sqlalchemy import Enum

from sqlalchemy.orm import relationship

from literature.database.base import Base

from literature.schemas import FileCategories

class FileModel(Base):
    __tablename__ = 'files'
    __versioned__ = {}

    file_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    s3_filename = Column(
        String,
        unique=True,
        nullable=False
    )

    reference_id = Column(
         Integer,
         ForeignKey('references.reference_id',
                    ondelete='CASCADE'),
         index=True
    )

    reference = relationship(
        'ReferenceModel',
        back_populates="files"
    )

    extension = Column(
        String,
        nullable=True
    )

    content_type = Column(
        String,
        nullable=True
    )


    category = Column(
        Enum(FileCategories),
        nullable=True
    )

    folder = Column(
        String(),
        unique=False,
        nullable=False
    )

    md5sum = Column(
        String(),
        unique=False,
        nullable=False
    )

    size = Column(
        Integer,
        nullable=False
    )

    display_name = Column(
        String(),
        unique=False,
        nullable=True
    )

    upload_date = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow
    )

    public = Column(
        Boolean,
        nullable=False
    )

    mod_submitted = Column(
        String,
        nullable=True
    )

    mods_permitted = Column(
        ARRAY(String()),
        nullable=True
    )

    institutes_permitted = Column(
        ARRAY(String()),
        nullable=True
    )
