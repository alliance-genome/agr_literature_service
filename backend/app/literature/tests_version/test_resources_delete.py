import pytest
from literature.crud.reference_crud import create, show, patch, destroy, show_changesets
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import desc
from sqlalchemy.orm import Session

from literature.models import (
    Base, ReferenceModel
)
from literature.database.config import SQLALCHEMY_DATABASE_URL
from sqlalchemy.orm import sessionmaker
from fastapi import HTTPException
from sqlalchemy_continuum import make_versioned
from sqlalchemy_continuum import Operation
from sqlalchemy_continuum import transaction_class
from sqlalchemy.orm import create_session, configure_mappers
from literature.schemas import ReferenceSchemaPost
from fastapi import HTTPException
from fastapi import status

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()
session = create_session(bind=engine, autocommit=False, autoflush=True) # type: Session
#session = sessionmaker(bind=engine, autoflush=True)
# Add tables/schema if not already there.
#Base.metadata.create_all(engine)sql
History = ReferenceModel.__versioned__['class']
Histroy = transaction_class(ReferenceModel)

history_obj = session.query(History).filter(History.operation_type == Operation.DELETE).filter(History.curie=='AGR:AGR-Reference-0000000010').order_by(desc(History.transaction_id)).first()

history_obj.previous.reify()
exit

reference3 = ReferenceSchemaPost(title="Bob1", category="thesis", abstract="3")
curie3 = create(session, reference3)
#session.commit

#reference1 = ReferenceModel(title="Bob", category="thesis", abstract="3")
#session.add(reference1)
#session.commit
print ('reference name now:', reference3.title)
title1='Bob1 TEST1'
patch(session, curie3, {'title': title1})
print ('reference name now:', reference3.title)
#check the transaction/version of reference
title2='Bob1 TEST2'
patch(session, curie3, {'title': title2})
print ('reference name now:', reference3.title)
reference = session.query(ReferenceModel).filter(ReferenceModel.curie==curie3).first()

if not reference:
    print ('unable to find record with this title:', title1)
else:
    print(reference.versions[0].operation_type)
    for version in reference.versions[::-1]:
        if version.operation_type==1:
            version.revert()
            session.commit
            print ('reverse commit here', version.curie)
            break


def test_destroy_note():
    destroy(db, 1)

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, 1)

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, 1)

