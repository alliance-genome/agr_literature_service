from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from literature.crud.reference_crud import create, patch, show, show_notes
from literature.database.config import SQLALCHEMY_DATABASE_URL
# from literature import models
from literature.models import Base
from literature.schemas import ReferenceSchemaPost, ReferenceSchemaUpdate

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)


def test_reference_create_with_existing_items():
    full_xml = {
        "category": "research_article",
        "abstract": "The Hippo (Hpo) pathway is a conserved tumor suppressor pathway",
        "authors": [
            {
                "orcid": 'ORCID:1234-1234-1234-123X'
            },
            {
                "orcid": 'ORCID:1111-2222-3333-444X'  # New
            }
        ],
        "resource": 'AGR:AGR-Resource-0000000001',
        "title": "Another title",
        "volume": "433",
        "open_access": True
    }
    assert 1 == 1
    # process the reference.
    reference = ReferenceSchemaPost(**full_xml)
    res = create(db, reference)
    assert res == 'AGR:AGR-Reference-0000000005'


def test_show():
    # so we have added several things since created via other cruds so show that
    res = show(db, 'AGR:AGR-Reference-0000000001')
    # assert res == "bob"
    assert res['notes'][0]['note'] == "Note for ref test"


def test_show_notes():
    res = show_notes(db, 'AGR:AGR-Reference-0000000001')
    assert res[0]['name'] == "Name for ref test"


def test_patch():
    xml = {'merged_into_reference_curie': "AGR:AGR-Reference-0000000003",
           'resource': "AGR:AGR-Resource-0000000003"}
    schema = ReferenceSchemaUpdate(**xml)
    res = patch(db, 'AGR:AGR-Reference-0000000001', schema)
    assert res == {'message': 'updated'}

    # fetch the new record.
    res = show(db, 'AGR:AGR-Reference-0000000001')
    # assert res == "bob"
    assert res['merged_into_id'] == 3
    assert res['resource_curie'] == 'AGR:AGR-Resource-0000000003'
