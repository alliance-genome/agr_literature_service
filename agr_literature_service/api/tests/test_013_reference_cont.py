from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.reference_crud import create, show, merge_references, patch
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.schemas import ReferenceSchemaPost, ReferenceSchemaUpdate

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)


def test_reference_merging():
    full_xml = {
        "category": "research_article",
        "abstract": "013 - abs A",
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
        "volume": "013a",
        "open_access": True
    }
    # process the references
    ref1 = ReferenceSchemaPost(**full_xml)
    res1 = create(db, ref1)

    full_xml['volume'] = '013b'
    full_xml['abstract'] = "013 - abs B"
    ref2 = ReferenceSchemaPost(**full_xml)
    res2 = create(db, ref2)

    full_xml['volume'] = '013c'
    full_xml['abstract'] = "013 - abs C"
    ref3 = ReferenceSchemaPost(**full_xml)
    res3 = create(db, ref3)

    # merge 1 into 2
    merge_references(db, res1, res2)

    # merge 2 into 3
    merge_references(db, res2, res3)

    # So now if we look up res1 we should get res3
    # and if we lookup res2 we should get res3
    res = show(db, res1)
    assert res['curie'] == res3
    res = show(db, res2)
    assert res['curie'] == res3


def test_patch():
    xml = {'resource': "AGR:AGR-Resource-0000000003"}
    schema = ReferenceSchemaUpdate(**xml)
    res = patch(db, 'AGR:AGR-Reference-0000000001', schema)
    assert res == {'message': 'updated'}

    # fetch the new record.
    res = show(db, 'AGR:AGR-Reference-0000000001')

    assert res['resource_curie'] == 'AGR:AGR-Resource-0000000003'