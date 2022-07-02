from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.reference_tag_crud import patch
from agr_literature_service.api.crud.mod_crud import create as mod_create
from agr_literature_service.api.crud.reference_crud import show, create as ref_create, patch as ref_patch
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import ModModel, ReferenceModel
from agr_literature_service.api.models.reference_tag_model import ReferenceTagModel
from agr_literature_service.api.schemas import ReferenceSchemaPost
from agr_literature_service.api.schemas import ReferenceSchemaPost, ReferenceSchemaUpdate

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)

test_source = 'WB Primary ID'
test_source2 = 'Assigned_for_review'
db.execute('delete from mod')
db.execute('delete from cross_reference')
db.execute('delete from author')
db.execute('delete from editor')
db.execute('delete from "reference"')
db.execute('delete from resource')

ref_curie = "AGR:AGR-Reference-0000000001"


def get_ids():

    mod = db.query(ModModel).filter_by(abbreviation="AtDB").one()
    mod_id = mod.mod_id
    ref = db.query(ReferenceModel).filter_by(curie=ref_curie).one()
    reference_id = ref.reference_id
    return (mod_id, reference_id)


def test_patch_ref_tag():
    db.execute('delete from mod')
    db.execute('delete from "reference"')
    mod = db.query(ModModel).filter(ModModel.abbreviation == 'AtDB').first()
    print("MOD")
    print(mod)
    mod_data = {
        "abbreviation": 'AtDB',
        "short_name": "AtDB",
        "full_name": "Test genome database"
    }
    res = mod_create(db, mod_data)
    assert res

    ref_tag = db.query(ReferenceTagModel).first()
    print(ref_tag)

    reference = ReferenceSchemaPost(title="Bob", category="thesis", abstract="3", language="MadeUp")
    res = ref_create(db, reference)
    assert res

    data = {
        "mod_abbreviation": "AtDB",
        "reference_curie": "AGR:AGR-Reference-0000000001",
        "tag_type": test_source,
        "value": 'Test1'
    }
    res = patch(db, data)
    assert res
    res = show(db, ref_curie)
    print(res)
    assert res['tags'][0]['tag_type'] == test_source
    assert res['tags'][0]['value'] == 'Test1'

    # Change the value BUT go via the reference
    data['value'] = 'new_val'
    ref_json = {
        'reference_curie': res['curie'],
        'tags': data
    }
    update_schema = ReferenceSchemaUpdate(title="new title", category="book", language="New", tags=[data])
    ref_patch(db, ref_curie, update_schema)
    res = show(db, ref_curie)
    print(res)
    assert res['tags'][0]['value'] == 'new_val'

# def test_destroy_mca():
#
#    (mod_id, reference_id) = get_ids()
#    mca = db.query(ModCorpusAssociationModel).filter_by(reference_id=reference_id, mod_id=mod_id).one()
#    destroy(db, mca.mod_corpus_association_id)

    # it should now give an error on lookup.
#    with pytest.raises(HTTPException):
#        show(db, mca.mod_corpus_association_id)

#    # deleting it again should give an error as the lookup will fail.
#    with pytest.raises(HTTPException):
#        destroy(db, mca.mod_corpus_association_id)
