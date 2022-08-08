from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_continuum import Operation

from agr_literature_service.api.crud.reference_crud import create, show, merge_references, patch
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.schemas import (
    ReferenceSchemaPost, ReferenceSchemaUpdate, ResourceSchemaPost)
from agr_literature_service.api.models import ReferenceModel
from agr_literature_service.api.crud.user_crud import create as user_create
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.api.crud.resource_crud import create as resource_create

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)

ress = []


def test_initialise():
    global refs

    # add User "013_Bob"
    user = user_create(db, "013_Bob")
    # By adding set_global_user_id here we do not need to pass the
    # created_by and updated_by dict elements to the schema validators.
    set_global_user_id(db, user.id)

    # Add resources.
    for title in ['Bob 013 1', 'Bob 013 2', 'Bob 013 3']:
        Resource = ResourceSchemaPost(title=title, abstract="3", open_access=True)
        ress.append(resource_create(db, Resource))


def test_reference_merging():
    global ress
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
        "resource": ress[0],
        "title": "Another title",
        "volume": "013a",
        "open_access": True
    }
    # process the references
    ref1 = ReferenceSchemaPost(**full_xml)
    print(ref1)
    ref_obj = create(db, ref1)
    # ref_obj = router_create(ref1)

    full_xml['volume'] = '013b'
    full_xml['abstract'] = "013 - abs B"
    ref2 = ReferenceSchemaPost(**full_xml)
    ref2_obj = create(db, ref2)

    full_xml['volume'] = '013c'
    full_xml['abstract'] = "013 - abs C"
    ref3 = ReferenceSchemaPost(**full_xml)
    ref3_obj = create(db, ref3)

    # update ref_obj with a different category
    # This is just to test the transactions and versions
    xml = {'category': "other"}
    schema = ReferenceSchemaUpdate(**xml)
    res = patch(db, ref_obj, schema)
    assert res == {'message': 'updated'}
    schema = ReferenceSchemaUpdate(**xml)
    res = patch(db, ref3_obj, schema)
    assert res == {'message': 'updated'}

    # fetch the new record.
    res = show(db, ref_obj)

    assert res['category'] == 'other'

    # merge 1 into 2
    merge_references(db, ref_obj, ref2_obj)

    # merge 2 into 3
    merge_references(db, ref2_obj, ref3_obj)

    # So now if we look up ref_obj we should get ref3_obj
    # and if we lookup ref2_obj we should get ref3_obj
    res = show(db, ref_obj)
    assert res['curie'] == ref3_obj
    res = show(db, ref2_obj)
    assert res['curie'] == ref3_obj

    ##########################################################################
    # The following are really examples of continuum and not testing the code
    ##########################################################################

    #####################################
    # 1) Manually examine the _version table
    #####################################
    sql = """SELECT transaction_id, operation_type, end_transaction_id, category, category_mod
             FROM reference_version
               WHERE curie = '{}'
               ORDER BY transaction_id
        """.format(ref_obj)

    with engine.connect() as con:
        rs = con.execute(sql)
        # (33, 1, 36, 'Research_Article', True)
        # (36, 1, 37, 'Other', True)
        # (37, 2, None, 'Other', True)

        print("insert: {}, update: {}, delete: {}".format(Operation.INSERT, Operation.UPDATE, Operation.DELETE))
        results = []
        for row in rs:
            print(row)
            results.append(row)

        # last transaction check.
        assert results[0][2] == results[1][0]

        # final Transaction_id is none
        assert results[2][2] is None

        # check category changed
        assert results[0][3] != results[1][3]

    ######################
    # 2) version traversal
    ######################
    ref = db.query(ReferenceModel).filter(ReferenceModel.curie == ref3_obj).first()
    first_ver = ref.versions[0]
    # lower case now???
    assert first_ver.category == 'research_article'

    sec_ver = first_ver.next
    assert sec_ver.category == 'other'

    ########################################
    # 3) changesets, see test_001_reference.
    ########################################
