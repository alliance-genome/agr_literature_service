import pytest
from sqlalchemy.exc import IntegrityError

from agr_literature_service.api.models import AuthorModel, PersonModel
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa
from .test_reference import test_reference  # noqa


def _person(db):  # noqa
    p = PersonModel(display_name="Test Person", curie="AGR:AP-TEST-1")
    db.add(p)
    db.commit()
    db.refresh(p)
    return p


class TestAuthorPersonConstraints:
    def test_neither_person_nor_order_rejected(self, db, test_reference):  # noqa
        ref_id = test_reference.related_ref_id
        db.add(AuthorModel(reference_id=ref_id))  # no person_id, no author_order
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()

    def test_person_only_with_author_metadata_rejected(self, db, test_reference):  # noqa
        ref_id = test_reference.related_ref_id
        p = _person(db)
        db.add(AuthorModel(reference_id=ref_id, person_id=p.person_id, name="Should Not Be Here"))
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()

    def test_person_only_link_only_allowed(self, db, test_reference):  # noqa
        ref_id = test_reference.related_ref_id
        p = _person(db)
        db.add(AuthorModel(reference_id=ref_id, person_id=p.person_id))
        db.commit()  # no error

    def test_duplicate_person_on_reference_rejected(self, db, test_reference):  # noqa
        ref_id = test_reference.related_ref_id
        p = _person(db)
        db.add(AuthorModel(reference_id=ref_id, person_id=p.person_id))
        db.commit()
        db.add(AuthorModel(reference_id=ref_id, person_id=p.person_id, author_order=3))
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()

    def test_duplicate_author_order_on_reference_rejected(self, db, test_reference):  # noqa
        ref_id = test_reference.related_ref_id
        db.add(AuthorModel(reference_id=ref_id, author_order=5, name="A"))
        db.commit()
        db.add(AuthorModel(reference_id=ref_id, author_order=5, name="B"))
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()

    def test_null_reference_id_rejected(self, db, test_reference):  # noqa
        # every author row must belong to a reference: reference_id is NOT NULL
        db.add(AuthorModel(author_order=1, name="No Reference"))  # no reference_id
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()

    def test_many_pure_author_and_person_only_rows_allowed(self, db, test_reference):  # noqa
        ref_id = test_reference.related_ref_id
        db.add(AuthorModel(reference_id=ref_id, author_order=1, name="A1"))
        db.add(AuthorModel(reference_id=ref_id, author_order=2, name="A2"))
        p1 = PersonModel(display_name="P1", curie="AGR:AP-TEST-2")
        p2 = PersonModel(display_name="P2", curie="AGR:AP-TEST-3")
        db.add_all([p1, p2])
        db.commit()
        db.add(AuthorModel(reference_id=ref_id, person_id=p1.person_id))
        db.add(AuthorModel(reference_id=ref_id, person_id=p2.person_id))
        db.commit()  # no error
