"""Unit tests for the pure Author value object and helpers in
``agr_literature_service.lit_processing.data_ingest.utils.author``.
"""
from agr_literature_service.api.models import (
    AuthorModel,
    PersonModel,
    ReferenceModel,
    UserModel,
)
from agr_literature_service.lit_processing.data_ingest.utils.author import (
    Author,
    add_order_to_list_of_authors,
    authors_have_same_name,
    authors_lists_are_equal,
)
from agr_literature_service.lit_processing.data_ingest.utils.db_write_utils import (
    _reference_touched_by_curator,
    update_authors,
)
from ....fixtures import db  # noqa


def _author(**overrides):
    base = dict(
        name="Jane Doe", first_name="Jane", last_name="Doe", first_initial="J",
        order=1, orcid=None, affiliations=[], string_affiliations="",
    )
    base.update(overrides)
    return Author(**base)


class TestNormalizeField:
    def test_string_is_stripped(self):
        assert Author.normalize_field("  Doe  ") == "Doe"

    def test_string_lowercased(self):
        assert Author.normalize_field("  DOE ", set_lowercase=True) == "doe"

    def test_list_joined_with_pipe(self):
        assert Author.normalize_field([" a ", "B "]) == "a|B"

    def test_list_lowercased(self):
        assert Author.normalize_field(["A", "B"], set_lowercase=True) == "a|b"

    def test_empty_list(self):
        assert Author.normalize_field([]) == ""

    def test_int_returned_as_is(self):
        assert Author.normalize_field(5) == 5

    def test_none_returned_as_is(self):
        assert Author.normalize_field(None) is None


class TestNormalization:
    def test_get_normalized_author_lowercases(self):
        a = _author(name="  JANE Doe ", first_name=" JANE ")
        norm = a.get_normalized_author(set_lowercase=True)
        assert norm.name == "jane doe"
        assert norm.first_name == "jane"
        # non-normalized fields carried through
        assert norm.order == 1

    def test_unique_key_based_on_names(self):
        a = _author(name="Jane Doe", first_name="Jane", last_name="Doe", first_initial="J")
        assert a.get_unique_key_based_on_names() == ("doe", "jane", "j", "jane doe")

    def test_normalized_lowercase_string(self):
        a = _author(name="Jane Doe", orcid="ORCID:1", order=2)
        s = a.get_normalized_lowercase_author_string()
        assert s.startswith("jane doe | jane | doe | j |")
        assert "orcid:1" not in s  # orcid not lowercased in field, kept as-is
        assert "| 2 |" in s


class TestUnaccented:
    def test_removes_accents(self):
        assert Author.get_unaccented_string("Müller") == "Muller"
        assert Author.get_unaccented_string("Éric") == "Eric"

    def test_none_returns_none(self):
        assert Author.get_unaccented_string(None) is None


class TestFixOrcidFormat:
    def test_adds_prefix(self):
        a = _author(orcid="0000-0001")
        a.fix_orcid_format()
        assert a.orcid == "ORCID:0000-0001"

    def test_uppercases_existing_prefix(self):
        a = _author(orcid="orcid:0000-0001")
        a.fix_orcid_format()
        assert a.orcid == "ORCID:0000-0001"

    def test_none_when_missing(self):
        a = _author(orcid=None)
        a.fix_orcid_format()
        assert a.orcid is None


class TestLoadFromJsonDict:
    def test_lowercase_key_variant(self):
        a = Author.load_from_json_dict({
            "name": "Jane Doe", "firstname": "Jane", "lastname": "Doe",
            "firstinit": "J", "authorRank": 3, "orcid": "0000-1",
            "affiliations": ["MIT"],
        })
        assert a.first_name == "Jane" and a.last_name == "Doe"
        assert a.order == 3
        assert a.orcid == "ORCID:0000-1"

    def test_camelcase_key_variant_and_defaults(self):
        a = Author.load_from_json_dict({
            "name": "John Roe", "firstName": "John", "lastName": "Roe",
            "firstInit": "J",
        })
        assert a.first_name == "John"
        assert a.order is None
        assert a.affiliations == []
        assert a.orcid is None


class TestLoadFromDbDict:
    def test_author_order_key(self):
        a = Author.load_from_db_dict({
            "name": "Jane Doe", "first_name": "Jane", "last_name": "Doe",
            "first_initial": "J", "author_order": 4, "orcid": "0000-2",
        })
        assert a.order == 4
        assert a.orcid == "ORCID:0000-2"

    def test_order_key_fallback(self):
        a = Author.load_from_db_dict({"name": "Jane Doe", "order": 9})
        assert a.order == 9
        assert a.orcid is None


class TestLoadLists:
    def test_json_list_none_and_empty(self):
        assert Author.load_list_of_authors_from_json_dict_list(None) == []
        assert Author.load_list_of_authors_from_json_dict_list([]) == []

    def test_json_list_assigns_order(self):
        authors = Author.load_list_of_authors_from_json_dict_list([
            {"name": "A A"}, {"name": "B B"},
        ])
        assert [a.order for a in authors] == [1, 2]

    def test_db_list_none_and_empty(self):
        assert Author.load_list_of_authors_from_db_dict_list(None) == []
        assert Author.load_list_of_authors_from_db_dict_list([]) == []

    def test_db_list_loaded(self):
        authors = Author.load_list_of_authors_from_db_dict_list([
            {"name": "A A", "author_order": 1},
        ])
        assert len(authors) == 1 and authors[0].name == "A A"


class TestModuleHelpers:
    def test_add_order_when_missing(self):
        authors = [_author(order=None), _author(order=None)]
        add_order_to_list_of_authors(authors)
        assert [a.order for a in authors] == [1, 2]

    def test_add_order_noop_when_present(self):
        authors = [_author(order=5), _author(order=6)]
        add_order_to_list_of_authors(authors)
        assert [a.order for a in authors] == [5, 6]

    def test_add_order_empty_list(self):
        add_order_to_list_of_authors([])  # should not raise

    def test_lists_equal(self):
        a = [_author(name="Jane Doe")]
        b = [_author(name="JANE DOE")]  # lowercased in comparison
        assert authors_lists_are_equal(a, b) is True

    def test_lists_not_equal(self):
        assert authors_lists_are_equal([_author(name="Jane Doe")],
                                       [_author(name="John Roe")]) is False

    def test_same_name_matches_long_token(self):
        assert authors_have_same_name(_author(name="Jane Smith"),
                                      _author(name="Robert Smith")) is True

    def test_same_name_no_match_short_tokens(self):
        assert authors_have_same_name(_author(name="Al Bo"),
                                      _author(name="Cy Bo")) is False


def _make_reference(db, curie):  # noqa
    ref = ReferenceModel(curie=curie, category="research_article")
    db.add(ref)
    db.commit()
    db.refresh(ref)
    return ref


def _make_curator_user(db, user_id, person_curie, display_name="Curator"):  # noqa
    """Get-or-create a person-linked (curator) users row: person_id set, no
    automation_username.

    The test-cleanup fixture deletes person rows but never deletes users rows (the
    default user is preserved), and it converts any leftover curator user to an
    automation user to keep the person/automation XOR valid. So on a re-run the
    users.id survives as an automation row: reuse it and re-link it to a fresh person.
    """
    person = PersonModel(display_name=display_name, curie=person_curie)
    db.add(person)
    db.commit()
    db.refresh(person)
    user = db.query(UserModel).filter_by(id=user_id).one_or_none()
    if user is None:
        user = UserModel(id=user_id, person_id=person.person_id)
    else:
        user.automation_username = None
        user.person_id = person.person_id
    db.add(user)
    db.commit()
    return user


def _make_automation_user(db, user_id, script_name="scriptNm"):  # noqa
    """Get-or-create an automation users row: automation_username set, person_id NULL."""
    user = db.query(UserModel).filter_by(id=user_id).one_or_none()
    if user is None:
        user = UserModel(id=user_id, automation_username=script_name)
        db.add(user)
        db.commit()
    return user


class TestReferenceTouchedByCurator:
    """Unit tests for the curator-touch skip-gate helper (DB-backed).

    A curator touch is detected by joining the author's created_by/updated_by
    (FK users.id) back to users and finding person_id IS NOT NULL.
    """

    def test_true_when_author_touched_by_curator(self, db):  # noqa
        ref = _make_reference(db, "AGRKB:CT-TEST-1")
        curator = _make_curator_user(db, "curator-user-1", "AGR:CTLINK-1")
        db.add(AuthorModel(reference_id=ref.reference_id, author_order=1,
                           name="Alpha A", created_by=curator.id, updated_by=curator.id))
        db.commit()
        assert _reference_touched_by_curator(db, ref.reference_id) is True

    def test_false_when_only_automation_touch(self, db):  # noqa
        ref = _make_reference(db, "AGRKB:CT-TEST-2")
        automation = _make_automation_user(db, "automation-user-1")
        db.add(AuthorModel(reference_id=ref.reference_id, author_order=1,
                           name="Alpha A", created_by=automation.id, updated_by=automation.id))
        db.add(AuthorModel(reference_id=ref.reference_id, author_order=2,
                           name="Beta B", created_by=automation.id, updated_by=automation.id))
        db.commit()
        assert _reference_touched_by_curator(db, ref.reference_id) is False


class TestUpdateAuthorsSync:
    """DB-level tests for the ingest author sync in update_authors()."""

    def test_sync_skips_curator_touched_reference(self, db):  # noqa
        ref = _make_reference(db, "AGRKB:CT-TEST-3")
        curator = _make_curator_user(db, "curator-user-2", "AGR:CTLINK-2")
        # an existing order-based author row that a curator has touched
        db.add(AuthorModel(reference_id=ref.reference_id, author_order=1,
                           name="Existing One", first_name="Existing",
                           last_name="One", first_initial="E",
                           created_by=curator.id, updated_by=curator.id))
        db.commit()

        author_list_in_db = [
            {"name": "Existing One", "first_name": "Existing", "last_name": "One",
             "first_initial": "E", "author_order": 1},
        ]
        author_list_in_json = [
            {"name": "New Person", "firstname": "New", "lastname": "Person",
             "firstinit": "N", "authorRank": 1},
        ]

        result = update_authors(db, ref.reference_id, author_list_in_db,
                                author_list_in_json, "x", {}, None, None, None, None)
        db.commit()

        assert result == []
        rows = db.query(AuthorModel).filter_by(reference_id=ref.reference_id).all()
        # curator-touched author untouched (no delete, no rename from the JSON)
        assert len(rows) == 1
        assert rows[0].name == "Existing One"
        assert rows[0].created_by == curator.id

    def test_sync_drop_and_reload_automation_touched_reference(self, db):  # noqa
        ref = _make_reference(db, "AGRKB:CT-TEST-4")
        automation = _make_automation_user(db, "automation-user-2")
        for order, (name, fn, ln, fi) in enumerate(
            [("Aa A", "Aa", "A", "A"), ("Bb B", "Bb", "B", "B"),
             ("Cc C", "Cc", "C", "C")], start=1
        ):
            db.add(AuthorModel(reference_id=ref.reference_id, author_order=order,
                               name=name, first_name=fn, last_name=ln, first_initial=fi,
                               created_by=automation.id, updated_by=automation.id))
        db.commit()

        author_list_in_db = [
            {"name": "Aa A", "first_name": "Aa", "last_name": "A", "first_initial": "A",
             "author_order": 1},
            {"name": "Bb B", "first_name": "Bb", "last_name": "B", "first_initial": "B",
             "author_order": 2},
            {"name": "Cc C", "first_name": "Cc", "last_name": "C", "first_initial": "C",
             "author_order": 3},
        ]
        author_list_in_json = [
            {"name": "Xx X", "firstname": "Xx", "lastname": "X", "firstinit": "X",
             "authorRank": 1},
            {"name": "Yy Y", "firstname": "Yy", "lastname": "Y", "firstinit": "Y",
             "authorRank": 2},
        ]

        result = update_authors(db, ref.reference_id, author_list_in_db,
                                author_list_in_json, "x", {}, None, None, None, None)
        db.commit()

        assert result == []
        rows = db.query(AuthorModel).filter_by(reference_id=ref.reference_id)\
            .order_by(AuthorModel.author_order).all()
        # old rows gone, fresh reload with clean sequential 1..N order
        assert [r.author_order for r in rows] == [1, 2]
        assert [r.name for r in rows] == ["Xx X", "Yy Y"]
