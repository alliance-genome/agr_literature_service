"""Unit tests for the pure Author value object and helpers in
``agr_literature_service.lit_processing.data_ingest.utils.author``.
"""
from sqlalchemy import text

from agr_literature_service.api.crud.utils.author_review import (
    AUTHOR_REVIEW_BLOCKED,
    AUTHOR_REVIEW_COMPLETE,
    AUTHOR_REVIEW_IN_PROGRESS,
    AUTHOR_REVIEW_NEEDED,
    AUTHOR_REVIEW_STATES,
)
from agr_literature_service.api.models import (
    AuthorModel,
    ModCorpusAssociationModel,
    ModModel,
    PersonModel,
    ReferenceModel,
    UserModel,
    WorkflowTagModel,
)
from agr_literature_service.api.user import (
    _set_current_user,
    get_current_agent_id,
    get_global_user_id,
    set_global_user_id,
)
from agr_literature_service.lit_processing.data_ingest.utils.author import (
    Author,
    add_order_to_list_of_authors,
    authors_have_same_name,
    authors_lists_are_equal,
    authors_lists_match_for_review,
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

    def test_review_match_ignores_other_fields(self):
        # SCRUM-6448: only name, order and ORCID count for an author review
        a = [_author(name="Jane Doe", orcid="ORCID:0000-0001")]
        b = [_author(name="JANE DOE", first_name="J.", last_name="D", first_initial="X",
                     orcid="ORCID:0000-0001", affiliations=["Somewhere"], string_affiliations="Somewhere",
                     email="jane@example.org")]
        assert authors_lists_match_for_review(a, b) is True

    def test_review_mismatch_on_name(self):
        assert authors_lists_match_for_review([_author(name="Jane Doe")],
                                              [_author(name="Jane Roe")]) is False

    def test_review_mismatch_on_order(self):
        a = [_author(name="Jane Doe", order=1), _author(name="John Roe", order=2)]
        b = [_author(name="John Roe", order=1), _author(name="Jane Doe", order=2)]
        assert authors_lists_match_for_review(a, b) is False

    def test_review_mismatch_on_orcid(self):
        assert authors_lists_match_for_review([_author(orcid="ORCID:0000-0001")],
                                              [_author(orcid=None)]) is False

    def test_review_mismatch_on_author_count(self):
        assert authors_lists_match_for_review([_author()],
                                              [_author(), _author(name="John Roe", order=2)]) is False

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


def _make_curator_user(db, person_curie, display_name="Curator"):  # noqa
    """Create a person and its person-linked (curator) agent; return the agent's
    label, the person curie, which is what created_by / updated_by take.

    The test-cleanup fixture deletes person rows but never agents (the default
    user is preserved): it turns a leftover curator agent into an automation
    agent first, so a re-run creates a fresh person and agent here.
    """
    person = PersonModel(display_name=display_name, curie=person_curie)
    db.add(person)
    db.commit()
    db.refresh(person)
    db.add(UserModel(person_id=person.person_id))
    db.commit()
    return person_curie


def _make_automation_user(db, label):  # noqa
    """Get-or-create an automation agent (person_id NULL); return its label."""
    if db.query(UserModel).filter_by(automation_username=label).one_or_none() is None:
        db.add(UserModel(automation_username=label))
        db.commit()
    return label


class TestReferenceTouchedByCurator:
    """Unit tests for the curator-touch skip-gate helper (DB-backed).

    A curator touch is detected by joining the author's created_by/updated_by
    (FK users.id) back to users and finding person_id IS NOT NULL.
    """

    def test_true_when_author_touched_by_curator(self, db):  # noqa
        ref = _make_reference(db, "AGRKB:CT-TEST-1")
        curator = _make_curator_user(db, "AGR:CTLINK-1")
        db.add(AuthorModel(reference_id=ref.reference_id, author_order=1,
                           name="Alpha A", created_by=curator, updated_by=curator))
        db.commit()
        assert _reference_touched_by_curator(db, ref.reference_id) is True

    def test_false_when_only_automation_touch(self, db):  # noqa
        ref = _make_reference(db, "AGRKB:CT-TEST-2")
        automation = _make_automation_user(db, "automation-user-1")
        db.add(AuthorModel(reference_id=ref.reference_id, author_order=1,
                           name="Alpha A", created_by=automation, updated_by=automation))
        db.add(AuthorModel(reference_id=ref.reference_id, author_order=2,
                           name="Beta B", created_by=automation, updated_by=automation))
        db.commit()
        assert _reference_touched_by_curator(db, ref.reference_id) is False


class TestUpdateAuthorsSync:
    """DB-level tests for the ingest author sync in update_authors()."""

    def test_sync_skips_curator_touched_reference(self, db):  # noqa
        ref = _make_reference(db, "AGRKB:CT-TEST-3")
        curator = _make_curator_user(db, "AGR:CTLINK-2")
        # an existing order-based author row that a curator has touched
        db.add(AuthorModel(reference_id=ref.reference_id, author_order=1,
                           name="Existing One", first_name="Existing",
                           last_name="One", first_initial="E",
                           created_by=curator, updated_by=curator))
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
        assert rows[0].created_by == curator

    def test_sync_drop_and_reload_automation_touched_reference(self, db):  # noqa
        ref = _make_reference(db, "AGRKB:CT-TEST-4")
        automation = _make_automation_user(db, "automation-user-2")
        for order, (name, fn, ln, fi) in enumerate(
            [("Aa A", "Aa", "A", "A"), ("Bb B", "Bb", "B", "B"),
             ("Cc C", "Cc", "C", "C")], start=1
        ):
            db.add(AuthorModel(reference_id=ref.reference_id, author_order=order,
                               name=name, first_name=fn, last_name=ln, first_initial=fi,
                               created_by=automation, updated_by=automation))
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


def _json_authors(*names):
    return [{"name": name, "firstname": name.split()[0], "lastname": name.split()[1],
             "firstinit": name[0], "authorRank": rank} for rank, name in enumerate(names, start=1)]


def _db_authors(db, reference_id):  # noqa
    return [{"name": row.name, "first_name": row.first_name, "last_name": row.last_name,
             "first_initial": row.first_initial, "author_order": row.author_order}
            for row in db.query(AuthorModel).filter_by(reference_id=reference_id)
            .order_by(AuthorModel.author_order)]


def _author_history(db, reference_id):  # noqa
    """(author_id, operation_type) of every author_version row of the reference."""
    return sorted(db.execute(text(
        "SELECT id, operation_type FROM author_version WHERE reference_id = :r"),
        {"r": reference_id}).all())


class TestUpdateAuthorsHistory:
    """SCRUM-6753: a PubMed/DQM author reload replaces the reference's automated
    author history instead of adding to it; history written by a person stays."""

    def setup_method(self):
        self._user = (get_global_user_id(), get_current_agent_id())

    def teardown_method(self):
        _set_current_user(*self._user)

    def _reload(self, db, ref, *names):  # noqa
        update_authors(db, ref.reference_id, _db_authors(db, ref.reference_id),
                       _json_authors(*names), "x", {}, None, None, None, None)
        db.commit()

    def test_reload_keeps_only_the_new_authors_history(self, db):  # noqa
        ref = _make_reference(db, "AGRKB:HIST-TEST-1")
        set_global_user_id(db, "pubmed_history_test")
        self._reload(db, ref, "Aa A", "Bb B", "Cc C")
        self._reload(db, ref, "Xx X", "Yy Y")
        self._reload(db, ref, "Xx X", "Zz Z")

        author_ids = [row.author_id for row in db.query(AuthorModel).filter_by(reference_id=ref.reference_id)]
        # one INSERT row per current author, none left from the earlier loads
        assert _author_history(db, ref.reference_id) == sorted((author_id, 0) for author_id in author_ids)

    def test_reload_keeps_history_written_by_a_curator(self, db):  # noqa
        ref = _make_reference(db, "AGRKB:HIST-TEST-2")
        set_global_user_id(db, "pubmed_history_test")
        self._reload(db, ref, "Aa A", "Bb B")
        removed = db.query(AuthorModel).filter_by(reference_id=ref.reference_id, author_order=2).one()
        removed_id = removed.author_id
        # a curator deletes the second author; the first is still automation-owned,
        # so the next PubMed update reloads the reference
        curator = _make_curator_user(db, "AGR:HISTLINK-2")
        _set_current_user(curator, db.query(UserModel).join(
            PersonModel, PersonModel.person_id == UserModel.person_id).filter(
            PersonModel.curie == curator).one().user_id)
        db.delete(removed)
        db.commit()

        set_global_user_id(db, "pubmed_history_test")
        self._reload(db, ref, "Aa A", "Cc C")

        author_ids = [row.author_id for row in db.query(AuthorModel).filter_by(reference_id=ref.reference_id)]
        assert _author_history(db, ref.reference_id) == sorted(
            [(author_id, 0) for author_id in author_ids] + [(removed_id, 2)])


def _get_or_create_mod(db, abbreviation):  # noqa
    mod = db.query(ModModel).filter_by(abbreviation=abbreviation).one_or_none()
    if mod is None:
        mod = ModModel(abbreviation=abbreviation, short_name=abbreviation, full_name=abbreviation)
        db.add(mod)
        db.commit()
        db.refresh(mod)
    return mod


class TestUpdateAuthorsAuthorReview:
    """SCRUM-6448: the PubMed update (flag_author_review=True) flags curator-edited
    author lists for review with the WB author review workflow tags."""

    DB_AUTHORS = [{"name": "Existing One", "first_name": "Existing", "last_name": "One",
                   "first_initial": "E", "author_order": 1}]
    MATCHING_JSON = [{"name": "Existing One", "firstname": "E.", "lastname": "One",
                      "firstinit": "E", "authorRank": 1, "affiliations": ["Elsewhere"]}]
    DIFFERENT_JSON = [{"name": "New Person", "firstname": "New", "lastname": "Person",
                       "firstinit": "N", "authorRank": 1}]

    def _curated_reference(self, db, n, mod_abbreviation="WB", corpus=True, tag=None):  # noqa
        ref = _make_reference(db, f"AGRKB:AR-TEST-{n}")
        curator = _make_curator_user(db, f"AGR:ARLINK-{n}")
        mod = _get_or_create_mod(db, mod_abbreviation)
        db.add(ModCorpusAssociationModel(reference_id=ref.reference_id, mod_id=mod.mod_id,
                                         corpus=corpus, mod_corpus_sort_source="manual_creation"))
        db.add(AuthorModel(reference_id=ref.reference_id, author_order=1,
                           name="Existing One", first_name="Existing", last_name="One",
                           first_initial="E", created_by=curator, updated_by=curator))
        if tag:
            db.add(WorkflowTagModel(reference_id=ref.reference_id, mod_id=mod.mod_id,
                                    workflow_tag_id=tag))
        db.commit()
        return ref, mod

    def _review_tags(self, db, ref, mod):  # noqa
        return [row.workflow_tag_id for row in db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == ref.reference_id,
            WorkflowTagModel.mod_id == mod.mod_id,
            WorkflowTagModel.workflow_tag_id.in_(AUTHOR_REVIEW_STATES)).all()]

    def _run(self, db, ref, author_list_in_json, flag_author_review=True, pubmed_record_changed=False):  # noqa
        result = update_authors(db, ref.reference_id, self.DB_AUTHORS, author_list_in_json,
                                "x", {}, None, None, None, None,
                                flag_author_review=flag_author_review,
                                pubmed_record_changed=pubmed_record_changed)
        db.commit()
        return result

    def test_difference_sets_needed_and_keeps_authors(self, db):  # noqa
        ref, mod = self._curated_reference(db, 1)
        assert self._run(db, ref, self.DIFFERENT_JSON) == []
        assert self._review_tags(db, ref, mod) == [AUTHOR_REVIEW_NEEDED]
        rows = db.query(AuthorModel).filter_by(reference_id=ref.reference_id).all()
        assert [row.name for row in rows] == ["Existing One"]

    def test_difference_reopens_complete_when_pubmed_changed(self, db):  # noqa
        ref, mod = self._curated_reference(db, 2, tag=AUTHOR_REVIEW_COMPLETE)
        self._run(db, ref, self.DIFFERENT_JSON, pubmed_record_changed=True)
        assert self._review_tags(db, ref, mod) == [AUTHOR_REVIEW_NEEDED]

    def test_difference_keeps_complete_when_pubmed_unchanged(self, db):  # noqa
        # a curator reviewed the paper and kept ABC's authors on purpose: later runs
        # over the same PubMed record must not reopen the review
        ref, mod = self._curated_reference(db, 10, tag=AUTHOR_REVIEW_COMPLETE)
        for _ in range(2):
            self._run(db, ref, self.DIFFERENT_JSON, pubmed_record_changed=False)
            assert self._review_tags(db, ref, mod) == [AUTHOR_REVIEW_COMPLETE]

    def test_difference_sets_needed_without_tag_even_if_pubmed_unchanged(self, db):  # noqa
        ref, mod = self._curated_reference(db, 11)
        self._run(db, ref, self.DIFFERENT_JSON, pubmed_record_changed=False)
        assert self._review_tags(db, ref, mod) == [AUTHOR_REVIEW_NEEDED]

    def test_difference_leaves_in_progress_and_blocked(self, db):  # noqa
        for n, tag in ((3, AUTHOR_REVIEW_IN_PROGRESS), (4, AUTHOR_REVIEW_BLOCKED)):
            ref, mod = self._curated_reference(db, n, tag=tag)
            self._run(db, ref, self.DIFFERENT_JSON)
            assert self._review_tags(db, ref, mod) == [tag]

    def test_match_completes_needed(self, db):  # noqa
        ref, mod = self._curated_reference(db, 5, tag=AUTHOR_REVIEW_NEEDED)
        self._run(db, ref, self.MATCHING_JSON)
        assert self._review_tags(db, ref, mod) == [AUTHOR_REVIEW_COMPLETE]

    def test_match_without_tag_adds_nothing(self, db):  # noqa
        ref, mod = self._curated_reference(db, 6)
        self._run(db, ref, self.MATCHING_JSON)
        assert self._review_tags(db, ref, mod) == []

    def test_not_flagged_without_opt_in(self, db):  # noqa
        # the DQM loads call update_authors without flag_author_review
        ref, mod = self._curated_reference(db, 7)
        self._run(db, ref, self.DIFFERENT_JSON, flag_author_review=False)
        assert self._review_tags(db, ref, mod) == []

    def test_not_flagged_outside_wb_corpus(self, db):  # noqa
        ref, wb = self._curated_reference(db, 8, corpus=False)
        self._run(db, ref, self.DIFFERENT_JSON)
        assert self._review_tags(db, ref, wb) == []
        ref, sgd = self._curated_reference(db, 9, mod_abbreviation="SGD")
        self._run(db, ref, self.DIFFERENT_JSON)
        assert self._review_tags(db, ref, sgd) == []
