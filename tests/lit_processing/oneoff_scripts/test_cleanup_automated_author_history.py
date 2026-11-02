"""The SCRUM-6753 author history cleanup keeps each author's current automated
version row and the history written by a person, and deletes the rest of the
automated history."""
import importlib.util
from pathlib import Path
from typing import Any, cast

from agr_literature_service.api.models import AuthorModel, PersonModel, UserModel
from agr_literature_service.api.user import _set_current_user, get_current_agent_id, get_global_user_id, \
    set_global_user_id
from ..data_ingest.utils.test_author import _author_history, _make_curator_user, _make_reference
from ...fixtures import db  # noqa

# ticket-prefixed file name, so load it by path (see test_mark_article_plus_supplement_main)
_SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "agr_literature_service" / "lit_processing" / "oneoff_scripts"
    / "SCRUM-6753_cleanup_automated_author_history.py"
)
_spec = importlib.util.spec_from_file_location("cleanup_automated_author_history", _SCRIPT)
assert _spec is not None and _spec.loader is not None
cleanup_automated_author_history = importlib.util.module_from_spec(_spec)
cast(Any, _spec.loader).exec_module(cleanup_automated_author_history)

cleanup = cast(Any, cleanup_automated_author_history).cleanup


def _add_author(db, reference_id, name, order):  # noqa
    author = AuthorModel(reference_id=reference_id, name=name, last_name=name, author_order=order)
    db.add(author)
    db.commit()
    return author.author_id


def test_cleanup(db):  # noqa
    saved_user = (get_global_user_id(), get_current_agent_id())
    try:
        ref = _make_reference(db, "AGRKB:HIST-CLEANUP-1")
        rid = ref.reference_id
        # automated history written before SCRUM-6753: a load, an update of the
        # first author, then a reload that drops the second author and adds a third
        set_global_user_id(db, "pubmed_cleanup_test")
        first = _add_author(db, rid, "First", 1)
        second = _add_author(db, rid, "Second", 2)
        db.get(AuthorModel, first).name = "First Updated"
        db.commit()
        db.delete(db.get(AuthorModel, second))
        db.commit()
        third = _add_author(db, rid, "Third", 2)
        # the reload's bulk delete is not tracked: the author's INSERT row stays open
        bulk_deleted = _add_author(db, rid, "Bulk Deleted", 3)
        db.query(AuthorModel).filter(AuthorModel.author_id == bulk_deleted).delete(synchronize_session=False)
        db.commit()
        # a curator then edits the third author
        curator = _make_curator_user(db, "AGR:HIST-CLEANUP-1")
        _set_current_user(curator, db.query(UserModel).join(
            PersonModel, PersonModel.person_id == UserModel.person_id).filter(
            PersonModel.curie == curator).one().user_id)
        db.get(AuthorModel, third).name = "Third Curated"
        db.commit()
        assert _author_history(db, rid) == sorted(
            [(first, 0), (first, 1), (second, 0), (second, 2), (third, 0), (third, 1), (bulk_deleted, 0)])

        # a dry run counts and deletes nothing
        assert cleanup(db, apply=False) == 5
        assert len(_author_history(db, rid)) == 7

        assert cleanup(db, apply=True, batch_references=1) == 5
        # the first author's current automated version, and the curator's edit
        assert _author_history(db, rid) == sorted([(first, 1), (third, 1)])
        # nothing left to delete
        assert cleanup(db, apply=False) == 0
    finally:
        _set_current_user(*saved_user)
