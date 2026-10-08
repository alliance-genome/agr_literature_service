"""SCRUM-6513: author email_address from PubMed, without a database."""
from unittest.mock import MagicMock

from agr_literature_service.lit_processing.data_ingest.utils.author import (
    Author,
    authors_lists_are_equal,
)
from agr_literature_service.lit_processing.data_ingest.utils.db_write_utils import (
    sync_author_emails,
)


def _json_author(rank, name, email=None):
    author = {"name": name, "firstname": name.split()[0], "lastname": name.split()[-1],
              "authorRank": rank}
    if email:
        author["email"] = email
    return author


def test_author_loads_email_from_json_and_db_dicts():
    assert Author.load_from_json_dict(_json_author(1, "Ann Lee", "ann@x.org")).email == "ann@x.org"
    assert Author.load_from_json_dict(_json_author(1, "Ann Lee")).email is None
    db_author = Author.load_from_db_dict({"name": "Ann Lee", "author_order": 1, "email_address": "ann@x.org"})
    assert db_author.email == "ann@x.org"


def test_email_is_not_part_of_author_equality():
    """A new or changed email must not trigger the drop-and-reload of authors."""
    from_json = Author.load_list_of_authors_from_json_dict_list(
        [_json_author(1, "Ann Lee", "ann@x.org"), _json_author(2, "Bo Kim")])
    from_db = Author.load_list_of_authors_from_db_dict_list(
        [{"name": "Ann Lee", "first_name": "Ann", "last_name": "Lee", "author_order": 1},
         {"name": "Bo Kim", "first_name": "Bo", "last_name": "Kim", "author_order": 2,
          "email_address": "old@y.org"}])
    assert authors_lists_are_equal(from_json, from_db)


def _db_with_authors(rows):
    db = MagicMock()
    db.query.return_value.filter.return_value = rows
    return db


def test_sync_author_emails_sets_and_changes_by_order():
    first = MagicMock(author_order=1, email_address=None)
    second = MagicMock(author_order=2, email_address="old@y.org")
    db = _db_with_authors([first, second])
    authors = Author.load_list_of_authors_from_json_dict_list(
        [_json_author(1, "Ann Lee", "ann@x.org"), _json_author(2, "Bo Kim", "bo@y.org")])
    assert sync_author_emails(db, 42, authors) == 2
    assert first.email_address == "ann@x.org"
    assert second.email_address == "bo@y.org"


def test_sync_author_emails_never_clears_and_skips_unchanged():
    kept = MagicMock(author_order=1, email_address="ann@x.org")
    db = _db_with_authors([kept])
    authors = Author.load_list_of_authors_from_json_dict_list(
        [_json_author(1, "Ann Lee", "ann@x.org"), _json_author(2, "Bo Kim")])
    assert sync_author_emails(db, 42, authors) == 0
    assert kept.email_address == "ann@x.org"


def test_sync_author_emails_without_any_email_does_not_query():
    db = MagicMock()
    authors = Author.load_list_of_authors_from_json_dict_list([_json_author(1, "Ann Lee")])
    assert sync_author_emails(db, 42, authors) == 0
    db.query.assert_not_called()
