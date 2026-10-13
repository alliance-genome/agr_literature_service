"""Removing email addresses from stored author affiliations: the per-reference
cleanup with the database query stubbed."""
from types import SimpleNamespace
from unittest.mock import MagicMock

from agr_literature_service.lit_processing.oneoff_scripts import cleanup_author_affiliation_emails as script


def _author(author_id, order, first, last, affiliations, email=None):
    return SimpleNamespace(author_id=author_id, author_order=order, first_name=first, last_name=last,
                           affiliations=affiliations, email_address=email)


def _cleanup(authors):
    db = MagicMock()
    db.query.return_value.filter.return_value.order_by.return_value.all.return_value = authors
    return script.cleanup_reference(db, 42)


def test_addresses_are_stripped_and_missing_emails_attributed_first():
    zhao = _author(1, 1, "Liming", "Zhao", ["East China University, Shanghai 200000, China. "
                                            "Electronic address: zhaoliming@ecust.edu.cn."])
    yang = _author(2, 2, "Ying", "Yang", ["Sichuan University, Yibin 644000, China. "
                                          "Electronic address: yangying@suse.edu.cn."], email="ying@other.org")
    assert _cleanup([zhao, yang]) == (2, 1)
    assert zhao.affiliations == ["East China University, Shanghai 200000, China."]
    assert zhao.email_address == "zhaoliming@ecust.edu.cn"
    # an existing email is never changed
    assert yang.affiliations == ["Sichuan University, Yibin 644000, China."]
    assert yang.email_address == "ying@other.org"


def test_audit_columns_are_left_alone_and_untouched_authors_skipped():
    only_address = _author(1, 1, "Ann", "Lee", ["ann.lee@x.org"], email="ann.lee@x.org")
    clean = _author(2, 2, "Bo", "Kim", ["Dept X, Kyoto."])
    assert _cleanup([only_address, clean]) == (1, 0)
    assert only_address.affiliations is None
    # updated_by / date_updated are not restamped, so a curator edit stays visible
    assert getattr(only_address, "_skip_audit_auto_update", False)
    assert not hasattr(clean, "_skip_audit_auto_update")
    assert clean.affiliations == ["Dept X, Kyoto."]


def test_parse_args_is_a_dry_run_by_default():
    assert script.parse_args([]).commit is False
    assert script.parse_args(["--commit", "--limit", "3"]).limit == 3
