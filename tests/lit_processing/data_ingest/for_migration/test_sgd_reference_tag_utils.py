"""Tests for sgd_reference_tag_utils.py (shared SGD reference-tag helpers)."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError

from agr_literature_service.lit_processing.data_ingest.for_migration import (
    sgd_reference_tag_utils as util,
)


class TestParseSgdDatetime:
    """NEX2 timestamps are Pacific local time; the ABC stores naive UTC."""

    def test_winter_timestamp_is_pst(self):
        # PST is UTC-8
        assert util.parse_sgd_datetime("2026-01-15 10:00:00") == \
            datetime(2026, 1, 15, 18, 0, 0)

    def test_summer_timestamp_is_pdt(self):
        # PDT is UTC-7
        assert util.parse_sgd_datetime("2026-07-15 10:00:00") == \
            datetime(2026, 7, 15, 17, 0, 0)

    def test_date_only_fallback_is_pacific_midnight(self):
        assert util.parse_sgd_datetime("2013-01-28") == \
            datetime(2013, 1, 28, 8, 0, 0)
        assert util.parse_sgd_datetime("2013-07-01") == \
            datetime(2013, 7, 1, 7, 0, 0)

    def test_empty_none_and_garbage_return_none(self):
        assert util.parse_sgd_datetime("") is None
        assert util.parse_sgd_datetime(None) is None
        assert util.parse_sgd_datetime("not a date") is None


class TestSgdDisplayTag:

    def test_maps_all_four_sgd_topics(self):
        assert util.sgd_display_tag("Primary Literature") == "ATP:0000147"
        assert util.sgd_display_tag("Reviews") == "ATP:0000130"
        assert util.sgd_display_tag("Omics") == "ATP:0000148"
        assert util.sgd_display_tag("Additional Literature") == "ATP:0000132"

    def test_strips_whitespace(self):
        assert util.sgd_display_tag(" Reviews ") == "ATP:0000130"

    def test_unknown_or_absent_topic_returns_none(self):
        assert util.sgd_display_tag("Renamed Section") is None
        assert util.sgd_display_tag("") is None
        assert util.sgd_display_tag(None) is None


class TestBuildTagPayload:

    def test_pure_entity_tag_fields(self):
        payload = util.build_tag_payload(
            "AGRKB:1", "ATP:0000005", "SGD:S000002284", source_id=230,
            created_by="AGRKB:103000000000019", display_tag="ATP:0000147",
            date_created=datetime(2021, 11, 1, 17, 0, 30))
        data = payload.dict()
        assert data["topic"] == "ATP:0000005"
        assert data["entity_type"] == "ATP:0000005"
        assert data["entity"] == "SGD:S000002284"
        assert data["entity_id_validation"] == util.ENTITY_ID_VALIDATION
        assert data["species"] == util.SACCHAROMYCES_CEREVISIAE_TAXON
        assert data["display_tag"] == "ATP:0000147"
        assert data["data_novelty"] == util.EXISTING_DATA_NOVELTY_ATP
        assert data["negated"] is False
        assert data["created_by"] == "AGRKB:103000000000019"
        assert data["updated_by"] == "AGRKB:103000000000019"
        assert data["date_created"] == datetime(2021, 11, 1, 17, 0, 30)
        # AuditedModel's before_insert copies date_created onto date_updated
        # when only one is set, so both must be set explicitly.
        assert data["date_updated"] is not None

    def test_topic_only_tag_fields(self):
        payload = util.build_tag_payload(
            "AGRKB:1", util.ROOT_TOPIC_ATP, None, source_id=230,
            display_tag="ATP:0000130")
        data = payload.dict()
        assert data["topic"] == util.ROOT_TOPIC_ATP
        assert data["entity_type"] is None
        assert data["entity"] is None
        assert data["entity_id_validation"] is None
        assert data["display_tag"] == "ATP:0000130"
        assert data["data_novelty"] == util.NEW_DATA_NOVELTY_ATP

    def test_no_date_leaves_both_dates_to_audit_layer(self):
        payload = util.build_tag_payload("AGRKB:1", "ATP:0000005", "SGD:S1",
                                         source_id=230)
        data = payload.dict()
        assert data["date_created"] is None
        assert data["date_updated"] is None
        assert data["created_by"] is None
        assert data["updated_by"] is None


class TestLoadExistingEntityTags:

    def test_key_shapes_for_entity_and_topic_only_tags(self):
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = [
            # entity tag: keyed by entity
            ("AGRKB:1", "ATP:0000005", "SGD:S1",
             11, "ATP:0000147", datetime(2021, 1, 1), "OTTO"),
            # topic-only tag: entity_type/entity NULL, keyed by display_tag
            ("AGRKB:1", None, None,
             12, "ATP:0000130", datetime(2022, 2, 2), "EDITH"),
        ]
        result = util.load_existing_entity_tags(db, 230)
        assert result == {
            ("AGRKB:1", "ATP:0000005", "SGD:S1"):
                (11, "ATP:0000147", datetime(2021, 1, 1), "OTTO"),
            ("AGRKB:1", util.ROOT_TOPIC_ATP, "ATP:0000130"):
                (12, "ATP:0000130", datetime(2022, 2, 2), "EDITH"),
        }
        # Pin the two-arm scoping: pure entity tags plus root-topic entity-less
        # tags (which must carry a display_tag, so the key can never be None).
        sql, params = db.execute.call_args[0]
        sql = str(sql)
        assert "tet.topic = tet.entity_type" in sql
        assert "tet.topic = :root_topic" in sql
        assert "tet.entity IS NULL" in sql
        assert "tet.display_tag IS NOT NULL" in sql
        assert params["sid"] == 230
        assert params["root_topic"] == util.ROOT_TOPIC_ATP
        assert set(params["atps"]) == set(util.ENTITY_TYPE_TO_ATP.values())

    def test_reference_curies_filter(self):
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = []
        util.load_existing_entity_tags(db, 230, ["AGRKB:1", "AGRKB:2"])
        sql, params = db.execute.call_args[0]
        assert "r.curie = ANY(:ref_curies)" in str(sql)
        assert params["ref_curies"] == ["AGRKB:1", "AGRKB:2"]


class TestLoadAbcEntityTags:

    def test_key_shapes_for_entity_and_entity_less_tags(self):
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = [
            # curator's entity tag (any topic): keyed by entity
            ("AGRKB:1", "ATP:0000005", "SGD:S1", None),
            # curator's entity-less tag (e.g. specific HTP topic): keyed by
            # display_tag under the ROOT_TOPIC_ATP key shape
            ("AGRKB:2", None, None, "ATP:0000148"),
        ]
        assert util.load_abc_entity_tags(db) == {
            ("AGRKB:1", "ATP:0000005", "SGD:S1"),
            ("AGRKB:2", util.ROOT_TOPIC_ATP, "ATP:0000148"),
        }
        sql, params = db.execute.call_args[0]
        sql = str(sql)
        assert "tet.entity IS NULL AND tet.display_tag IS NOT NULL" in sql
        assert params["species"] == util.SACCHAROMYCES_CEREVISIAE_TAXON
        assert params["abc_method"] == util.ABC_SOURCE_METHOD


class TestResolveSgdCreatedBy:

    def _db(self, rows):
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = rows
        return db

    def test_empty_id_returns_none(self):
        assert util.resolve_sgd_created_by(self._db([]), "  ", {}) is None

    def test_unique_match_returns_users_id(self):
        db = self._db([("AGRKB:103000000000019",)])
        assert util.resolve_sgd_created_by(db, "EDITH", {}) == "AGRKB:103000000000019"

    def test_no_match_returns_id_verbatim(self):
        assert util.resolve_sgd_created_by(self._db([]), "OTTO", {}) == "OTTO"

    def test_ambiguous_match_returns_id_verbatim(self):
        db = self._db([("AGRKB:1",), ("AGRKB:2",)])
        assert util.resolve_sgd_created_by(db, "KIM", {}) == "KIM"

    def test_memoizes_per_id(self):
        db = self._db([("AGRKB:9",)])
        cache = {}
        assert util.resolve_sgd_created_by(db, "NASH", cache) == "AGRKB:9"
        assert util.resolve_sgd_created_by(db, "NASH", cache) == "AGRKB:9"
        db.execute.assert_called_once()


@patch.object(util, "add_user_if_not_exists")
class TestMaybeUpdateExistingTag:

    def _existing(self):
        # (tag_id, display_tag, date_created, created_by)
        return (11, "ATP:0000147", datetime(2021, 1, 1, 8, 0, 0), "OTTO")

    def test_no_op_when_input_matches(self, mock_add_user):
        db = MagicMock()
        assert util.maybe_update_existing_tag(
            db, self._existing(), "ATP:0000147",
            datetime(2021, 1, 1, 8, 0, 0), "OTTO") is False
        db.query.assert_not_called()
        db.commit.assert_not_called()

    def test_none_input_never_clears_stored_values(self, mock_add_user):
        db = MagicMock()
        assert util.maybe_update_existing_tag(
            db, self._existing(), None, None, None) is False
        db.query.assert_not_called()

    def test_corrects_display_tag(self, mock_add_user):
        db = MagicMock()
        tag = MagicMock()
        db.query.return_value.filter_by.return_value.one.return_value = tag
        assert util.maybe_update_existing_tag(
            db, self._existing(), "ATP:0000132",
            datetime(2021, 1, 1, 8, 0, 0), "OTTO") is True
        assert tag.display_tag == "ATP:0000132"
        db.commit.assert_called_once()
        mock_add_user.assert_not_called()

    def test_corrects_created_by_and_updated_by_together(self, mock_add_user):
        db = MagicMock()
        tag = MagicMock()
        db.query.return_value.filter_by.return_value.one.return_value = tag
        assert util.maybe_update_existing_tag(
            db, self._existing(), "ATP:0000147",
            datetime(2021, 1, 1, 8, 0, 0), "AGRKB:19") is True
        assert tag.created_by == "AGRKB:19"
        assert tag.updated_by == "AGRKB:19"
        mock_add_user.assert_called_once_with(db, "AGRKB:19")

    def test_corrects_date_created_on_exact_timestamp_mismatch(self, mock_add_user):
        db = MagicMock()
        tag = MagicMock()
        db.query.return_value.filter_by.return_value.one.return_value = tag
        new_date = datetime(2021, 11, 1, 17, 0, 30)
        assert util.maybe_update_existing_tag(
            db, self._existing(), "ATP:0000147", new_date, "OTTO") is True
        assert tag.date_created == new_date


class TestCreateEntityTags:

    def _counts(self):
        return util.new_counts()

    def test_topic_only_association_skipped_when_curated_in_abc(self):
        db = MagicMock()
        counts = self._counts()
        abc_tags = {("AGRKB:1", util.ROOT_TOPIC_ATP, "ATP:0000130")}
        associations = [("AGRKB:1", util.ROOT_TOPIC_ATP, None, None,
                         "ATP:0000130", None)]
        with patch.object(util, "create_tag") as mock_create:
            util.create_entity_tags(db, iter(associations), 230, {}, abc_tags, counts)
        assert counts["skipped_in_abc"] == 1
        mock_create.assert_not_called()

    def test_seen_dedupe_keys_topic_only_rows_on_display_tag(self):
        db = MagicMock()
        counts = self._counts()
        associations = [
            ("AGRKB:1", util.ROOT_TOPIC_ATP, None, None, "ATP:0000130", None),
            ("AGRKB:1", util.ROOT_TOPIC_ATP, None, None, "ATP:0000130", None),  # dup
            ("AGRKB:1", util.ROOT_TOPIC_ATP, None, None, "ATP:0000148", None),  # distinct
        ]
        with patch.object(util, "create_tag", return_value=(1, False)) as mock_create:
            util.create_entity_tags(db, iter(associations), 230, {}, set(), counts)
        assert counts["created"] == 2
        assert counts["duplicate_in_input"] == 1
        assert mock_create.call_count == 2

    def test_existing_topic_only_tag_never_gets_display_tag_corrected(self):
        # display_tag is a topic-only tag's identity, so only date/creator are
        # corrected in place; the display_tag passed on must be None.
        db = MagicMock()
        counts = self._counts()
        existing = {("AGRKB:1", util.ROOT_TOPIC_ATP, "ATP:0000130"):
                    (11, "ATP:0000130", None, None)}
        associations = [("AGRKB:1", util.ROOT_TOPIC_ATP, None, "OTTO",
                         "ATP:0000130", datetime(2021, 1, 1))]
        with patch.object(util, "maybe_update_existing_tag",
                          return_value=True) as mock_update:
            util.create_entity_tags(db, iter(associations), 230, existing, set(), counts)
        assert counts["updated_existing"] == 1
        mock_update.assert_called_once_with(
            db, (11, "ATP:0000130", None, None), None, datetime(2021, 1, 1), "OTTO")

    def test_existing_entity_tag_passes_display_tag_for_correction(self):
        db = MagicMock()
        counts = self._counts()
        existing = {("AGRKB:1", "ATP:0000005", "SGD:S1"):
                    (11, "ATP:0000147", None, None)}
        associations = [("AGRKB:1", "ATP:0000005", "SGD:S1", None,
                         "ATP:0000132", None)]
        with patch.object(util, "maybe_update_existing_tag",
                          return_value=False) as mock_update:
            util.create_entity_tags(db, iter(associations), 230, existing, set(), counts)
        assert counts["skipped_duplicate"] == 1
        mock_update.assert_called_once_with(
            db, (11, "ATP:0000147", None, None), "ATP:0000132", None, None)

    def test_409_counts_as_duplicate(self):
        db = MagicMock()
        counts = self._counts()
        associations = [("AGRKB:1", "ATP:0000005", "SGD:S1", None, None, None)]
        with patch.object(util, "create_tag",
                          side_effect=HTTPException(status_code=409, detail="dup")):
            util.create_entity_tags(db, iter(associations), 230, {}, set(), counts)
        assert counts["skipped_duplicate"] == 1
        assert counts["errors"] == 0

    def test_generator_failure_aborts_gracefully(self):
        # An exception while PRODUCING an association (file read, curator
        # resolution, payload access) must not escape as a bare traceback:
        # the run aborts with the counts accumulated so far.
        db = MagicMock()
        counts = self._counts()

        def associations():
            yield ("AGRKB:1", "ATP:0000005", "SGD:S1", None, None, None)
            raise OSError("disk gone")

        with patch.object(util, "create_tag", return_value=(1, False)):
            util.create_entity_tags(db, associations(), 230, {}, set(), counts)
        assert counts["created"] == 1
        assert counts["aborted"] is True
        assert counts["errors"] == 1
        db.rollback.assert_called()

    @patch.object(util, "sleep")
    def test_aborts_after_consecutive_create_errors(self, mock_sleep):
        db = MagicMock()
        counts = self._counts()
        associations = ((f"AGRKB:{i}", "ATP:0000005", f"SGD:S{i}", None, None, None)
                        for i in range(util.ABORT_AFTER_CONSECUTIVE_ERRORS + 10))
        with patch.object(util, "create_tag", side_effect=RuntimeError("boom")):
            util.create_entity_tags(db, associations, 230, {}, set(), counts)
        assert counts["aborted"] is True
        assert counts["errors"] == util.ABORT_AFTER_CONSECUTIVE_ERRORS


class TestSelectOverCapPapers:

    def test_returns_only_groups_strictly_over_the_cap(self):
        entities_by_paper = {
            ("SGD:S1", "gene"): {f"SGD:G{i}" for i in range(
                util.MAX_ASSOCIATIONS_PER_PAPER + 1)},
            ("SGD:S1", "allele"): {f"SGD:A{i}" for i in range(
                util.MAX_ASSOCIATIONS_PER_PAPER)},
            ("SGD:S2", "gene"): {"SGD:G1"},
        }
        assert util.select_over_cap_papers(entities_by_paper) == {
            ("SGD:S1", "gene"): util.MAX_ASSOCIATIONS_PER_PAPER + 1,
        }


def _make_source_db(existing_first, existing_second=None):
    """db whose query() returns a mod row for ModModel and, for the source model,
    yields ``existing_first`` then ``existing_second`` from one_or_none()."""
    db = MagicMock()
    mod_q = MagicMock()
    mod_q.filter_by.return_value.one.return_value = MagicMock(mod_id=4)
    src_q = MagicMock()
    src_q.filter_by.return_value.one_or_none.side_effect = [existing_first, existing_second]
    db.query.side_effect = lambda model: mod_q if model is util.ModModel else src_q
    return db


class TestGetOrCreateSource:

    def test_returns_existing_source_id(self):
        db = _make_source_db(MagicMock(topic_entity_tag_source_id=230))
        assert util.get_or_create_source(db) == 230
        db.add.assert_not_called()

    def test_creates_source_when_absent(self):
        db = _make_source_db(None)
        with patch.object(util, "TopicEntityTagSourceModel",
                          return_value=MagicMock(topic_entity_tag_source_id=500)):
            assert util.get_or_create_source(db) == 500
        db.add.assert_called_once()
        db.commit.assert_called_once()

    def test_recovers_from_unique_constraint_race(self):
        db = _make_source_db(None, MagicMock(topic_entity_tag_source_id=230))
        db.commit.side_effect = IntegrityError("stmt", "params", Exception("dup"))
        with patch.object(util, "TopicEntityTagSourceModel",
                          return_value=MagicMock(topic_entity_tag_source_id=999)):
            assert util.get_or_create_source(db) == 230
        db.rollback.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
