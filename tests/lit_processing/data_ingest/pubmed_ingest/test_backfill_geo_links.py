"""Unit coverage for the pure orchestration logic of backfill_geo_links.

Database/session paths require a live Postgres + ATEAM stack and are exercised
by the existing make run-test-bash integration target. Here we only assert the
behaviour of the helpers that don't touch the DB.
"""
from unittest.mock import MagicMock, patch

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest import backfill_geo_links


class TestInsertGeoXrefs:

    def test_dry_run_counts_but_does_not_create(self):
        with patch.object(backfill_geo_links.cross_reference_crud, "create") as create_mock:
            n = backfill_geo_links._insert_geo_xrefs(
                db=MagicMock(), ref_curie="AGRKB:1", missing=["GSE1", "GSE2"], dry_run=True
            )
        assert n == 2
        assert create_mock.call_count == 0

    def test_live_run_calls_crud_create_per_missing_accession(self):
        with patch.object(backfill_geo_links.cross_reference_crud, "create") as create_mock:
            n = backfill_geo_links._insert_geo_xrefs(
                db=MagicMock(), ref_curie="AGRKB:1", missing=["GSE1", "GSE2"], dry_run=False
            )
        assert n == 2
        assert create_mock.call_count == 2
        for call in create_mock.call_args_list:
            payload = call.args[1]
            assert payload.curie.startswith("GEO:GSE")
            assert payload.reference_curie == "AGRKB:1"

    def test_continues_after_individual_insert_failure(self):
        with patch.object(backfill_geo_links.cross_reference_crud, "create",
                          side_effect=[Exception("dup"), None]) as create_mock:
            n = backfill_geo_links._insert_geo_xrefs(
                db=MagicMock(), ref_curie="AGRKB:1",
                missing=["GSE1", "GSE2"], dry_run=False
            )
        assert n == 1
        assert create_mock.call_count == 2
