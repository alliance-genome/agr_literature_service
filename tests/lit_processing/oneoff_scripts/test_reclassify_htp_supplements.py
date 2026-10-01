"""The SCRUM-6612 backfill's size measurement: the stored S3 objects are
gzipped, so the script must apply the HTP thresholds to the UNCOMPRESSED
size (like path.getsize at download-time classification), and it should stop
reading as soon as the threshold is passed."""
import gzip
import importlib.util
import io
from pathlib import Path
from typing import Any, cast

# The script file is ticket-prefixed ("SCRUM-6612_..."), which is not an
# importable module name — load it by path. spec_from_file_location is typed
# Optional and its loader protocol lacks exec_module, hence the narrowing
# assert and the cast for mypy.
_SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "agr_literature_service" / "lit_processing" / "oneoff_scripts"
    / "SCRUM-6612_reclassify_htp_supplements.py"
)
_spec = importlib.util.spec_from_file_location("reclassify_htp_supplements", _SCRIPT)
assert _spec is not None and _spec.loader is not None
reclassify_htp_supplements = importlib.util.module_from_spec(_spec)
cast(Any, _spec.loader).exec_module(reclassify_htp_supplements)

uncompressed_size_exceeds = cast(Any, reclassify_htp_supplements).uncompressed_size_exceeds


def gz_body(payload: bytes):
    """An S3-body-like object (read(n)) holding the gzipped payload."""
    return io.BytesIO(gzip.compress(payload))


class TestUncompressedSizeExceeds:

    def test_measures_the_uncompressed_size_not_the_stored_size(self):
        # Highly compressible: 600,000 uncompressed bytes gzip to a few KB.
        # The decision must be about the 600,000.
        payload = b"a" * 600_000
        body = gz_body(payload)
        assert body.getbuffer().nbytes < 500_000  # stored object is tiny
        exceeds, measured = uncompressed_size_exceeds(gz_body(payload), 500_000)
        assert exceeds is True
        assert measured > 500_000

    def test_at_the_threshold_is_not_htp(self):
        # strictly greater, matching is_htp_supplement_by_size
        exceeds, measured = uncompressed_size_exceeds(gz_body(b"a" * 500_000), 500_000)
        assert exceeds is False
        assert measured == 500_000

    def test_one_byte_over_is_htp(self):
        exceeds, _ = uncompressed_size_exceeds(gz_body(b"a" * 500_001), 500_000)
        assert exceeds is True

    def test_stops_early_once_over_threshold(self):
        # A body that records how much was read: with a tiny threshold the
        # reader must not consume the whole (large) stream. Incompressible
        # (random) data keeps the gzipped stream large, so there is actually
        # something left to skip.
        import os
        payload = gzip.compress(os.urandom(5_000_000))
        reads = []

        class CountingBody(io.BytesIO):
            def read(self, n=-1):
                chunk = super().read(n)
                reads.append(len(chunk))
                return chunk

        exceeds, _ = uncompressed_size_exceeds(CountingBody(payload), 1_000)
        assert exceeds is True
        assert sum(reads) < len(payload)

    def test_empty_file(self):
        exceeds, measured = uncompressed_size_exceeds(gz_body(b""), 500_000)
        assert exceeds is False
        assert measured == 0


class TestCandidateRows:
    """SQLAlchemy 2.0 rejects plain SQL strings in Session.execute with an
    ArgumentError before anything reaches the database (review finding) —
    the query must be a text() clause. A recording stub session verifies the
    statement type and shape without needing a database."""

    class RecordingSession:
        def __init__(self):
            self.statements = []

        def execute(self, statement, params=None):
            self.statements.append(statement)

            class EmptyResult:
                @staticmethod
                def fetchall():
                    return []
            return EmptyResult()

    def test_query_is_a_text_clause_with_the_expected_shape(self):
        from sqlalchemy.sql.elements import TextClause
        session = self.RecordingSession()
        rows = reclassify_htp_supplements.candidate_rows(session, limit=5)
        assert rows == []
        (statement,) = session.statements
        assert isinstance(statement, TextClause)
        sql = str(statement)
        assert "file_class = 'supplement'" in sql
        assert "md5sum IS NOT NULL" in sql
        assert "LIMIT 5" in sql
        # extensions come from the shared threshold map
        for ext in ("'txt'", "'tsv'", "'csv'", "'xls'", "'xlsx'"):
            assert ext in sql
