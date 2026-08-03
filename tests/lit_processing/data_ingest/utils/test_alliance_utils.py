from unittest.mock import MagicMock, patch

from agr_literature_service.lit_processing.data_ingest.utils import alliance_utils


class TestGetSchemaDataFromAlliance:
    def test_injects_mod_corpus_associations(self):
        # Reset the memoization cache so the patched urlopen is actually used.
        alliance_utils.get_schema_data_from_alliance.cache = {}  # type: ignore[attr-defined]

        payload = b'{"properties": {"title": {"type": "string"}}}'
        mock_response = MagicMock()
        mock_response.read.return_value = payload
        mock_response.__enter__.return_value = mock_response
        mock_response.__exit__.return_value = False

        with patch.object(alliance_utils.urllib.request, "urlopen",
                          return_value=mock_response) as mock_urlopen:
            schema = alliance_utils.get_schema_data_from_alliance()

        mock_urlopen.assert_called_once()
        assert schema["properties"]["mod_corpus_associations"] == "injected_okay"
        assert schema["properties"]["title"] == {"type": "string"}
