import json
import logging
import time
import urllib.error
import urllib.request

from agr_literature_service.global_utils import memoized

logger = logging.getLogger(__name__)

AGR_SCHEMAS_REFERENCE_JSON_URL = (
    'https://raw.githubusercontent.com/alliance-genome/agr_schemas/master/'
    'ingest/resourcesAndReferences/reference.json'
)

# raw.githubusercontent.com is served through a CDN that occasionally returns a
# transient 503 "first byte timeout". Retry a few times before giving up so a
# momentary blip does not abort an entire DQM ingest run.
_REQUEST_TIMEOUT = 30   # seconds to wait for a response
_MAX_ATTEMPTS = 5       # total tries before giving up
_RETRY_BACKOFF = 3      # base seconds, multiplied by the attempt number


@memoized
def get_schema_data_from_alliance():
    last_error = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(
                    AGR_SCHEMAS_REFERENCE_JSON_URL, timeout=_REQUEST_TIMEOUT) as url:
                schema_data = json.loads(url.read().decode())
                schema_data['properties']['mod_corpus_associations'] = 'injected_okay'
            return schema_data
        except (urllib.error.URLError, TimeoutError) as error:
            # Client errors (4xx, e.g. a wrong URL or renamed branch) will not
            # fix themselves, so fail fast rather than burning through retries.
            if isinstance(error, urllib.error.HTTPError) and error.code < 500:
                raise
            last_error = error
            if attempt < _MAX_ATTEMPTS:
                wait = _RETRY_BACKOFF * attempt
                logger.warning(
                    "Attempt %d/%d to fetch AGR reference schema from %s failed (%s); "
                    "retrying in %ds", attempt, _MAX_ATTEMPTS,
                    AGR_SCHEMAS_REFERENCE_JSON_URL, error, wait)
                time.sleep(wait)
    raise RuntimeError(
        f"Failed to fetch AGR reference schema from {AGR_SCHEMAS_REFERENCE_JSON_URL} "
        f"after {_MAX_ATTEMPTS} attempts") from last_error
