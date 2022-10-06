import json
import urllib.request

from agr_literature_service.global_utils import memoized


@memoized
def get_schema_data_from_alliance():
    agr_schemas_reference_json_url = 'https://raw.githubusercontent.com/alliance-genome/agr_schemas/master/ingest/resourcesAndReferences/reference.json'
    with urllib.request.urlopen(agr_schemas_reference_json_url) as url:
        schema_data = json.loads(url.read().decode())
        schema_data['properties']['mod_corpus_associations'] = 'injected_okay'
    return schema_data
