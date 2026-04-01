import html
import logging
import re
from os import environ

import requests
from sqlalchemy.orm import Session

from agr_literature_service.api.models import CrossReferenceModel, ResourceModel
from agr_literature_service.api.crud import resource_crud
from agr_literature_service.api.schemas import ResourceSchemaPost

logger = logging.getLogger(__name__)

EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"


def find_resources_in_db(db: Session, nlm_id: str = None,
                         issns: list = None) -> list:
    """Search for existing resources by NLM ID and/or ISSNs.

    Checks cross_reference table for NLM:<nlm_id> and ISSN:<issn>,
    and resource.print_issn / online_issn columns for each ISSN.

    When ISBN are supported, add them here.

    Returns a deduplicated list of dicts:
      [{'curie': 'AGRKB:...', 'resource_id': 123, 'title': '...'}, ...]
    """
    seen_ids = set()
    results = []

    if nlm_id:
        xref = db.query(CrossReferenceModel).filter(
            CrossReferenceModel.curie == "NLM:" + nlm_id,
            CrossReferenceModel.resource_id.isnot(None)
        ).one_or_none()
        if xref:
            resource = db.query(ResourceModel).filter_by(
                resource_id=xref.resource_id
            ).one_or_none()
            if resource and resource.resource_id not in seen_ids:
                seen_ids.add(resource.resource_id)
                results.append({
                    'curie': resource.curie,
                    'resource_id': resource.resource_id,
                    'title': resource.title or ''
                })

    if issns:
        for issn in issns:
            if not issn:
                continue
            # Check resource table columns
            resources = db.query(ResourceModel).filter(
                (ResourceModel.print_issn == issn)
                | (ResourceModel.online_issn == issn)
            ).all()
            for resource in resources:
                if resource.resource_id not in seen_ids:
                    seen_ids.add(resource.resource_id)
                    results.append({
                        'curie': resource.curie,
                        'resource_id': resource.resource_id,
                        'title': resource.title or ''
                    })
            # Check cross_reference table
            xrefs = db.query(CrossReferenceModel).filter(
                CrossReferenceModel.curie == "ISSN:" + issn,
                CrossReferenceModel.resource_id.isnot(None)
            ).all()
            for xref in xrefs:
                if xref.resource_id not in seen_ids:
                    resource = db.query(ResourceModel).filter_by(
                        resource_id=xref.resource_id
                    ).one_or_none()
                    if resource:
                        seen_ids.add(resource.resource_id)
                        results.append({
                            'curie': resource.curie,
                            'resource_id': resource.resource_id,
                            'title': resource.title or ''
                        })

    return results


def search_nlm_catalog(term: str, field: str) -> str:
    """Query NLM catalog esearch to get a UID for a term.

    Example: search_nlm_catalog("0028-0836", "issn") queries
    esearch.fcgi?db=nlmcatalog&term=0028-0836[issn]&retmode=json
    and returns the first UID from esearchresult.idlist, or ''.
    """
    url = f"{EUTILS_BASE}/esearch.fcgi"
    parameters = {
        'db': 'nlmcatalog',
        'term': f'{term}[{field}]',
        'retmode': 'json'
    }
    if environ.get('NCBI_API_KEY'):
        parameters['api_key'] = environ['NCBI_API_KEY']
    r = requests.get(url, params=parameters)
    r.raise_for_status()
    data = r.json()
    id_list = data.get('esearchresult', {}).get('idlist', [])
    if id_list:
        return id_list[0]
    return ''


def fetch_nlm_catalog_xml(uid: str) -> str:
    """Fetch NLM catalog XML from efetch API. Returns raw XML text.

    Example: fetch_nlm_catalog_xml("410462") queries
    efetch.fcgi?db=nlmcatalog&id=410462&retmode=xml
    """
    url = f"{EUTILS_BASE}/efetch.fcgi"
    parameters = {
        'db': 'nlmcatalog',
        'id': uid,
        'retmode': 'xml'
    }
    if environ.get('NCBI_API_KEY'):
        parameters['api_key'] = environ['NCBI_API_KEY']
    r = requests.get(url, params=parameters)
    r.raise_for_status()
    return r.text


def _extract_title_from_nlm_xml(xml: str) -> str:
    """Extract title from NLM catalog XML.

    The title lives inside <TitleMain><Title>...</Title></TitleMain>.
    """
    title_match = re.search(
        r"<TitleMain>\s*<Title[^>]*>(.+?)</Title>", xml, re.DOTALL)
    if not title_match:
        return ''
    title = title_match.group(1).replace('\n', ' ').replace('\r', '')
    title = re.sub(r'\s+', ' ', title).strip()
    return html.unescape(title)


def _clean_text(text: str) -> str:
    """Normalize whitespace and unescape HTML entities."""
    text = text.replace('\n', ' ').replace('\r', '')
    text = re.sub(r'\s+', ' ', text).strip()
    return html.unescape(text)


def parse_nlm_catalog_xml(xml: str) -> dict:
    """Parse NLM catalog XML and extract fields for resource creation.

    Returns a camelCase dict compatible with process_resource_entry:
      {
          'primaryId': 'NLM:0410462',
          'nlm': '0410462',
          'title': 'Nature.',
          'medlineAbbreviation': 'Nature',
          'printISSN': '0028-0836',
          'onlineISSN': '1476-4687',
          'titleSynonyms': ['Nature (London)', ...],
          'publisher': 'Nature Publishing Group',
          'crossReferences': [{'id': 'NLM:0410462'}]
      }

    Only keys with valid extracted values are included.
    Returns empty dict if no NlmUniqueID is found.
    """
    nlm_match = re.search(r"<NlmUniqueID>(.+?)</NlmUniqueID>", xml)
    if not nlm_match:
        return {}

    nlm_id = nlm_match.group(1).strip()
    result = {
        'primaryId': 'NLM:' + nlm_id,
        'nlm': nlm_id,
        'crossReferences': [{'id': 'NLM:' + nlm_id}]
    }

    title = _extract_title_from_nlm_xml(xml)
    if title:
        result['title'] = title

    medline_match = re.search(r"<MedlineTA>(.+?)</MedlineTA>", xml)
    if medline_match:
        result['medlineAbbreviation'] = _clean_text(medline_match.group(1))

    for issn_match in re.finditer(r'<ISSN\s([^>]*)>(.+?)</ISSN>', xml):
        attrs, value = issn_match.group(1), issn_match.group(2).strip()
        if 'ValidYN="N"' in attrs:
            continue
        if 'IssnType="Print"' in attrs and 'printISSN' not in result:
            result['printISSN'] = value
        elif 'IssnType="Electronic"' in attrs and 'onlineISSN' not in result:
            result['onlineISSN'] = value

    synonyms = []
    for alt_match in re.finditer(
            r"<TitleAlternate[^>]*>\s*<Title[^>]*>(.+?)</Title>",
            xml, re.DOTALL):
        syn = _clean_text(alt_match.group(1))
        if syn and syn not in synonyms and syn != title:
            synonyms.append(syn)
    if synonyms:
        result['titleSynonyms'] = synonyms

    publisher_match = re.search(
        r'<Imprint\s+ImprintType="Current"[^>]*>.*?<Entity>(.+?)</Entity>',
        xml, re.DOTALL)
    if not publisher_match:
        publisher_match = re.search(
            r'<Imprint\s+ImprintType="Original"[^>]*>.*?<Entity>(.+?)</Entity>',
            xml, re.DOTALL)
    if publisher_match:
        result['publisher'] = _clean_text(publisher_match.group(1))

    return result


def _parsed_dict_to_schema(parsed: dict) -> ResourceSchemaPost:
    """Convert a camelCase parsed dict to a ResourceSchemaPost."""
    schema_data = {'title': parsed.get('title', '')}
    if 'medlineAbbreviation' in parsed:
        schema_data['medline_abbreviation'] = parsed['medlineAbbreviation']
    if 'printISSN' in parsed:
        schema_data['print_issn'] = parsed['printISSN']
    if 'onlineISSN' in parsed:
        schema_data['online_issn'] = parsed['onlineISSN']
    if 'publisher' in parsed:
        schema_data['publisher'] = parsed['publisher']
    if 'titleSynonyms' in parsed:
        schema_data['title_synonyms'] = parsed['titleSynonyms']
    xrefs = parsed.get('crossReferences', [])
    if xrefs:
        schema_data['cross_references'] = [
            {'curie': x['id']} for x in xrefs
        ]
    return ResourceSchemaPost(**schema_data)


def create_resource_from_external_curie(identifier: str, field: str, db: Session) -> dict:
    """Look up resource in DB; if not found, fetch from NLM catalog and create it.

    Args:
        identifier: The NLM ID or ISSN value to look up.
        field: The esearch field ('nlmid' or 'issn').
        db: SQLAlchemy session.

    Returns a dict with:
        exists_in_db: bool - whether the resource already existed
        resource_curies: list or None - AGRKB curies of matching/created resources
    """
    if field == 'nlmid':
        found = find_resources_in_db(db, nlm_id=identifier)
    else:
        found = find_resources_in_db(db, issns=[identifier])
    if found:
        return {
            'exists_in_db': True,
            'resource_curies': [r['curie'] for r in found]
        }

    uid = search_nlm_catalog(identifier, field)
    if not uid:
        return {'exists_in_db': False, 'resource_curies': None}

    xml_text = fetch_nlm_catalog_xml(uid)
    parsed = parse_nlm_catalog_xml(xml_text)
    if not parsed or 'title' not in parsed:
        return {'exists_in_db': False, 'resource_curies': None}

    issns = [v for v in [parsed.get('printISSN'), parsed.get('onlineISSN')] if v]
    found = find_resources_in_db(
        db, nlm_id=parsed.get('nlm'), issns=issns)
    if found:
        return {
            'exists_in_db': True,
            'resource_curies': [r['curie'] for r in found]
        }

    resource_schema = _parsed_dict_to_schema(parsed)
    curie = resource_crud.create(db, resource_schema)
    return {'exists_in_db': False, 'resource_curies': [curie]}


def lookup_resource(identifier: str, prefix: str, db: Session) -> dict:
    """Look up a resource by NLM ID, ISSN, or ISBN.

    Checks local DB first, then NLM catalog for NLM/ISSN.
    ISBN only checks DB (external lookup not yet supported).

    Returns a dict with:
      exists_in_db, resource_curies, external_curie, external_curie_found, title
    """
    identifier = identifier.strip()
    prefix_upper = prefix.upper()
    if prefix_upper == 'NLMID':
        prefix_upper = 'NLM'
    external_curie = prefix_upper + ':' + identifier

    not_found = {
        'exists_in_db': False,
        'resource_curies': None,
        'external_curie': external_curie,
        'external_curie_found': False,
        'title': ''
    }

    if not identifier:
        if prefix.lower() == 'isbn':
            not_found['title'] = 'ISBN not supported yet'
        return not_found

    # Initial DB check
    prefix_lower = prefix.lower()
    if prefix_lower in ('nlm', 'nlmid'):
        found = find_resources_in_db(db, nlm_id=identifier)
    elif prefix_lower == 'issn':
        found = find_resources_in_db(db, issns=[identifier])
    elif prefix_lower == 'isbn':
        found = []
        xref = db.query(CrossReferenceModel).filter_by(
            curie="ISBN:" + identifier
        ).one_or_none()
        if xref and xref.resource_id:
            resource = db.query(ResourceModel).filter_by(
                resource_id=xref.resource_id
            ).one_or_none()
            if resource:
                found = [{'curie': resource.curie,
                          'resource_id': resource.resource_id,
                          'title': resource.title or ''}]
    else:
        found = []

    if found:
        return {
            'exists_in_db': True,
            'resource_curies': [r['curie'] for r in found],
            'external_curie': external_curie,
            'external_curie_found': True,
            'title': found[0]['title']
        }

    # ISBN: no external lookup supported yet
    if prefix_lower == 'isbn':
        not_found['title'] = 'ISBN not supported yet'
        return not_found

    # NLM catalog lookup
    field = 'nlmid' if prefix_lower in ('nlm', 'nlmid') else 'issn'
    try:
        uid = search_nlm_catalog(identifier, field)
    except Exception:
        logger.warning("NLM catalog esearch failed for %s:%s",
                       prefix_upper, identifier, exc_info=True)
        return not_found

    if not uid:
        return not_found

    try:
        xml_text = fetch_nlm_catalog_xml(uid)
    except Exception:
        logger.warning("NLM catalog efetch failed for UID:%s",
                       uid, exc_info=True)
        return not_found

    parsed = parse_nlm_catalog_xml(xml_text)
    title = parsed.get('title', '')

    # Post-fetch DB check with all identifiers from the catalog
    issns = [v for v in [parsed.get('printISSN'),
                         parsed.get('onlineISSN')] if v]
    found = find_resources_in_db(
        db, nlm_id=parsed.get('nlm'), issns=issns)
    if found:
        return {
            'exists_in_db': True,
            'resource_curies': [r['curie'] for r in found],
            'external_curie': external_curie,
            'external_curie_found': True,
            'title': title or found[0]['title']
        }

    if title:
        return {
            'exists_in_db': False,
            'resource_curies': None,
            'external_curie': external_curie,
            'external_curie_found': True,
            'title': title
        }

    return not_found
