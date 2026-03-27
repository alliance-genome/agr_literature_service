import html
import logging
import re
from os import environ

import requests
from sqlalchemy.orm import Session

from agr_literature_service.api.models import CrossReferenceModel, ResourceModel

logger = logging.getLogger(__name__)

EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"


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


def lookup_resource_by_nlm(nlm_id: str, db: Session) -> dict:
    """Look up a resource by NLM ID in the local DB, then at NLM catalog.

    Returns a dict with:
      exists_in_db, resource_curie, external_curie, external_curie_found, title
    """
    nlm_id = nlm_id.strip()
    external_curie = 'NLM:' + nlm_id

    if not nlm_id:
        return {
            'exists_in_db': False,
            'resource_curie': None,
            'external_curie': external_curie,
            'external_curie_found': False,
            'title': ''
        }

    xref = db.query(CrossReferenceModel).filter_by(
        curie="NLM:" + nlm_id
    ).one_or_none()
    if xref and xref.resource_id:
        resource = db.query(ResourceModel).filter_by(
            resource_id=xref.resource_id
        ).one_or_none()
        if resource:
            return {
                'exists_in_db': True,
                'resource_curie': resource.curie,
                'external_curie': external_curie,
                'external_curie_found': True,
                'title': resource.title or ''
            }

    try:
        uid = search_nlm_catalog(nlm_id, 'nlmid')
    except Exception:
        logger.warning("NLM catalog esearch failed for NLM:%s",
                       nlm_id, exc_info=True)
        return {
            'exists_in_db': False,
            'resource_curie': None,
            'external_curie': external_curie,
            'external_curie_found': False,
            'title': ''
        }

    if not uid:
        return {
            'exists_in_db': False,
            'resource_curie': None,
            'external_curie': external_curie,
            'external_curie_found': False,
            'title': ''
        }

    try:
        xml_text = fetch_nlm_catalog_xml(uid)
    except Exception:
        logger.warning("NLM catalog efetch failed for UID:%s",
                       uid, exc_info=True)
        return {
            'exists_in_db': False,
            'resource_curie': None,
            'external_curie': external_curie,
            'external_curie_found': False,
            'title': ''
        }

    title = _extract_title_from_nlm_xml(xml_text)
    if title:
        return {
            'exists_in_db': False,
            'resource_curie': None,
            'external_curie': external_curie,
            'external_curie_found': True,
            'title': title
        }

    return {
        'exists_in_db': False,
        'resource_curie': None,
        'external_curie': external_curie,
        'external_curie_found': False,
        'title': ''
    }


def lookup_resource_by_isbn(isbn: str, db: Session) -> dict:
    """Look up a resource by ISBN in the local DB.

    ISBN external lookup is not yet supported, so if the resource is not
    in the DB we return external_curie_found=False with a message.

    Returns a dict with:
      exists_in_db, resource_curie, external_curie, external_curie_found, title
    """
    isbn = isbn.strip()
    external_curie = 'ISBN:' + isbn

    if not isbn:
        return {
            'exists_in_db': False,
            'resource_curie': None,
            'external_curie': external_curie,
            'external_curie_found': False,
            'title': 'ISBN not supported yet'
        }

    xref = db.query(CrossReferenceModel).filter_by(
        curie="ISBN:" + isbn
    ).one_or_none()
    if xref and xref.resource_id:
        resource = db.query(ResourceModel).filter_by(
            resource_id=xref.resource_id
        ).one_or_none()
        if resource:
            return {
                'exists_in_db': True,
                'resource_curie': resource.curie,
                'external_curie': external_curie,
                'external_curie_found': True,
                'title': resource.title or ''
            }

    return {
        'exists_in_db': False,
        'resource_curie': None,
        'external_curie': external_curie,
        'external_curie_found': False,
        'title': 'ISBN not supported yet'
    }


def lookup_resource_by_issn(issn: str, db: Session) -> dict:
    """Look up a resource by ISSN in the local DB, then at NLM catalog.

    Returns a dict with:
      exists_in_db, resource_curie, external_curie, external_curie_found, title
    """
    issn = issn.strip()
    external_curie = 'ISSN:' + issn

    if not issn:
        return {
            'exists_in_db': False,
            'resource_curie': None,
            'external_curie': external_curie,
            'external_curie_found': False,
            'title': ''
        }

    resource = db.query(ResourceModel).filter(
        (ResourceModel.print_issn == issn) | (ResourceModel.online_issn == issn)
    ).first()
    if resource:
        return {
            'exists_in_db': True,
            'resource_curie': resource.curie,
            'external_curie': external_curie,
            'external_curie_found': True,
            'title': resource.title or ''
        }

    try:
        uid = search_nlm_catalog(issn, 'issn')
    except Exception:
        logger.warning("NLM catalog esearch failed for ISSN:%s",
                       issn, exc_info=True)
        return {
            'exists_in_db': False,
            'resource_curie': None,
            'external_curie': external_curie,
            'external_curie_found': False,
            'title': ''
        }

    if not uid:
        return {
            'exists_in_db': False,
            'resource_curie': None,
            'external_curie': external_curie,
            'external_curie_found': False,
            'title': ''
        }

    try:
        xml_text = fetch_nlm_catalog_xml(uid)
    except Exception:
        logger.warning("NLM catalog efetch failed for UID:%s",
                       uid, exc_info=True)
        return {
            'exists_in_db': False,
            'resource_curie': None,
            'external_curie': external_curie,
            'external_curie_found': False,
            'title': ''
        }

    title = _extract_title_from_nlm_xml(xml_text)
    if title:
        return {
            'exists_in_db': False,
            'resource_curie': None,
            'external_curie': external_curie,
            'external_curie_found': True,
            'title': title
        }

    return {
        'exists_in_db': False,
        'resource_curie': None,
        'external_curie': external_curie,
        'external_curie_found': False,
        'title': ''
    }
