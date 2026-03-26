import html
import logging
import re

from agr_literature_service.api.models import CrossReferenceModel, ReferenceModel
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import fetch_pubmed_xml
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logger = logging.getLogger(__name__)


def lookup_reference_by_pmid(pmid: str) -> dict:
    """Look up a PMID in the local DB, then at PubMed if not found.

    Returns a dict with:
      exists_in_db: bool
      reference_curie: str or None
      external_curie: str
      external_curie_found: bool
      title: str or empty string
    """
    pmid = re.sub(r'[^\d]', '', pmid)
    external_curie = 'PMID:' + pmid

    if not pmid:
        return {
            'exists_in_db': False,
            'reference_curie': None,
            'external_curie': external_curie,
            'external_curie_found': False,
            'title': ''
        }

    db_session = create_postgres_session(False)
    try:
        xref = db_session.query(CrossReferenceModel).filter_by(
            curie="PMID:" + pmid
        ).one_or_none()
        if xref and xref.reference_id:
            ref = db_session.query(ReferenceModel).filter_by(
                reference_id=xref.reference_id
            ).one_or_none()
            if ref:
                return {
                    'exists_in_db': True,
                    'reference_curie': ref.curie,
                    'external_curie': external_curie,
                    'external_curie_found': True,
                    'title': ref.title or ''
                }
    finally:
        db_session.close()

    xml_text = fetch_pubmed_xml(pmid)
    title = _extract_title_from_pubmed_xml(xml_text)

    if title:
        return {
            'exists_in_db': False,
            'reference_curie': None,
            'external_curie': external_curie,
            'external_curie_found': True,
            'title': title
        }

    return {
        'exists_in_db': False,
        'reference_curie': None,
        'external_curie': external_curie,
        'external_curie_found': False,
        'title': ''
    }


def _extract_title_from_pubmed_xml(xml: str) -> str:
    """Extract title from PubMed XML using the same regex patterns
    as xml_to_json.py:253-278."""
    title_match = re.search(
        r"<ArticleTitle[^>]*?>(.+?)</ArticleTitle>", xml, re.DOTALL)
    if not title_match:
        title_match = re.search(
            r"<BookTitle[^>]*?>(.+?)</BookTitle>", xml, re.DOTALL)
    if not title_match:
        title_match = re.search(
            r"<VernacularTitle[^>]*?>(.+?)</VernacularTitle>", xml, re.DOTALL)
    if not title_match:
        return ''
    title = title_match.group(1).replace('\n', ' ').replace('\r', '')
    title = re.sub(r'\s+', ' ', title)
    return html.unescape(title)
