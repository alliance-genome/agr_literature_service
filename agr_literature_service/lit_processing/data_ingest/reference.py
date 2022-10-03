from __future__ import annotations
import logging
from typing import List, Iterable

from agr_literature_service.lit_processing.data_ingest.utils.alliance_utils import get_schema_data_from_alliance

logger = logging.getLogger(__name__)


REPLACE_VALUE_FIELDS = ['authors', 'pubMedType', 'meshTerms']

SINGLE_VALUE_FIELDS = ['volume', 'title', 'pages', 'issueName', 'datePublished',
                       'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'publisher',
                       'plainLanguageAbstract', 'pubmedAbstractLanguages',
                       'publicationStatus', 'allianceCategory', 'journal']

DATE_FIELDS = ['dateArrivedInPubmed', 'dateLastModified']

PMID_FIELDS = ['authors', 'volume', 'title', 'pages', 'issueName', 'datePublished',
               'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'pubMedType', 'publisher',
               'meshTerms', 'plainLanguageAbstract', 'pubmedAbstractLanguages',
               'publicationStatus', 'allianceCategory', 'journal']


class Reference:

    def __init__(self, data: dict = None):
        self.data = {}
        if data:
            self.data = data

    def __getitem__(self, item):
        return self.data[item]

    def __iter__(self):
        for k in self.data.keys():
            yield k

    def keys(self):
        return self.data.keys()

    def __setitem__(self, key, value):
        self.data[key] = value

    def __contains__(self, item):
        return item in self.data

    def get_data(self):
        return self.data

    def get_list_of_unexpected_mod_properties(self):
        schema_data = get_schema_data_from_alliance()
        return [field for field in self.data.keys() if field not in schema_data['properties']]

    def delete_blank_fields(self):
        for ref_field in list(self.data.keys()):
            if ref_field in SINGLE_VALUE_FIELDS and self.data[ref_field] == "":
                del self.data[ref_field]

    def set_xrefs_from_unmerged_data(self, unmerged_dqm_data_for_single_pmid: Iterable[Reference]):
        cross_references_dict = dict()
        for entry in unmerged_dqm_data_for_single_pmid:
            if 'crossReferences' in entry:
                for cross_ref in entry['crossReferences']:
                    cross_references_dict[cross_ref['id']] = cross_ref['pages'] if 'pages' in cross_ref else None

        for cross_ref_id, pages in cross_references_dict.items():
            sanitized_cross_ref_dict = {"id": cross_ref_id}
            if pages:
                sanitized_cross_ref_dict["pages"] = pages
            if 'crossReferences' not in self.data:
                self.data['crossReferences'] = []
            self.data['crossReferences'].append(sanitized_cross_ref_dict)

