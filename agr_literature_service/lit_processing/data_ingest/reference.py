from __future__ import annotations

import json
import logging
import os
import re
from collections import defaultdict
from typing import List, Iterable, Dict

import bs4

from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.report_writer import ReportWriter
from agr_literature_service.lit_processing.data_ingest.utils.alliance_utils import get_schema_data_from_alliance
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import write_json, chunks
from agr_literature_service.lit_processing.utils.generic_utils import split_identifier

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

CROSS_REF_NO_PAGES_OK_FIELDS = ['DOI', 'PMID', 'PMC', 'PMCID', 'ISBN']

EXPECTED_XREF_TYPES = {
    xref.lower() for xref in
    [
        'PMID:',
        'PMCID:PMC',
        'DOI:',
        'DOI:/S',
        'DOI:IJIv',
        'WB:WBPaper',
        'SGD:S',
        'RGD:',
        'MGI:',
        'ISBN:',
        'FB:FBrf',
        'ZFIN:ZDB-PUB-',
        'Xenbase:XB-ART-'
    ]
}

# when getting pubmed data and merging mod cross references, was excluding these types, but
# now merging so long as the type does not already exist from pubmed (mods have DOIs not in PubMed)
# pubmed_not_dqm_cross_reference_type = set()
# pubmed_not_dqm_cross_reference_type.add('PMID:'.lower())
# pubmed_not_dqm_cross_reference_type.add('PMCID:PMC'.lower())
# pubmed_not_dqm_cross_reference_type.add('DOI:'.lower())
# pubmed_not_dqm_cross_reference_type.add('DOI:/S'.lower())
# pubmed_not_dqm_cross_reference_type.add('DOI:IJIv'.lower())

EXCLUDE_XREF_TYPES = {
    xref for xref in
    [
        'WB:WBTransgene',
        'WB:WBGene',
        'WB:WBVar',
        'Xenbase:XB-GENEPAGE-'
    ]
}


class Reference:

    def __init__(self, data: dict = None):
        self.data = {}
        self.original_primary_id = None
        self.need_update_primary_id = False
        if data:
            self.data = data
            self.original_primary_id = data["primaryId"]

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

    @staticmethod
    def validate_xref_pages(cross_reference, prefix, mod, primary_id, report_writer: ReportWriter):
        if 'pages' in cross_reference:
            if len(cross_reference["pages"]) > 1:
                report_writer.write(mod=mod, report_type="generic",
                                    message="mod %s primaryId %s has cross reference identifier %s with "
                                            "multiple web pages %s\n" % (mod, primary_id, cross_reference["id"],
                                                                         cross_reference["pages"]))
            else:
                return True
        else:
            if prefix not in CROSS_REF_NO_PAGES_OK_FIELDS:
                report_writer.write(mod=mod, report_type="generic",
                                    message="mod %s primaryId %s has cross reference identifier %s without "
                                            "web pages\n" % (mod, primary_id, cross_reference["id"]))
        return False

    def process_xrefs_and_find_pmid_if_necessary(self, mod, report_writer: ReportWriter,
                                                 cross_reference_types: Dict[str, Dict[str, list]]):
        # need to process crossReferences once to reassign primaryId if PMID and filter out
        # unexpected crossReferences,
        # then again later to clean up crossReferences that get data from pubmed xml (once the PMID is known)
        update_primary_id = False
        too_many_xref_per_type_failure = False
        if 'crossReferences' not in self.data:
            report_writer.write(
                mod=mod, report_type="generic", message="mod %s primaryId %s has no cross references\n" % (
                    mod, self.original_primary_id))
        else:
            expected_cross_references = []
            dqm_xrefs = defaultdict(set)
            for cross_reference in self.data['crossReferences']:
                prefix, identifier, separator = split_identifier(cross_reference["id"])
                needs_pmid_extraction = self.validate_xref_pages(
                    cross_reference=cross_reference, prefix=prefix, mod=mod, primary_id=self.original_primary_id,
                    report_writer=report_writer)
                if needs_pmid_extraction:
                    if not re.match(r"^PMID:[0-9]+", self.original_primary_id) and \
                            cross_reference["pages"][0] == 'PubMed' and \
                            re.match(r"^PMID:[0-9]+", cross_reference["id"]):
                        update_primary_id = True
                        self.data['primaryId'] = cross_reference["id"]
                        self.original_primary_id = cross_reference["id"]

                cross_ref_type_group = re.search(r"^([^0-9]+)[0-9]", cross_reference['id'])
                if cross_ref_type_group is not None:
                    if cross_ref_type_group[1].lower() not in EXPECTED_XREF_TYPES:
                        cross_reference_types[mod][cross_ref_type_group[1]].append(
                            self.original_primary_id + ' ' + cross_reference['id'])
                    if cross_ref_type_group[1].lower() not in EXCLUDE_XREF_TYPES:
                        dqm_xrefs[prefix].add(identifier)
                        expected_cross_references.append(cross_reference)
            self.data['crossReferences'] = expected_cross_references
            for prefix, identifiers in dqm_xrefs.items():
                if len(identifiers) > 1:
                    too_many_xref_per_type_failure = True
                    report_writer.write(mod=mod, report_type="generic",
                                        message="mod %s primaryId %s has too many identifiers for %s %s\n" % (
                                            mod, self.original_primary_id, prefix,
                                            ', '.join(sorted(dqm_xrefs[prefix]))))

        if too_many_xref_per_type_failure:
            self.need_update_primary_id = False
        else:
            self.need_update_primary_id = update_primary_id

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

    def clean_up_keywords(self, mod):
        # e.g. 9882485 25544291 24201188 31188077
        if mod == 'ZFIN':
            if 'keywords' in self.data:
                if self.data['keywords'][0] == '':
                    self.data['keywords'] = []
                else:
                    # zfin has all keywords in the first array element, they cannot fix it
                    zfin_value = self.data['keywords'][0]
                    zfin_value = str(bs4.BeautifulSoup(zfin_value, "html.parser"))
                    comma_count = 0
                    semicolon_count = 0
                    if ", " in zfin_value:
                        comma_count = zfin_value.count(',')
                    if "; " in zfin_value:
                        semicolon_count = zfin_value.count(';')
                    if (comma_count == 0) and (semicolon_count == 0):
                        self.data['keywords'] = [zfin_value]
                    elif comma_count >= semicolon_count:
                        self.data['keywords'] = zfin_value.split(", ")
                    else:
                        self.data['keywords'] = zfin_value.split("; ")
        else:
            keywords = []
            for mod_keyword in self.data['keywords']:
                mod_keyword = str(bs4.BeautifulSoup(mod_keyword, "html.parser"))
                keywords.append(mod_keyword)
            self.data['keywords'] = keywords

    def merge_keywords_from_pubmed(self, pubmed_data: Reference, mod):
        if 'keywords' not in self.data:
            self.data['keywords'] = []
        else:
            # e.g. 9882485 25544291 24201188 31188077
            self.clean_up_keywords(mod)
        if 'keywords' in pubmed_data:
            # aggregate for all MODs except ZFIN, which has misformed data and can't fix it.
            # 19308247 aggregates keywords for WB
            entry_keywords = {keyword.upper() for keyword in self.data['keywords']}
            for pubmed_keyword in pubmed_data['keywords']:
                if pubmed_keyword.upper() not in entry_keywords:
                    self.data['keywords'].append(pubmed_keyword)

    def process_pubmod_authors_xrefs_keywords(self, mod):
        if 'authors' in self.data:
            all_authors_have_rank = all(['authorRank' in author for author in self.data['authors']])
            for author in self.data['authors']:
                author['correspondingAuthor'] = False
                author['firstAuthor'] = False
            if not all_authors_have_rank:
                for idx, _ in enumerate(self.data['authors']):
                    self.data['authors'][idx]['authorRank'] = idx + 1
            if self.need_update_primary_id:
                for idx, _ in enumerate(self.data['authors']):
                    self.data['authors'][idx]['referenceId'] = self.original_primary_id
        if 'crossReferences' in self.data:
            self.data['crossReferences'] = [cross_reference for cross_reference in self.data['crossReferences'] if
                                            split_identifier(cross_reference['id'])[0].lower() != 'pmid']
        if 'keywords' in self.data:
            self.clean_up_keywords(mod)


def load_references_data_from_dqm_json(filename):
    """
    Load reference data from file
    """
    logger.info("Loading %s", filename)
    if os.path.exists(filename):
        return json.load(open(filename, 'r'))["data"]
    else:
        logger.info("No file found %s", filename)
        return None


def write_sanitized_references_to_json(references: List[Reference], entries_size, base_file_name):
    data = [ref.get_data() for ref in references]
    for i, sanitized_pubmed_data_chunk in enumerate(chunks(data, entries_size)):
        json_filename = base_file_name + "_" + str(i + 1) + '.json'
        write_json(json_filename, sanitized_pubmed_data_chunk)
