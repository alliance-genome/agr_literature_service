from __future__ import annotations

import json
import logging
import os
import re
from collections import defaultdict
from typing import List, Iterable, Dict

import bs4

from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.dqm_processing_utils import compare_dqm_pubmed, \
    simplify_text_keep_digits
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

    def __init__(self, data: dict = None, report_writer: ReportWriter = None):
        self.data = {}
        self.pmid = None
        self._original_primary_id = None
        self.need_update_primary_id = False
        if data:
            self.data = data
            self.original_primary_id = data["primaryId"]
        self.report_writer = report_writer
        self.is_pubmod = True
        self.pubmed_data = {}

    @property
    def original_primary_id(self):
        return self._original_primary_id

    @original_primary_id.setter
    def original_primary_id(self, primary_id):
        self._original_primary_id = primary_id
        self.extract_pmid(primary_id)

    def extract_pmid(self, primary_id):
        pmid_group = re.search(r"^PMID:([0-9]+)", primary_id)
        if pmid_group is not None:
            self.pmid = pmid_group[1]

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
    def generate_default_mod_corpus_association_for_dqm_data(mod):
        return {
            "modAbbreviation": mod,
            "modCorpusSortSource": "dqm_files",
            "corpus": True
        }

    def sanitize_and_sort_entry_into_pubmod_pubmed_or_multi(self, mod, cross_reference_types, resource_to_mod_issn_nlm,
                                                            resource_to_nlm_id, resource_to_nlm_highest_id,
                                                            resource_to_mod, resource_not_found, sanitized_pubmod_data,
                                                            pmid_multi_mods, unmerged_dqm_data_with_pmid,
                                                            sanitized_pubmed_single_mod_data, resource_nlm_id_to_title,
                                                            compare_if_dqm_empty, base_path):
        # inject the mod corpus association data because if it came from that mod dqm file it should have this entry
        self.data['modCorpusAssociations'] = [self.generate_default_mod_corpus_association_for_dqm_data(mod)]

        self.process_xrefs_and_find_pmid_if_necessary(mod=mod, cross_reference_types=cross_reference_types)
        if not self.original_primary_id:
            return

        self.load_pubmed_data_and_determine_if_ref_is_pubmed(mod=mod, pubmed_file_base_path=base_path)
        if self.is_pubmod:
            self.process_pubmod_authors_xrefs_keywords(mod)
            self.set_resource_info_from_abbreviation(mod, resource_to_mod_issn_nlm,
                                                     resource_to_nlm_id, resource_to_nlm_highest_id,
                                                     resource_to_mod,
                                                     resource_not_found)
            sanitized_pubmod_data.append(self)
        else:
            # processing pubmed data
            self.merge_pubmed_single_value_fields_from_pubmed_ref(mod=mod, compare_if_dqm_empty=compare_if_dqm_empty)
            self.replace_fields_with_pubmed_values()
            self.set_additional_author_values_in_dqm_data()
            self.update_xrefs_from_pubmed_data(mod)
            self.update_nlm_resource_info_from_pubmed_data(mod, resource_nlm_id_to_title, resource_to_nlm_id)
            self.merge_keywords_from_pubmed(mod)

            if self.pmid in pmid_multi_mods.keys():
                # logger.info("MULTIPLE pmid %s mod %s", pmid, mod)
                unmerged_dqm_data_with_pmid[self.pmid][mod] = self
            else:
                sanitized_pubmed_single_mod_data.append(self)

    def validate_xref_pages(self, cross_reference, prefix, mod, primary_id):
        if 'pages' in cross_reference:
            if len(cross_reference["pages"]) > 1:
                self.report_writer.write(mod=mod, report_type="generic",
                                         message="mod %s primaryId %s has cross reference identifier %s with "
                                                 "multiple web pages %s\n" % (mod, primary_id, cross_reference["id"],
                                                                              cross_reference["pages"]))
            else:
                return True
        else:
            if prefix not in CROSS_REF_NO_PAGES_OK_FIELDS:
                self.report_writer.write(mod=mod, report_type="generic",
                                         message="mod %s primaryId %s has cross reference identifier %s without "
                                                 "web pages\n" % (mod, primary_id, cross_reference["id"]))
        return False

    def process_xrefs_and_find_pmid_if_necessary(self, mod, cross_reference_types: Dict[str, Dict[str, list]]):
        # need to process crossReferences once to reassign primaryId if PMID and filter out
        # unexpected crossReferences,
        # then again later to clean up crossReferences that get data from pubmed xml (once the PMID is known)
        update_primary_id = False
        too_many_xref_per_type_failure = False
        if 'crossReferences' not in self.data:
            self.report_writer.write(
                mod=mod, report_type="generic", message="mod %s primaryId %s has no cross references\n" % (
                    mod, self.original_primary_id))
        else:
            expected_cross_references = []
            dqm_xrefs = defaultdict(set)
            for cross_reference in self.data['crossReferences']:
                prefix, identifier, separator = split_identifier(cross_reference["id"])
                needs_pmid_extraction = self.validate_xref_pages(
                    cross_reference=cross_reference, prefix=prefix, mod=mod, primary_id=self.original_primary_id)
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
                    self.report_writer.write(mod=mod, report_type="generic",
                                             message="mod %s primaryId %s has too many identifiers for %s %s\n" % (
                                                 mod, self.original_primary_id, prefix,
                                                 ', '.join(sorted(dqm_xrefs[prefix]))))

        if too_many_xref_per_type_failure:
            self.need_update_primary_id = False
        else:
            self.need_update_primary_id = update_primary_id

    def load_pubmed_data_and_determine_if_ref_is_pubmed(self, mod, pubmed_file_base_path):
        if self.pmid:
            filename = pubmed_file_base_path + 'pubmed_json/' + self.pmid + '.json'
            try:
                with open(filename, 'r') as f:
                    self.pubmed_data = json.load(f)
                    self.is_pubmod = False
            except IOError:
                self.report_writer.write(mod=mod, report_type="generic",
                                         message="Warning: PMID %s does not have PubMed xml, from Mod %s primary_id "
                                                 "%s\n" % (self.pmid, mod, self.original_primary_id))

    def set_additional_author_values_in_dqm_data(self):
        # needs to happen after "replace_fields_in_dqm_data_with_pubmed_values"
        if 'authors' in self.data:
            for author in self.data['authors']:
                author['correspondingAuthor'] = False
                author['firstAuthor'] = False

    def update_xrefs_from_pubmed_data(self, mod):
        prefix_xrefs_dict = {}
        if 'crossReferences' in self.pubmed_data:
            for xref in self.pubmed_data['crossReferences']:
                prefix, identifier, _ = split_identifier(xref["id"])
                prefix_xrefs_dict[prefix] = (xref, identifier)
        if 'crossReferences' in self.data:
            for cross_reference in self.data['crossReferences']:
                prefix, identifier, separator = split_identifier(cross_reference['id'])
                if prefix not in prefix_xrefs_dict:
                    prefix_xrefs_dict[prefix] = cross_reference, identifier
                else:
                    if prefix_xrefs_dict[prefix][1].lower() != identifier.lower():
                        self.report_writer.write(
                            mod=mod, report_type="generic",
                            message="primaryId %s has xref %s PubMed has %s%s%s\n" % (
                                self.original_primary_id, cross_reference['id'], prefix, separator,
                                prefix_xrefs_dict[prefix][1]))
        self.data['crossReferences'] = [cross_reference[0] for cross_reference in prefix_xrefs_dict.values()]

    def update_nlm_resource_info_from_pubmed_data(self, mod, resource_nlm_id_to_title, resource_to_nlm_id):
        if 'nlm' in self.pubmed_data:
            nlm_identifier = self.pubmed_data['nlm']
            self.data['nlm'] = ['NLM:' + nlm_identifier]
            self.data['resource'] = 'NLM:' + nlm_identifier
            if nlm_identifier in resource_nlm_id_to_title:
                # logger.info("PMID %s has NLM %s setting to title %s", pmid, nlm, resource_nlm_to_title[nlm])
                self.data['resourceAbbreviation'] = resource_nlm_id_to_title[nlm_identifier]
            nlm_id_simplified = simplify_text_keep_digits(nlm_identifier)
            if nlm_id_simplified not in resource_to_nlm_id:
                self.report_writer.write(
                    mod=mod, report_type="generic",
                    message="NLM value %s from PMID %s XML does not map to a proper resource.\n" % (
                        self.pubmed_data['nlm'], self.pmid))
        else:
            if 'is_journal' in self.pubmed_data:
                self.report_writer.write(mod=mod, report_type="generic",
                                         message="PMID %s does not have an NLM resource.\n" % self.pmid)

    def merge_pubmed_single_value_fields_from_pubmed_ref(self, mod, compare_if_dqm_empty):
        for single_value_field in SINGLE_VALUE_FIELDS:
            pubmed_data_for_field = ""
            dqm_data_for_field = ""
            if single_value_field in self.pubmed_data:
                if single_value_field in DATE_FIELDS:
                    pubmed_data_for_field = self.pubmed_data[single_value_field]['date_string']
                else:
                    pubmed_data_for_field = self.pubmed_data[single_value_field]
            if single_value_field in self.data:
                dqm_data_for_field = self.data[single_value_field]
            if dqm_data_for_field != "":
                dqm_data_for_field = str(bs4.BeautifulSoup(dqm_data_for_field, "html.parser"))
            # UNCOMMENT to output log of data comparison between dqm and pubmed
            if dqm_data_for_field != "" or compare_if_dqm_empty:
                if single_value_field == 'title':
                    compare_dqm_pubmed(mod, "title", self.pmid, single_value_field, dqm_data_for_field,
                                       pubmed_data_for_field,
                                       report_writer=self.report_writer)
                else:
                    compare_dqm_pubmed(mod, "differ", self.pmid, single_value_field, dqm_data_for_field,
                                       pubmed_data_for_field,
                                       report_writer=self.report_writer)
            if pubmed_data_for_field != "":
                self.data[single_value_field] = pubmed_data_for_field
            if single_value_field == 'datePublished':
                if pubmed_data_for_field == "" and dqm_data_for_field != "":
                    self.data[single_value_field] = dqm_data_for_field

    def replace_fields_with_pubmed_values(self):
        for replace_value_field in REPLACE_VALUE_FIELDS:
            # always delete dqm value to be replaced even if the respective pubmed value is empty
            self.data[replace_value_field] = []
            if replace_value_field in self.pubmed_data:
                # logger.info("PMID %s pmid_field %s data %s", pmid, pmid_field, pubmed_data[pmid_field])
                self.data[replace_value_field] = self.pubmed_data[replace_value_field]

    def set_resource_info_from_abbreviation(self, mod, resource_to_mod_issn_nlm, resource_to_nlm_id,
                                            resource_to_nlm_highest_id, resource_to_mod, resource_not_found):
        if 'resourceAbbreviation' in self.data:
            journal_simplified = simplify_text_keep_digits(self.data['resourceAbbreviation'])
            if journal_simplified:
                # logger.info("CHECK mod %s journal_simplified %s", mod, journal_simplified)
                # highest priority to mod resources from dqm resource file with an issn in crossReferences that maps to a single nlm
                if journal_simplified in resource_to_mod_issn_nlm[mod]:
                    self.data['nlm'] = [resource_to_mod_issn_nlm[mod][journal_simplified]]
                    self.data['resource'] = resource_to_mod_issn_nlm[mod][journal_simplified]
                # next highest priority to resource names that map to an nlm
                elif journal_simplified in resource_to_nlm_id:
                    # a resourceAbbreviation can resolve to multiple NLMs, so we cannot use a list of NLMs to get a single canonical NLM title
                    self.data['nlm'] = resource_to_nlm_id[journal_simplified]
                    self.data['resource'] = 'NLM:' + resource_to_nlm_highest_id[journal_simplified]
                    if len(resource_to_nlm_id[
                               journal_simplified]) > 1:  # e.g. ZFIN:ZDB-PUB-020604-2  FB:FBrf0009739  WB:WBPaper00000557
                        self.report_writer.write(
                            mod=mod, report_type="generic",
                            message="primaryId %s has resourceAbbreviation %s mapping to multiple NLMs %s.\n" % (
                                self.original_primary_id, self.data['resourceAbbreviation'],
                                ", ".join(resource_to_nlm_id[journal_simplified])))
                # next highest priority to resource names that are in the dqm resource submission
                elif journal_simplified in resource_to_mod[mod]:
                    self.data['modResources'] = resource_to_mod[mod][journal_simplified]
                    if len(resource_to_mod[mod][journal_simplified]) > 1:
                        self.report_writer.write(
                            mod=mod, report_type="generic",
                            message="primaryId %s has resourceAbbreviation %s mapping to multiple MOD "
                                    "resources %s.\n" % (self.original_primary_id, self.data['resourceAbbreviation'],
                                                         ", ".join(resource_to_mod[mod][journal_simplified])))
                    else:
                        self.data['resource'] = resource_to_mod[mod][journal_simplified][0]
                else:
                    self.report_writer.write(
                        mod=mod, report_type="resource_unmatched",
                        message="primaryId %s has resourceAbbreviation %s not in NLM nor DQM resource "
                                "file.\n" % (self.original_primary_id, self.data['resourceAbbreviation']))
                    resource_not_found[mod][self.data['resourceAbbreviation']] += 1
        else:
            self.report_writer.write(mod=mod, report_type="reference_no_resource",
                                     message="primaryId %s does not have a resourceAbbreviation.\n" %
                                             self.original_primary_id)

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

    def merge_keywords_from_pubmed(self, mod):
        if 'keywords' not in self.data:
            self.data['keywords'] = []
        else:
            # e.g. 9882485 25544291 24201188 31188077
            self.clean_up_keywords(mod)
        if 'keywords' in self.pubmed_data:
            # aggregate for all MODs except ZFIN, which has misformed data and can't fix it.
            # 19308247 aggregates keywords for WB
            entry_keywords = {keyword.upper() for keyword in self.data['keywords']}
            for pubmed_keyword in self.pubmed_data['keywords']:
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
